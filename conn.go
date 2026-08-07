package raknet

import (
	"bytes"
	"context"
	"encoding"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sandertv/go-raknet/internal"
	"github.com/sandertv/go-raknet/internal/message"
)

const (
	// protocolVersion is the current RakNet protocol version. This is Minecraft
	// specific.
	protocolVersion byte = 11

	minMTUSize    = 400
	maxMTUSize    = 1492
	maxWindowSize = 2048

	ackFlushInterval       = time.Second / 100
	ackImmediateThreshold  = 64
	retransmissionInterval = time.Second * 3 / 10
	keepAliveInterval      = time.Second / 2
	closeCheckInterval     = time.Second / 10
)

// Conn represents a connection to a specific client. It is not a real
// connection, as UDP is connectionless, but rather a connection emulated using
// RakNet. Methods may be called on Conn from multiple goroutines
// simultaneously.
type Conn struct {
	// rtt is the ACK-derived round-trip time used for retransmission. The rtt
	// is measured in nanoseconds.
	rtt atomic.Int64
	// pingRTT is the most recent ConnectedPing/ConnectedPong round-trip time.
	// It is kept separate from rtt so ACK scheduling cannot distort the
	// user-facing latency measurement.
	pingRTT      atomic.Int64
	pingMeasured atomic.Bool
	pingMu       sync.Mutex
	pingTime     int64
	pingPending  bool

	closing   atomic.Int64
	closeWake chan struct{}

	ctx        context.Context
	cancelFunc context.CancelFunc

	conn    net.PacketConn
	raddr   net.Addr
	handler connectionHandler

	once      sync.Once
	connected chan struct{}

	mu  sync.Mutex
	buf *bytes.Buffer

	ackBuf, nackBuf *bytes.Buffer

	pk *packet

	seq, orderIndex, messageIndex uint24
	splitID                       uint32

	// mtu is the MTU size of the connection. Packets longer than this size
	// must be split into fragments for them to arrive at the client without
	// losing bytes.
	mtu uint16

	// splits is a map of slices indexed by split IDs. The length of each of the
	// slices is equal to the split count, and packets are positioned in that
	// slice indexed by the split index.
	splits map[uint16][][]byte

	// win is an ordered queue used to track which datagrams were received and
	// which datagrams were missing, so that we can send NACKs to request
	// missing datagrams.
	win *datagramWindow

	ackMu sync.Mutex
	// ackSlice contains sequence numbers waiting to be acknowledged.
	ackSlice     []uint24
	ackScheduled bool
	ackWake      chan struct{}

	// packetQueue is an ordered queue containing packets indexed by their order
	// index.
	packetQueue *packetQueue
	// packets is a channel containing content of packets that were fully
	// processed. Calling Conn.Read() consumes a value from this channel.
	packets *internal.ElasticChan[[]byte]

	// retransmission is a queue filled with packets that were sent with a given
	// datagram sequence number.
	retransmission *resendMap

	lastActivity atomic.Pointer[time.Time]
}

// newConn constructs a new connection specifically dedicated to the address
// passed.
func newConn(conn net.PacketConn, raddr net.Addr, mtu uint16, h connectionHandler) *Conn {
	mtu = clampMTU(mtu, minMTUSize)
	c := &Conn{
		raddr:          raddr,
		conn:           conn,
		mtu:            mtu,
		handler:        h,
		pk:             new(packet),
		connected:      make(chan struct{}),
		packets:        internal.Chan[[]byte](4, 4096),
		splits:         make(map[uint16][][]byte),
		win:            newDatagramWindow(),
		packetQueue:    newPacketQueue(),
		retransmission: newRecoveryQueue(),
		buf:            bytes.NewBuffer(make([]byte, 0, mtu-28)), // - headers.
		ackBuf:         bytes.NewBuffer(make([]byte, 0, 128)),
		nackBuf:        bytes.NewBuffer(make([]byte, 0, 64)),
		ackWake:        make(chan struct{}, 1),
		closeWake:      make(chan struct{}, 1),
	}
	c.ctx, c.cancelFunc = context.WithCancel(context.Background())
	t := time.Now()
	c.lastActivity.Store(&t)
	go c.startTicking()
	return c
}

// clampMTU bounds mtu to the supported range.
func clampMTU(mtu, minMTU uint16) uint16 {
	if mtu == 0 || mtu > maxMTUSize {
		return maxMTUSize
	}
	return max(mtu, minMTU)
}

// effectiveMTU returns the mtu size without the space allocated for IP and
// UDP headers (28 bytes).
func (conn *Conn) effectiveMTU() uint16 {
	return conn.mtu - 28
}

// startTicking schedules ACKs independently from retransmission, keepalive and
// close maintenance. ACKs are only scheduled while some are pending, without
// introducing a 100 Hz idle ticker per connection.
func (conn *Conn) startTicking() {
	retransmissionTicker := time.NewTicker(retransmissionInterval)
	keepAliveTicker := time.NewTicker(keepAliveInterval)
	ackTimer := time.NewTimer(ackFlushInterval)
	if !ackTimer.Stop() {
		<-ackTimer.C
	}
	var (
		ackTimerC    <-chan time.Time
		closeTicker  *time.Ticker
		closeTickerC <-chan time.Time
		acksLeft     int
	)
	defer retransmissionTicker.Stop()
	defer keepAliveTicker.Stop()
	defer ackTimer.Stop()
	defer func() {
		if closeTicker != nil {
			closeTicker.Stop()
		}
	}()

	stopACKTimer := func() {
		if ackTimerC == nil {
			return
		}
		if !ackTimer.Stop() {
			select {
			case <-ackTimer.C:
			default:
			}
		}
		ackTimerC = nil
	}
	for {
		select {
		case <-conn.ackWake:
			pending := conn.pendingACKs()
			if pending >= ackImmediateThreshold {
				stopACKTimer()
				conn.flushACKs()
			} else if ackTimerC == nil && pending != 0 {
				ackTimer.Reset(ackFlushInterval)
				ackTimerC = ackTimer.C
			}
		case <-ackTimerC:
			ackTimerC = nil
			conn.flushACKs()
		case t := <-retransmissionTicker.C:
			conn.checkResend(t)
		case t := <-keepAliveTicker.C:
			if conn.closing.Load() != 0 {
				continue
			}
			// Ping the other end periodically to prevent timeouts and measure
			// latency independently from ACK scheduling.
			_ = conn.sendLatencyPing()

			conn.mu.Lock()
			timedOut := t.Sub(*conn.lastActivity.Load()) > time.Second*5+conn.retransmission.rtt(t)*2
			conn.mu.Unlock()
			if timedOut {
				_ = conn.Close()
			}
		case <-conn.closeWake:
			if closeTicker == nil {
				closeTicker = time.NewTicker(closeCheckInterval)
				closeTickerC = closeTicker.C
			}
			conn.checkClose(time.Now(), &acksLeft)
		case t := <-closeTickerC:
			conn.checkClose(t, &acksLeft)
		case <-conn.ctx.Done():
			return
		}
	}
}

// checkClose finishes a graceful close after all reliable packets are
// acknowledged, or once its deadline is reached.
func (conn *Conn) checkClose(now time.Time, acksLeft *int) {
	unix := conn.closing.Load()
	if unix == 0 {
		return
	}
	before := *acksLeft
	conn.mu.Lock()
	*acksLeft = len(conn.retransmission.unacknowledged)
	conn.mu.Unlock()
	if before != 0 && *acksLeft == 0 {
		conn.closeImmediately()
		return
	}
	since := now.Sub(time.Unix(unix, 0))
	if (*acksLeft == 0 && since > time.Second) || since > time.Second*5 {
		conn.closeImmediately()
	}
}

// pendingACKs returns the number of datagrams waiting to be acknowledged.
func (conn *Conn) pendingACKs() int {
	conn.ackMu.Lock()
	defer conn.ackMu.Unlock()
	return len(conn.ackSlice)
}

// queueACK records a received datagram and wakes the ACK scheduler when the
// first entry or the immediate-flush threshold is reached.
func (conn *Conn) queueACK(seq uint24) {
	conn.ackMu.Lock()
	conn.ackSlice = append(conn.ackSlice, seq)
	wake := false
	if !conn.ackScheduled {
		conn.ackScheduled = true
		wake = true
	} else if len(conn.ackSlice) == ackImmediateThreshold {
		wake = true
	}
	conn.ackMu.Unlock()
	if wake {
		select {
		case conn.ackWake <- struct{}{}:
		default:
		}
	}
}

// flushACKs flushes all pending datagram acknowledgements.
func (conn *Conn) flushACKs() {
	conn.ackMu.Lock()
	defer conn.ackMu.Unlock()
	defer func() { conn.ackScheduled = false }()

	if len(conn.ackSlice) > 0 {
		// Write an ACK packet to the connection containing all datagram
		// sequence numbers that we received since the last tick.
		if err := conn.sendACK(conn.ackSlice...); err != nil {
			return
		}
		conn.ackSlice = conn.ackSlice[:0]
	}
}

// checkResend checks if the connection needs to resend any packets. It sends
// an ACK for packets it has received and sends any packets that have been
// pending for too long.
func (conn *Conn) checkResend(now time.Time) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	var (
		resend []uint24
		rtt    = conn.retransmission.rtt(now)
		delay  = rtt + rtt/2
	)
	conn.rtt.Store(int64(rtt))

	for seq, t := range conn.retransmission.unacknowledged {
		// These packets have not been acknowledged for too long: We resend them
		// by ourselves, even though no NACK has been issued yet.
		if now.Sub(t.timestamp) > delay {
			resend = append(resend, seq)
		}
	}
	_ = conn.resend(resend)
}

// Write writes a buffer b over the RakNet connection. The amount of bytes
// written n is always equal to the length of the bytes written if writing was
// successful. If not, an error is returned and n is 0. Write may be called
// simultaneously from multiple goroutines, but will write one by one.
func (conn *Conn) Write(b []byte) (n int, err error) {
	return conn.writeWithReliability(b, reliabilityReliableOrdered)
}

// writeWithReliability writes a buffer b over the RakNet connection using the
// reliability passed. The amount of bytes written n is always equal to the
// length of the bytes written if writing was successful. If not, an error is
// returned and n is 0. writeWithReliability may be called simultaneously from
// multiple goroutines, but will write one by one. Unlike Write, it allows
// specifying the reliability.
func (conn *Conn) writeWithReliability(b []byte, rel reliability) (n int, err error) {
	select {
	case <-conn.ctx.Done():
		return 0, conn.error(net.ErrClosed, "write")
	default:
		conn.mu.Lock()
		defer conn.mu.Unlock()
		n, err = conn.write(b, rel)
		return n, conn.error(err, "write")
	}
}

// write writes a buffer b over the RakNet connection. The amount of bytes
// written n is always equal to the length of the bytes written if the write
// was successful. If not, an error is returned and n is 0. Write may be called
// simultaneously from multiple goroutines, but will write one by one. Unlike
// Write, write will not lock.
func (conn *Conn) write(b []byte, rel reliability) (n int, err error) {
	fragments := split(b, conn.effectiveMTU())
	var orderIndex uint24
	if rel.sequencedOrOrdered() {
		orderIndex = conn.orderIndex.Inc()
	}

	splitID := uint16(conn.splitID)
	if len(fragments) > 1 {
		conn.splitID++
	}
	for splitIndex, content := range fragments {
		pk := packetPool.Get().(*packet)
		if cap(pk.content) < len(content) {
			pk.content = make([]byte, len(content))
		}
		// We set the actual slice size to the same size as the content. It
		// might be bigger than the previous size, in which case it will grow,
		// which is fine as the underlying array will always be big enough.
		pk.content = pk.content[:len(content)]
		copy(pk.content, content)

		pk.orderIndex = orderIndex
		pk.reliability = rel
		if rel.reliable() {
			pk.messageIndex = conn.messageIndex.Inc()
		}
		if pk.split = len(fragments) > 1; pk.split {
			// If there were more than one fragment, the pk was split, so we
			// need to make sure we set the appropriate fields.
			pk.splitCount = uint32(len(fragments))
			pk.splitIndex = uint32(splitIndex)
			pk.splitID = splitID
		}
		if err = conn.sendDatagram(pk); err != nil {
			return 0, err
		}
		n += len(content)
	}
	return n, nil
}

// Read reads from the connection into the byte slice passed. If successful,
// the amount of bytes read n is returned, and the error returned will be nil.
// Read blocks until a packet is received over the connection, or until the
// session is closed or the read times out, in which case an error is returned.
func (conn *Conn) Read(b []byte) (n int, err error) {
	pk, ok := conn.packets.Recv(conn.ctx)
	if !ok {
		return 0, conn.error(net.ErrClosed, "read")
	} else if len(b) < len(pk) {
		return 0, conn.error(ErrBufferTooSmall, "read")
	}
	return copy(b, pk), err
}

// ReadPacket attempts to read the next packet as a byte slice. ReadPacket
// blocks until a packet is received over the connection, or until the session
// is closed or the read times out, in which case an error is returned.
func (conn *Conn) ReadPacket() (b []byte, err error) {
	pk, ok := conn.packets.Recv(conn.ctx)
	if !ok {
		return nil, conn.error(net.ErrClosed, "read")
	}
	return pk, err
}

// Close closes the connection. All blocking Read or Write actions are
// cancelled and will return an error, as soon as the closing of the connection
// is acknowledged by the client.
func (conn *Conn) Close() error {
	if conn.closing.CompareAndSwap(0, time.Now().Unix()) {
		select {
		case conn.closeWake <- struct{}{}:
		default:
		}
	}
	return nil
}

// Context returns the connection's context. The context is canceled when
// the connection is closed, allowing for cancellation of operations
// that are tied to the lifecycle of the connection.
func (conn *Conn) Context() context.Context {
	return conn.ctx
}

// closeImmediately sends a Disconnect notification to the other end of the
// connection and closes the underlying UDP connection immediately.
func (conn *Conn) closeImmediately() {
	conn.once.Do(func() {
		_, _ = conn.Write([]byte{message.IDDisconnectNotification})
		conn.handler.close(conn)
		conn.cancelFunc()

		conn.mu.Lock()
		defer conn.mu.Unlock()
		// Make sure to return all unacknowledged packets to the packet pool.
		for _, record := range conn.retransmission.unacknowledged {
			record.pk.content = record.pk.content[:0]
			packetPool.Put(record.pk)
		}
		clear(conn.retransmission.unacknowledged)
	})
}

// RemoteAddr returns the remote address of the connection, meaning the address
// this connection leads to.
func (conn *Conn) RemoteAddr() net.Addr {
	return conn.raddr
}

// LocalAddr returns the local address of the connection, which is always the
// same as the listener's.
func (conn *Conn) LocalAddr() net.Addr {
	return conn.conn.LocalAddr()
}

// SetReadDeadline is unimplemented. It always returns ErrNotSupported.
func (conn *Conn) SetReadDeadline(time.Time) error { return ErrNotSupported }

// SetWriteDeadline is unimplemented. It always returns ErrNotSupported.
func (conn *Conn) SetWriteDeadline(time.Time) error { return ErrNotSupported }

// SetDeadline is unimplemented. It always returns ErrNotSupported.
func (conn *Conn) SetDeadline(time.Time) error { return ErrNotSupported }

// Latency returns half the most recent ConnectedPing/ConnectedPong round-trip
// time. Before the first pong arrives, it falls back to the ACK-derived RTT.
func (conn *Conn) Latency() time.Duration {
	if conn.pingMeasured.Load() {
		return time.Duration(conn.pingRTT.Load() / 2)
	}
	return time.Duration(conn.rtt.Load() / 2)
}

// sendLatencyPing sends a connected ping and records its timestamp so only
// the matching pong may update the user-facing latency measurement.
func (conn *Conn) sendLatencyPing() error {
	pingTime := timestamp()
	conn.pingMu.Lock()
	conn.pingTime = pingTime
	conn.pingPending = true
	conn.pingMu.Unlock()
	if err := conn.sendUnreliable(&message.ConnectedPing{PingTime: pingTime}); err != nil {
		conn.pingMu.Lock()
		if conn.pingTime == pingTime {
			conn.pingPending = false
		}
		conn.pingMu.Unlock()
		return err
	}
	return nil
}

// send encodes an encoding.BinaryMarshaler and writes it to the Conn.
func (conn *Conn) send(pk encoding.BinaryMarshaler) error {
	b, _ := pk.MarshalBinary()
	_, err := conn.Write(b)
	return err
}

// sendUnreliable encodes an encoding.BinaryMarshaler and writes it to the Conn using
// unreliable reliability.
func (conn *Conn) sendUnreliable(pk encoding.BinaryMarshaler) error {
	b, _ := pk.MarshalBinary()
	_, err := conn.writeWithReliability(b, reliabilityUnreliable)
	return err
}

// packetPool is used to pool packets that encapsulate their content.
var packetPool = sync.Pool{New: func() any { return &packet{reliability: reliabilityReliableOrdered} }}

// receive receives a packet from the connection, handling it as appropriate.
// If not successful, an error is returned.
func (conn *Conn) receive(b []byte) error {
	t := time.Now()
	conn.lastActivity.Store(&t)

	switch {
	case b[0]&bitFlagACK != 0:
		return conn.handleACK(b[1:])
	case b[0]&bitFlagNACK != 0:
		return conn.handleNACK(b[1:])
	case b[0]&bitFlagDatagram != 0:
		return conn.receiveDatagram(b[1:])
	}
	return nil
}

// receiveDatagram handles the receiving of a datagram found in buffer b. If
// successful, all packets inside the datagram are handled. if not, an error is
// returned.
func (conn *Conn) receiveDatagram(b []byte) error {
	if len(b) < 3 {
		return fmt.Errorf("read datagram: %w", io.ErrUnexpectedEOF)
	}
	seq := loadUint24(b)
	if !conn.win.add(seq) {
		// Datagram was already received, this might happen if a packet took a
		// long time to arrive, and we already sent a NACK for it. This is
		// expected to happen sometimes under normal circumstances, so no reason
		// to return an error.
		return nil
	}
	conn.queueACK(seq)

	if conn.win.shift() == 0 {
		// Datagram window couldn't be shifted up, so we're still missing
		// packets.
		rtt := time.Duration(conn.rtt.Load())
		if missing := conn.win.missing(rtt + rtt/2); len(missing) > 0 {
			if err := conn.sendNACK(missing); err != nil {
				return fmt.Errorf("receive datagram: send NACK: %w", err)
			}
		}
	}
	if conn.win.size() > maxWindowSize && conn.handler.limitsEnabled() {
		return fmt.Errorf("receive datagram: queue window size is too big (%v-%v)", conn.win.lowest, conn.win.highest)
	}
	return conn.handleDatagram(b[3:])
}

// handleDatagram handles the contents of a datagram encoded in a bytes.Buffer.
func (conn *Conn) handleDatagram(b []byte) error {
	for len(b) > 0 {
		n, err := conn.pk.read(b)
		if err != nil {
			return fmt.Errorf("handle datagram: read packet: %w", err)
		}
		b = b[n:]

		handle := conn.receivePacket
		if conn.pk.split {
			handle = conn.receiveSplitPacket
		}
		if err := handle(conn.pk); err != nil {
			return fmt.Errorf("handle datagram: receive packet: %w", err)
		}
	}
	return nil
}

// receivePacket handles the receiving of a packet. It puts the packet in the
// queue and takes out all packets that were obtainable after that, and handles
// them.
func (conn *Conn) receivePacket(packet *packet) error {
	if packet.reliability != reliabilityReliableOrdered {
		// If it isn't a reliable ordered packet, handle it immediately.
		return conn.handlePacket(packet.content)
	}
	if !conn.packetQueue.put(packet.orderIndex, packet.content) {
		// An ordered packet arrived twice.
		return nil
	}
	if conn.packetQueue.WindowSize() > maxWindowSize && conn.handler.limitsEnabled() {
		return fmt.Errorf("packet queue window size is too big (%v-%v)", conn.packetQueue.lowest, conn.packetQueue.highest)
	}
	for _, content := range conn.packetQueue.fetch() {
		if err := conn.handlePacket(content); err != nil {
			return err
		}
	}
	return nil
}

var errZeroPacket = errors.New("handle packet: zero packet length")

// handlePacket handles a packet serialised in byte slice b. If not successful,
// an error is returned. If the packet was not handled by RakNet, it is sent to
// the packet channel.
func (conn *Conn) handlePacket(b []byte) error {
	if len(b) == 0 {
		return errZeroPacket
	}
	if conn.closing.Load() != 0 {
		// Don't continue handling packets if the connection is being closed.
		return nil
	}
	handled, err := conn.handler.handle(conn, b)
	if err != nil {
		return fmt.Errorf("handle packet: %w", err)
	}
	if !handled {
		conn.packets.Send(b)
	}
	return nil
}

func resolve(addr net.Addr) netip.AddrPort {
	if udpAddr, ok := addr.(*net.UDPAddr); ok {
		uaddr := *udpAddr
		ip, _ := netip.AddrFromSlice(uaddr.IP)
		if ip.Is4In6() {
			ip = ip.Unmap()
		}
		return netip.AddrPortFrom(ip, uint16(uaddr.Port))
	}
	return netip.AddrPort{}
}

// receiveSplitPacket handles a passed split packet. If it is the last split
// packet of its sequence, it will continue handling the full packet as it
// otherwise would. An error is returned if the packet was not valid.
func (conn *Conn) receiveSplitPacket(p *packet) error {
	const maxSplitCount = 512
	const maxConcurrentSplits = 16

	if p.splitCount > maxSplitCount && conn.handler.limitsEnabled() {
		return fmt.Errorf("split packet: split count %v exceeds the maximum %v", p.splitCount, maxSplitCount)
	}
	if len(conn.splits) > maxConcurrentSplits && conn.handler.limitsEnabled() {
		return fmt.Errorf("split packet: maximum concurrent splits %v reached", maxConcurrentSplits)
	}
	m, ok := conn.splits[p.splitID]
	if !ok {
		m = make([][]byte, p.splitCount)
		conn.splits[p.splitID] = m
	}
	if p.splitIndex > uint32(len(m)-1) {
		// The split index was either negative or was bigger than the slice
		// size, meaning the packet is invalid.
		return fmt.Errorf("split packet: split index %v is out of range (0 - %v)", p.splitIndex, len(m)-1)
	}
	m[p.splitIndex] = p.content

	if slices.ContainsFunc(m, func(i []byte) bool { return len(i) == 0 }) {
		// We haven't yet received all split fragments, so we cannot add the
		// packets together yet.
		return nil
	}
	p.content = slices.Concat(m...)

	delete(conn.splits, p.splitID)
	return conn.receivePacket(p)
}

// sendACK sends an acknowledgement packet containing the packet sequence
// numbers passed. If not successful, an error is returned.
func (conn *Conn) sendACK(packets ...uint24) error {
	defer conn.ackBuf.Reset()
	return conn.sendAcknowledgement(packets, bitFlagACK, conn.ackBuf)
}

// sendNACK sends an acknowledgement packet containing the packet sequence
// numbers passed. If not successful, an error is returned.
func (conn *Conn) sendNACK(packets []uint24) error {
	defer conn.nackBuf.Reset()
	return conn.sendAcknowledgement(packets, bitFlagNACK, conn.nackBuf)
}

// sendAcknowledgement sends an acknowledgement packet with the packets passed,
// potentially sending multiple if too many packets are passed. The bitflag is
// added to the header byte.
func (conn *Conn) sendAcknowledgement(packets []uint24, bitflag byte, buf *bytes.Buffer) error {
	ack := &acknowledgement{packets: packets}

	for len(ack.packets) != 0 {
		buf.WriteByte(bitflag | bitFlagDatagram)
		n := ack.write(buf, conn.effectiveMTU())
		// We managed to write n packets in the ACK with this MTU size, write
		// the next of the packets in a new ACK.
		ack.packets = ack.packets[n:]
		if err := conn.writeTo(buf.Bytes(), conn.raddr); err != nil {
			return fmt.Errorf("send acknowlegement: %w", err)
		}
		buf.Reset()
	}
	return nil
}

// handleACK handles an acknowledgement packet from the other end of the
// connection. These mean that a datagram was successfully received by the
// other end.
func (conn *Conn) handleACK(b []byte) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	ack := &acknowledgement{}
	if err := ack.read(b); err != nil {
		return fmt.Errorf("read ACK: %w", err)
	}
	for _, sequenceNumber := range ack.packets {
		// Take out all stored packets from the recovery queue.
		if p, ok := conn.retransmission.acknowledge(sequenceNumber); ok {
			// Clear the packet and return it to the pool so that it may be
			// re-used.
			p.content = p.content[:0]
			packetPool.Put(p)
		}
	}
	return nil
}

// handleNACK handles a negative acknowledgment packet from the other end of
// the connection. These mean that a datagram was found missing.
func (conn *Conn) handleNACK(b []byte) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	nack := &acknowledgement{}
	if err := nack.read(b); err != nil {
		return fmt.Errorf("read NACK: %w", err)
	}
	return conn.resend(nack.packets)
}

// resend sends all datagrams currently in the recovery queue with the sequence
// numbers passed.
func (conn *Conn) resend(sequenceNumbers []uint24) (err error) {
	for _, sequenceNumber := range sequenceNumbers {
		pk, ok := conn.retransmission.retransmit(sequenceNumber)
		if !ok {
			continue
		}
		if err = conn.sendDatagram(pk); err != nil {
			return err
		}
	}
	return nil
}

// sendDatagram sends a datagram over the connection that includes the packet
// passed. It is assigned a new sequence number and added to the retransmission.
func (conn *Conn) sendDatagram(pk *packet) error {
	conn.buf.WriteByte(bitFlagDatagram | bitFlagNeedsBAndAS)
	seq := conn.seq.Inc()
	writeUint24(conn.buf, seq)
	pk.write(conn.buf)
	defer conn.buf.Reset()

	if pk.reliability.reliable() {
		// We then re-add the pk to the recovery queue in case the new one gets
		// lost too, in which case we need to resend it again.
		conn.retransmission.add(seq, pk)
	}

	if err := conn.writeTo(conn.buf.Bytes(), conn.raddr); err != nil {
		return fmt.Errorf("send datagram: %w", err)
	}
	return nil
}

// writeTo calls WriteTo on the underlying UDP connection and returns an error
// only if the error returned is net.ErrClosed. In any other case, the error
// is logged but not returned. This is done because at this stage, packets
// being lost to an error can be recovered through resending.
func (conn *Conn) writeTo(p []byte, raddr net.Addr) error {
	if _, err := conn.conn.WriteTo(p, raddr); errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("write to: %w", err)
	} else if err != nil {
		conn.handler.log().Error("write to: "+err.Error(), "raddr", raddr.String())
	}
	return nil
}

// startTime is the time the system or client was started.
var startTime = time.Now()

// timestamp returns a timestamp since startTime in milliseconds.
func timestamp() int64 {
	return time.Since(startTime).Milliseconds()
}
