package raknet

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestConn_ACKFlushesOnDemand(t *testing.T) {
	packetConn := newRecordingPacketConn()
	conn := newTestConn(t, packetConn)

	started := time.Now()
	conn.queueACK(1)
	write := waitForWrite(t, packetConn.writes, time.Second)
	if write[0]&bitFlagACK == 0 {
		t.Fatalf("first write flag = %#x, want ACK", write[0])
	}
	if elapsed := time.Since(started); elapsed > ackFlushInterval*5 {
		t.Fatalf("ACK flushed after %v, want within %v", elapsed, ackFlushInterval*5)
	}
	if pending := conn.pendingACKs(); pending != 0 {
		t.Fatalf("pending ACKs after flush = %d, want 0", pending)
	}

	select {
	case write := <-packetConn.writes:
		t.Fatalf("idle ACK scheduler produced an extra write: %x", write)
	case <-time.After(ackFlushInterval * 3):
	}
}

func TestConn_ACKThresholdFlushesImmediately(t *testing.T) {
	packetConn := newRecordingPacketConn()
	conn := newTestConn(t, packetConn)

	started := time.Now()
	for seq := range uint24(ackImmediateThreshold) {
		conn.queueACK(seq)
	}
	write := waitForWrite(t, packetConn.writes, time.Second)
	if write[0]&bitFlagACK == 0 {
		t.Fatalf("first write flag = %#x, want ACK", write[0])
	}
	if elapsed := time.Since(started); elapsed > ackFlushInterval*5 {
		t.Fatalf("threshold ACK flushed after %v, want within %v", elapsed, ackFlushInterval*5)
	}
}

func TestConn_LatencyUsesConnectedPong(t *testing.T) {
	conn := newTestConn(t, newRecordingPacketConn())
	conn.rtt.Store(int64(400 * time.Millisecond))

	for timestamp() < 20 {
		time.Sleep(time.Millisecond)
	}
	pingTime := timestamp() - 20
	conn.pingMu.Lock()
	conn.pingTime = pingTime
	conn.pingPending = true
	conn.pingMu.Unlock()
	data, err := (&message.ConnectedPong{PingTime: pingTime, PongTime: timestamp()}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := handleConnectedPong(conn, data[1:]); err != nil {
		t.Fatal(err)
	}

	latency := conn.Latency()
	if latency < 5*time.Millisecond || latency > 20*time.Millisecond {
		t.Fatalf("Latency() = %v, want Pong-derived one-way latency", latency)
	}
}

func TestConn_LatencyIgnoresUnmatchedPong(t *testing.T) {
	conn := newTestConn(t, newRecordingPacketConn())
	data, err := (&message.ConnectedPong{PingTime: timestamp(), PongTime: timestamp()}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := handleConnectedPong(conn, data[1:]); err != nil {
		t.Fatal(err)
	}
	if conn.pingMeasured.Load() {
		t.Fatal("unmatched pong updated latency")
	}
}

func TestConn_LatencyIsIndependentOfApplicationTraffic(t *testing.T) {
	listener, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	type dialResult struct {
		conn *Conn
		err  error
	}
	dialed := make(chan dialResult, 1)
	go func() {
		conn, err := Dial(listener.Addr().String())
		dialed <- dialResult{conn: conn, err: err}
	}()
	acceptedConn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	accepted := acceptedConn.(*Conn)
	t.Cleanup(accepted.closeImmediately)

	var dial *Conn
	select {
	case result := <-dialed:
		if result.err != nil {
			t.Fatal(result.err)
		}
		dial = result.conn
	case <-time.After(3 * time.Second):
		t.Fatal("dial timed out")
	}
	t.Cleanup(dial.closeImmediately)

	deadline := time.Now().Add(2 * time.Second)
	for !accepted.pingMeasured.Load() || !dial.pingMeasured.Load() {
		if time.Now().After(deadline) {
			t.Fatal("connected pong measurement timed out")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if latency := accepted.Latency() * 2; latency > 20*time.Millisecond {
		t.Fatalf("accepted idle RTT = %v, want loopback RTT without ACK batching delay", latency)
	}
	if latency := dial.Latency() * 2; latency > 20*time.Millisecond {
		t.Fatalf("dial idle RTT = %v, want loopback RTT without ACK batching delay", latency)
	}

	for range 12 {
		if _, err := accepted.Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if latency := accepted.Latency() * 2; latency > 20*time.Millisecond {
		t.Fatalf("accepted active RTT = %v, want loopback RTT independent of application traffic", latency)
	}
	if latency := dial.Latency() * 2; latency > 20*time.Millisecond {
		t.Fatalf("dial active RTT = %v, want loopback RTT independent of application traffic", latency)
	}
}

type recordingPacketConn struct {
	writes chan []byte
	addr   net.Addr
}

func newRecordingPacketConn() *recordingPacketConn {
	return &recordingPacketConn{
		writes: make(chan []byte, 128),
		addr:   &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)},
	}
}

func (conn *recordingPacketConn) ReadFrom([]byte) (int, net.Addr, error) {
	return 0, nil, net.ErrClosed
}

func (conn *recordingPacketConn) WriteTo(b []byte, _ net.Addr) (int, error) {
	copyOfB := append([]byte(nil), b...)
	conn.writes <- copyOfB
	return len(b), nil
}

func (*recordingPacketConn) Close() error                     { return nil }
func (conn *recordingPacketConn) LocalAddr() net.Addr         { return conn.addr }
func (*recordingPacketConn) SetDeadline(time.Time) error      { return nil }
func (*recordingPacketConn) SetReadDeadline(time.Time) error  { return nil }
func (*recordingPacketConn) SetWriteDeadline(time.Time) error { return nil }

type testConnectionHandler struct{}

func (testConnectionHandler) handle(*Conn, []byte) (bool, error) { return false, nil }
func (testConnectionHandler) limitsEnabled() bool                { return false }
func (testConnectionHandler) close(*Conn)                        {}
func (testConnectionHandler) log() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestConn(t *testing.T, packetConn net.PacketConn) *Conn {
	t.Helper()
	conn := newConn(packetConn, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 19132}, maxMTUSize, testConnectionHandler{})
	t.Cleanup(conn.cancelFunc)
	return conn
}

func waitForWrite(t *testing.T, writes <-chan []byte, timeout time.Duration) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	select {
	case write := <-writes:
		return write
	case <-ctx.Done():
		t.Fatal("timed out waiting for packet write")
		return nil
	}
}
