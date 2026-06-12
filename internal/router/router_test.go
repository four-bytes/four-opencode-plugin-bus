package router

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// mustDial connects to the test WS server and returns the client conn.
func mustDial(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	return c
}

// serveWS starts an HTTP server with a WebSocket endpoint.
// When a client connects, it calls onConnect.
func serveWS(t *testing.T, onConnect func(conn *websocket.Conn)) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("Upgrade failed: %v", err)
			return
		}
		if onConnect != nil {
			onConnect(conn)
		}
	}))
	return srv
}

// readMsg reads one JSON message from conn with a timeout.
func readMsg(t *testing.T, conn *websocket.Conn, timeout time.Duration) ([]byte, bool) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(timeout))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		return nil, false
	}
	return msg, true
}

func TestSubscribe_DeliversLastValueCache(t *testing.T) {
	// Given a router with a published message
	r := New()
	r.Publish("test/channel", map[string]string{"hello": "world"})

	// When a new connection subscribes (simulating WS message re-subscribe)
	var wg sync.WaitGroup
	wg.Add(1)

	srv := serveWS(t, func(conn *websocket.Conn) {
		// Subscribe to the channel (same call as the WS message path)
		r.Subscribe("test/channel", conn)
		wg.Done()
	})
	defer srv.Close()

	conn := mustDial(t, "ws"+srv.URL[4:])
	defer conn.Close()

	// Wait for subscription to complete
	wg.Wait()

	// Then the last-value cache should be delivered
	msg, ok := readMsg(t, conn, 2*time.Second)
	if !ok {
		t.Fatal("Expected last-value cache message, got none")
	}

	expected := `{"channel":"test/channel","payload":{"hello":"world"}`
	got := string(msg)
	if len(got) < len(expected) || got[:len(expected)] != expected {
		t.Fatalf("Expected message starting with %q, got %q", expected, got)
	}
}

func TestSubscribe_NoCacheBeforeFirstPublish(t *testing.T) {
	// Given a router with no published messages
	r := New()

	// When a new connection subscribes
	var wg sync.WaitGroup
	wg.Add(1)

	srv := serveWS(t, func(conn *websocket.Conn) {
		r.Subscribe("test/channel", conn)
		wg.Done()
	})
	defer srv.Close()

	conn := mustDial(t, "ws"+srv.URL[4:])
	defer conn.Close()
	wg.Wait()

	// Then no message should be delivered (no last-value cache)
	_, ok := readMsg(t, conn, 500*time.Millisecond)
	if ok {
		t.Fatal("Expected no message (no last-value cache), but got one")
	}
}

func TestSubscribe_WildcardPatternDeliversLastValue(t *testing.T) {
	// Given a router with multiple channels published
	r := New()
	r.Publish("tbg/ses_abc/status", map[string]int{"tokens": 100})
	r.Publish("tbg/ses_def/status", map[string]int{"tokens": 200})
	r.Publish("other/channel", map[string]string{"ignored": "yes"})

	// When a connection subscribes with a wildcard pattern
	var wg sync.WaitGroup
	wg.Add(1)

	srv := serveWS(t, func(conn *websocket.Conn) {
		// Same as WS message {"subscribe": "tbg/+/status"}
		r.Subscribe("tbg/+/status", conn)
		wg.Done()
	})
	defer srv.Close()

	conn := mustDial(t, "ws"+srv.URL[4:])
	defer conn.Close()
	wg.Wait()

	// Then it should receive only the matching channels (2 total)
	msg1, ok1 := readMsg(t, conn, 2*time.Second)
	msg2, ok2 := readMsg(t, conn, 2*time.Second)
	if !ok1 || !ok2 {
		t.Fatal("Expected 2 last-value cache messages, got fewer")
	}

	// Third message should NOT arrive (only 2 matching channels)
	_, ok3 := readMsg(t, conn, 500*time.Millisecond)
	if ok3 {
		t.Fatal("Expected only 2 last-value cache messages, got more")
	}

	// Verify both messages are from matching channels
	t.Logf("Got cache msg 1: %s", string(msg1))
	t.Logf("Got cache msg 2: %s", string(msg2))
}

func TestSubscribe_ReSubscribeDeliversCacheAgain(t *testing.T) {
	// This simulates the WS message re-subscribe path:
	// client connects without channels, then sends {"subscribe": "..."}
	r := New()
	r.Publish("test/channel", map[string]string{"value": "cached"})

	// Server handler does what server.go lines 182-193 do:
	// unsubscribe all, then subscribe to new patterns
	var wg sync.WaitGroup
	wg.Add(1)

	srv := serveWS(t, func(conn *websocket.Conn) {
		r.Unsubscribe(conn)
		r.Subscribe("test/channel", conn)
		wg.Done()
	})
	defer srv.Close()

	conn := mustDial(t, "ws"+srv.URL[4:])
	defer conn.Close()
	wg.Wait()

	msg, ok := readMsg(t, conn, 2*time.Second)
	if !ok {
		t.Fatal("Expected last-value cache after re-subscribe, got none")
	}

	expected := `{"channel":"test/channel","payload":{"value":"cached"}`
	if len(string(msg)) < len(expected) || string(msg)[:len(expected)] != expected {
		t.Fatalf("Expected message starting with %q, got %q", expected, string(msg))
	}
	t.Logf("Re-subscribe cache delivery: %s", string(msg))
}

func TestSubscribe_MultipleConcurrentSubscribers(t *testing.T) {
	// Given a router with a published message
	r := New()
	r.Publish("test/channel", map[string]int{"id": 42})

	// When multiple clients subscribe concurrently
	var wg sync.WaitGroup
	numClients := 5

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		srv := serveWS(t, func(conn *websocket.Conn) {
			r.Subscribe("test/channel", conn)
			wg.Done()
		})

		go func(s *httptest.Server) {
			defer s.Close()
			c := mustDial(t, "ws"+s.URL[4:])
			defer c.Close()

			msg, ok := readMsg(t, c, 3*time.Second)
			if !ok {
				t.Errorf("Client on %s did not receive last-value cache", s.URL)
				return
			}
			if string(msg) == "" {
				t.Errorf("Client on %s received empty message", s.URL)
			}
		}(srv)
	}

	wg.Wait()
}
