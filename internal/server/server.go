package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/four-bytes/four-local-bus/internal/router"
	"github.com/gorilla/websocket"
)

// upgrader configures WebSocket connection upgrades.
// CheckOrigin is open to allow connections from any local process.
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Server wraps the HTTP+WebSocket endpoints for the plugin bus.
type Server struct {
	router    *router.Router
	startTime time.Time

	idleTimer *time.Timer
	idleMu    sync.Mutex
	idleDone  chan struct{} // closed when idle shutdown is triggered
	closeOnce sync.Once     // ensures idleDone is closed only once
}

// New creates a Server with the given router.
func New(r *router.Router) *Server {
	return &Server{
		router:    r,
		startTime: time.Now(),
		idleDone:  make(chan struct{}),
	}
}

// HasSubscribers reports whether any client is currently subscribed.
// Used by the startup idle timer to detect orphan bus processes.
func (s *Server) HasSubscribers() bool {
	return s.router.SubscriberCount() > 0
}

// resetIdleTimer cancels the pending idle shutdown timer.
// Called when a subscriber connects (new subscriber = bus is in use).
func (s *Server) resetIdleTimer() {
	s.idleMu.Lock()
	defer s.idleMu.Unlock()

	select {
	case <-s.idleDone:
		return // already shutting down
	default:
	}

	if s.idleTimer != nil {
		s.idleTimer.Stop()
		s.idleTimer = nil
	}
}

// startIdleTimer starts a 30-second countdown to idle shutdown.
// Called when the last subscriber disconnects.
// If a new subscriber connects before the timer fires, resetIdleTimer cancels it.
func (s *Server) startIdleTimer() {
	s.idleMu.Lock()
	defer s.idleMu.Unlock()

	select {
	case <-s.idleDone:
		return // already shutting down
	default:
	}

	if s.idleTimer != nil {
		s.idleTimer.Stop()
	}

	s.idleTimer = time.AfterFunc(30*time.Second, func() {
		log.Println("[bus] idle timeout — no subscribers, shutting down")
		s.closeOnce.Do(func() {
			close(s.idleDone)
		})
	})
}

// IdleDone returns a channel that is closed when the idle shutdown timer fires.
func (s *Server) IdleDone() <-chan struct{} {
	return s.idleDone
}

// Handler returns the http.Handler with all routes registered.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /publish", s.handlePublish)
	mux.HandleFunc("GET /subscribe", s.handleSubscribe)
	mux.HandleFunc("GET /health", s.handleHealth)
	return mux
}

// --- Request/Response types ---

type publishRequest struct {
	Channel string      `json:"channel"`
	Payload interface{} `json:"payload"`
}

// --- Handlers ---

// handlePublish accepts a JSON message and publishes it to the bus.
func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, "failed to read body", http.StatusBadRequest)
		return
	}

	var req publishRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if err := validateChannel(req.Channel); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.router.Publish(req.Channel, req.Payload)
	writeJSON(w, map[string]bool{"ok": true}, http.StatusOK)
}

// handleSubscribe upgrades the HTTP connection to WebSocket and subscribes
// to the requested channel patterns. The connection is then kept alive for
// bidirectional messaging: incoming messages from the client are published
// to the bus.
func (s *Server) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	channelsParam := r.URL.Query().Get("channels")
	patterns := parseChannelPatterns(channelsParam)

	// Reject invalid patterns but allow empty — client can subscribe via WS messages later
	for _, p := range patterns {
		if err := validateChannel(p); err != nil {
			writeError(w, fmt.Sprintf("invalid channel pattern %q: %v", p, err), http.StatusBadRequest)
			return
		}
	}

	// Upgrade to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}

	// Subscribe to all requested patterns (may be empty — client can update via WS)
	for _, pattern := range patterns {
		s.router.Subscribe(pattern, conn)
	}
	s.resetIdleTimer()

	// Read loop: bidirectional — incoming messages are published to the bus
	go func() {
		defer func() {
			s.router.Unsubscribe(conn)
			if s.router.SubscriberCount() == 0 {
				s.startIdleTimer()
			}
			_ = conn.Close()
		}()

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				// Connection closed or error — cleanup in defer
				break
			}

			// Check for subscription update message
			var subMsg struct {
				Subscribe string `json:"subscribe"`
			}
			if err := json.Unmarshal(message, &subMsg); err == nil && subMsg.Subscribe != "" {
				// Re-subscribe: drop all existing subscriptions, then subscribe to new patterns
				s.router.Unsubscribe(conn)
				for _, pattern := range strings.Split(subMsg.Subscribe, ",") {
					pattern = strings.TrimSpace(pattern)
					if pattern != "" {
						s.router.Subscribe(pattern, conn)
					}
				}
				s.resetIdleTimer()
				continue
			}

			var req publishRequest
			if err := json.Unmarshal(message, &req); err != nil {
				continue // ignore malformed messages
			}
			if err := validateChannel(req.Channel); err != nil {
				continue
			}
			s.router.Publish(req.Channel, req.Payload)
		}
	}()
}

// handleHealth returns server health status with uptime in seconds.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	uptime := int(time.Since(s.startTime).Seconds())
	writeJSON(w, map[string]interface{}{
		"status": "ok",
		"uptime": uptime,
	}, http.StatusOK)
}

// --- Validation ---

// validateChannel checks that a channel name conforms to the bus naming rules:
// - Non-empty
// - Max 256 characters
// - No leading or trailing '/'
// - No empty segments
//
// This is used for both publish channels and subscribe patterns.
// Patterns containing '+' (wildcard single-segment) are permitted here;
// they are resolved at match time, not publish time.
func validateChannel(ch string) error {
	ch = strings.TrimSpace(ch)
	if ch == "" {
		return fmt.Errorf("channel is required")
	}
	if len(ch) > 256 {
		return fmt.Errorf("channel too long (max 256 characters)")
	}
	if strings.HasPrefix(ch, "/") || strings.HasSuffix(ch, "/") {
		return fmt.Errorf("channel must not start or end with '/'")
	}
	segments := strings.Split(ch, "/")
	for _, seg := range segments {
		if seg == "" {
			return fmt.Errorf("channel must not have empty segments")
		}
	}
	return nil
}

// --- Helpers ---

func writeJSON(w http.ResponseWriter, data interface{}, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Failed to encode JSON response: %v", err)
	}
}

func writeError(w http.ResponseWriter, msg string, status int) {
	writeJSON(w, map[string]string{"error": msg}, status)
}

// parseChannelPatterns splits a comma-separated channel string into trimmed patterns.
// Returns an empty slice for empty input (no error).
func parseChannelPatterns(input string) []string {
	if input == "" {
		return nil
	}
	raw := strings.Split(input, ",")
	patterns := make([]string, 0, len(raw))
	for _, p := range raw {
		p = strings.TrimSpace(p)
		if p != "" {
			patterns = append(patterns, p)
		}
	}
	return patterns
}
