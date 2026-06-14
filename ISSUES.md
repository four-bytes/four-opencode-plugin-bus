**Status:** Last reviewed 2026-06-14. 2/2 fixed (brain), 3/3 fixed (plugin-lib), 2/2 fixed (local-bus), 2/2 fixed (context-curator).

# Known Issues

## #1 — Orphan processes: no startup idle timer

**Symptom:** Multiple `four-local-bus` processes accumulate over time. Each new opencode
start may spawn a new bus binary while old ones linger.

**Root cause:** `startIdleTimer()` is only called when a subscriber *disconnects* and
`SubscriberCount() == 0`. If no subscriber ever connects (e.g., a bus process was spawned
in a race but the TUI discovered a different instance), the idle countdown never starts and
the orphan runs indefinitely.

**Location:** `internal/server/server.go` — `New()` constructor.

**Fix:** Call `startIdleTimer()` immediately in `New()` with a longer grace period (60–120 s).
When the first subscriber connects, `resetIdleTimer()` (already called on connect) cancels it.
After that, the existing 30 s post-disconnect timer takes over.

Parameterize `startIdleTimer` to accept a `time.Duration` so the startup grace period can
differ from the post-disconnect timeout.

```go
// New creates a Server with the given router.
func New(r *router.Router) *Server {
    s := &Server{
        router:    r,
        startTime: time.Now(),
        idleDone:  make(chan struct{}),
    }
    // Grace period: if no subscriber connects within 60 s, treat as orphan and shut down.
    s.startIdleTimer(60 * time.Second)
    return s
}

// startIdleTimer starts a countdown; callers pass the desired duration.
func (s *Server) startIdleTimer(d time.Duration) { ... }
```

---

✅ FIXED — commit 6d8656c (startup idle timer raised to 5 minutes to absorb slow TUI startup).

## #2 — Race condition: concurrent spawns produce extra bus processes

**Symptom:** Two callers of `BusClient.connect()` (in `@four-bytes/opencode-plugin-lib`)
can both conclude the bus is absent at the same moment and both call `startBus()`, spawning
two binaries. Both write to `port.json`; one "wins" and one is forever orphaned (Issue #1
above, made worse by the missing startup timer).

**Location:** `@four-bytes/opencode-plugin-lib/src/bus-client.ts` — `connect()` / `startBus()`.

**Fix:** Add a module-level spawn lock (promise deduplication) so concurrent connect calls
share a single spawn attempt. See ISSUES.md in `four-opencode-plugin-lib`.

✅ FIXED — see `four-opencode-plugin-lib` commit b96489a (`_spawnLock` in `BusClient.connect()`).
