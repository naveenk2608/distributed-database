package console

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Handler serves the console's API and, if configured, the dashboard:
//
//	GET /api/overview           Overview: nodes, shards, totals, recent events
//	GET /api/history?addr=A     the samples kept for node A, oldest first
//	GET /api/events?limit=N     the N most recent merged events, oldest first
//	GET /api/stream             server-sent events: an "overview" after every poll round
//	GET /api/health             the console itself: its history store
//	GET /                       the dashboard (console.web_dir)
func (c *Console) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/overview", func(w http.ResponseWriter, _ *http.Request) {
		c.mu.Lock()
		ov := c.overview
		c.mu.Unlock()
		writeJSON(w, ov)
	})
	mux.HandleFunc("GET /api/history", func(w http.ResponseWriter, r *http.Request) {
		addr := r.URL.Query().Get("addr")
		c.mu.Lock()
		h, ok := c.history[addr]
		out := append([]Sample{}, h...)
		c.mu.Unlock()
		if !ok && !c.polls(addr) {
			http.Error(w, fmt.Sprintf("the console does not poll %q", addr), http.StatusNotFound)
			return
		}
		writeJSON(w, out)
	})
	mux.HandleFunc("GET /api/events", func(w http.ResponseWriter, r *http.Request) {
		limit := 200
		if v := r.URL.Query().Get("limit"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 {
				http.Error(w, "limit: want a positive number", http.StatusBadRequest)
				return
			}
			limit = min(n, maxEvents)
		}
		c.mu.Lock()
		out := append([]EventView{}, c.events[max(0, len(c.events)-limit):]...)
		c.mu.Unlock()
		writeJSON(w, out)
	})
	mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, _ *http.Request) {
		c.mu.Lock()
		h := map[string]any{"store": "memory", "store_error": c.storeErr}
		c.mu.Unlock()
		if c.store != nil {
			h["store"] = "postgres"
		}
		writeJSON(w, h)
	})
	mux.HandleFunc("GET /api/stream", c.stream)
	if c.cfg.WebDir != "" {
		mux.Handle("GET /", spa(c.cfg.WebDir))
	}
	return mux
}

func (c *Console) polls(addr string) bool {
	for _, n := range c.nodes {
		if n.addr == addr {
			return true
		}
	}
	return false
}

func writeJSON(w http.ResponseWriter, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(b) // the client went away; nothing to do
}

// encodeLocked renders the overview as one server-sent event.
func (c *Console) encodeLocked() []byte {
	b, err := json.Marshal(c.overview)
	if err != nil {
		c.log.Error("encoding the overview", "error", err)
		return nil
	}
	return fmt.Appendf(nil, "event: overview\ndata: %s\n\n", b)
}

// stream sends the current overview, then a new one after every poll
// round, until the client leaves or the console stops.
func (c *Console) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	ch := make(chan []byte, 4)
	c.mu.Lock()
	if c.subs == nil {
		c.mu.Unlock()
		http.Error(w, "the console is stopping", http.StatusServiceUnavailable)
		return
	}
	c.subs[ch] = struct{}{}
	first := c.encodeLocked()
	c.mu.Unlock()
	defer c.unsubscribe(ch)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	msg := first
	for {
		if msg != nil {
			if _, err := w.Write(msg); err != nil {
				return
			}
			flusher.Flush()
		}
		select {
		case <-r.Context().Done():
			return
		case m, open := <-ch:
			if !open {
				return
			}
			msg = m
		case <-keepalive.C:
			msg = []byte(": keepalive\n\n") // a comment line keeps proxies from closing the stream
		}
	}
}

func (c *Console) unsubscribe(ch chan []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.subs[ch]; ok {
		delete(c.subs, ch)
		close(ch)
	}
}

// broadcast hands msg to every subscriber; one that has fallen behind
// misses it and gets the next round's.
func (c *Console) broadcast(msg []byte) {
	if msg == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for ch := range c.subs {
		select {
		case ch <- msg:
		default:
		}
	}
}

// closeSubscribers ends every stream; new ones are refused.
func (c *Console) closeSubscribers() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for ch := range c.subs {
		close(ch)
	}
	c.subs = nil
}

// spa serves the built dashboard from dir; paths that are not files fall
// back to index.html, so the dashboard's own routes load it.
func spa(dir string) http.Handler {
	files := http.FileServer(http.Dir(dir))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clean := filepath.Clean("/" + r.URL.Path)
		if st, err := os.Stat(filepath.Join(dir, clean)); err != nil || st.IsDir() && clean != "/" {
			if !strings.HasPrefix(clean, "/assets/") {
				http.ServeFile(w, r, filepath.Join(dir, "index.html"))
				return
			}
		}
		files.ServeHTTP(w, r)
	})
}
