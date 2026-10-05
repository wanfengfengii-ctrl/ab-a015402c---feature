package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"interlock-events/internal/store"
)

const rfc = "2026-10-05T12:00:00Z"

func newTestServer(t *testing.T, retention int) (*httptest.Server, *store.Store) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	st, err := store.Open(dir, retention)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(st, Config{Retention: retention}))
	t.Cleanup(srv.Close)
	return srv, st
}

func postBatch(t *testing.T, base, channel string, evs []map[string]any) (int, []byte) {
	t.Helper()
	body, _ := json.Marshal(evs)
	resp, err := http.Post(base+"/events/"+channel, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func oneEvent(key, sev, msg string) map[string]any {
	return map[string]any{"eventKey": key, "time": rfc, "severity": sev, "message": msg}
}

func TestHealth(t *testing.T) {
	srv, _ := newTestServer(t, 0)
	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("health status %d", resp.StatusCode)
	}
}

func TestPostValidationAndResults(t *testing.T) {
	srv, _ := newTestServer(t, 0)

	if code, _ := postBatch(t, srv.URL, "ch", nil); code != http.StatusBadRequest {
		t.Fatalf("empty batch = %d", code)
	}
	big := make([]map[string]any, 51)
	for i := range big {
		big[i] = oneEvent(fmt.Sprintf("b%d", i), "info", "x")
	}
	if code, _ := postBatch(t, srv.URL, "ch", big); code != http.StatusBadRequest {
		t.Fatalf("51 batch = %d", code)
	}
	badTime := oneEvent("k", "info", "x")
	badTime["time"] = "not-a-time"
	if code, raw := postBatch(t, srv.URL, "ch", []map[string]any{badTime}); code != http.StatusBadRequest ||
		!strings.Contains(string(raw), "RFC3339") {
		t.Fatalf("bad time = %d %s", code, raw)
	}

	code, raw := postBatch(t, srv.URL, "ch", []map[string]any{
		oneEvent("trip", "critical", "dump"),
		oneEvent("warn", "warning", "hot"),
	})
	if code != 200 {
		t.Fatalf("fresh = %d %s", code, raw)
	}
	var first struct {
		Results []store.AppendResult `json:"results"`
	}
	if err := json.Unmarshal(raw, &first); err != nil {
		t.Fatal(err)
	}
	if len(first.Results) != 2 || first.Results[0].ID != 1 || first.Results[1].ID != 2 {
		t.Fatalf("results = %s", raw)
	}

	// Exact retry: same ids, replay true.
	code, raw = postBatch(t, srv.URL, "ch", []map[string]any{
		oneEvent("trip", "critical", "dump"),
		oneEvent("warn", "warning", "hot"),
	})
	if code != 200 {
		t.Fatalf("retry = %d", code)
	}
	json.Unmarshal(raw, &first)
	if first.Results[0].ID != 1 || !first.Results[0].Replay || first.Results[1].ID != 2 {
		t.Fatalf("replay results = %s", raw)
	}

	// Different content -> 409 with locatable items, zero writes.
	code, raw = postBatch(t, srv.URL, "ch", []map[string]any{
		oneEvent("trip", "critical", "edited message"),
	})
	if code != http.StatusConflict {
		t.Fatalf("conflict = %d %s", code, raw)
	}
	var cb struct {
		Items []struct {
			Index    int    `json:"index"`
			EventKey string `json:"eventKey"`
			Existing struct {
				ID int64 `json:"id"`
			} `json:"existing"`
		} `json:"items"`
	}
	if err := json.Unmarshal(raw, &cb); err != nil || len(cb.Items) != 1 ||
		cb.Items[0].Index != 0 || cb.Items[0].EventKey != "trip" || cb.Items[0].Existing.ID != 1 {
		t.Fatalf("conflict body not locatable: %s", raw)
	}
}

// frame is a parsed SSE dispatch.
type frame struct {
	id      int64
	event   string
	data    string
	comment bool
}

func openSSE(ctx context.Context, t *testing.T, base, channel string, lastID int64) (int, <-chan frame) {
	t.Helper()
	return openSSEQuery(ctx, t, base, channel, "", lastID)
}

// openSSEQuery opens the SSE endpoint with an extra raw query string (e.g.
// "severity=critical&severity=warning"). Non-200 responses are drained and
// reported by status with a nil frame channel.
func openSSEQuery(ctx context.Context, t *testing.T, base, channel, query string, lastID int64) (int, <-chan frame) {
	t.Helper()
	url := base + "/streams/" + channel
	if query != "" {
		url += "?" + query
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if lastID > 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(lastID, 10))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp.StatusCode, nil
	}
	out := make(chan frame, 64)
	go func() {
		defer close(out)
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		var f frame
		var data []string
		flush := func() {
			if f.id == 0 && f.event == "" && len(data) == 0 {
				return
			}
			f.data = strings.Join(data, "\n")
			select {
			case out <- f:
			case <-ctx.Done():
			}
			f, data = frame{}, nil
		}
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				flush()
			case strings.HasPrefix(line, ":"):
				select {
				case out <- frame{comment: true}:
				case <-ctx.Done():
					return
				}
			case strings.HasPrefix(line, "id:"):
				f.id, _ = strconv.ParseInt(strings.TrimSpace(line[3:]), 10, 64)
			case strings.HasPrefix(line, "event:"):
				f.event = strings.TrimSpace(line[6:])
			case strings.HasPrefix(line, "data:"):
				d := line[5:]
				d = strings.TrimPrefix(d, " ")
				data = append(data, d)
			}
		}
	}()
	return http.StatusOK, out
}

func waitEvent(t *testing.T, frames <-chan frame, timeout time.Duration) frame {
	t.Helper()
	for {
		select {
		case f := <-frames:
			if f.comment {
				continue
			}
			return f
		case <-time.After(timeout):
			t.Fatal("timed out waiting for event frame")
		}
	}
}

func TestSSEHistoryLiveAndHeartbeat(t *testing.T) {
	srv, _ := newTestServer(t, 0)
	postBatch(t, srv.URL, "ch", []map[string]any{oneEvent("h1", "warning", "history one")})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	code, frames := openSSE(ctx, t, srv.URL, "ch", 0)
	if code != 200 {
		t.Fatalf("stream code %d", code)
	}

	f := waitEvent(t, frames, 3*time.Second)
	if f.id != 1 || f.event != "event" || !strings.Contains(f.data, "history one") {
		t.Fatalf("history frame = %+v", f)
	}

	// Idle heartbeat within 5s.
	select {
	case f := <-frames:
		if !f.comment {
			t.Fatalf("expected heartbeat comment, got %+v", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no heartbeat within 5s")
	}

	// Live publish.
	postBatch(t, srv.URL, "ch", []map[string]any{oneEvent("live1", "critical", "beam off")})
	f = waitEvent(t, frames, 3*time.Second)
	if f.id != 2 || !strings.Contains(f.data, "beam off") {
		t.Fatalf("live frame = %+v", f)
	}
}

func TestSSEResumeExactlyOnceGapless(t *testing.T) {
	srv, _ := newTestServer(t, 0)
	ch := "resume"
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("a", "info", "1")})
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("b", "info", "2")})

	ctx1, cancel1 := context.WithTimeout(context.Background(), 8*time.Second)
	code, frames1 := openSSE(ctx1, t, srv.URL, ch, 0)
	if code != 200 {
		t.Fatal(code)
	}
	f1 := waitEvent(t, frames1, 3*time.Second)
	f2 := waitEvent(t, frames1, 3*time.Second)
	if f1.id != 1 || f2.id != 2 {
		t.Fatalf("history %d,%d", f1.id, f2.id)
	}
	cancel1() // network drop

	// Published while disconnected.
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("c", "info", "3")})
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("d", "critical", "4")})

	ctx2, cancel2 := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel2()
	_, frames2 := openSSE(ctx2, t, srv.URL, ch, 2)
	g1 := waitEvent(t, frames2, 3*time.Second)
	g2 := waitEvent(t, frames2, 3*time.Second)
	if g1.id != 3 || g2.id != 4 {
		t.Fatalf("gap resume = %d,%d", g1.id, g2.id)
	}

	// Seamless live event after resume, delivered exactly once.
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("e", "info", "5")})
	live := waitEvent(t, frames2, 3*time.Second)
	if live.id != 5 {
		t.Fatalf("post-resume live = %d", live.id)
	}
	select {
	case extra := <-frames2:
		if !extra.comment {
			t.Fatalf("duplicate/extra frame id=%d", extra.id)
		}
	case <-time.After(1500 * time.Millisecond):
	}
}

func TestSSEExpiredCursor410(t *testing.T) {
	srv, _ := newTestServer(t, 2)
	ch := "trim"
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("a", "info", "1")}) // id 1
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("b", "info", "2")}) // id 2
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("c", "info", "3")}) // id 3 -> window 2,3

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/streams/"+ch, nil)
	req.Header.Set("Last-Event-ID", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusGone {
		t.Fatalf("want 410, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("410 content-type = %q", ct)
	}
	var body struct {
		EarliestAvailable int64 `json:"earliestAvailableId"`
	}
	raw, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	if body.EarliestAvailable != 2 {
		t.Fatalf("earliestAvailableId = %d, want 2, body=%s", body.EarliestAvailable, raw)
	}
}

func TestConcurrentPublishesExactlyOnce(t *testing.T) {
	srv, _ := newTestServer(t, 0)
	ch := "concurrent"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, frames := openSSE(ctx, t, srv.URL, ch, 0)

	const publishers = 8
	const perPub = 15
	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perPub; i++ {
				key := fmt.Sprintf("p%d-e%d", p, i)
				code, raw := postBatch(t, srv.URL, ch, []map[string]any{oneEvent(key, "info", key)})
				if code != 200 {
					t.Errorf("publish %s: %d %s", key, code, raw)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	want := publishers * perPub
	got := make(map[int64]string, want)
	var prev int64
	deadline := time.After(10 * time.Second)
	for len(got) < want {
		select {
		case f := <-frames:
			if f.comment {
				continue
			}
			if f.id <= prev {
				t.Fatalf("non-monotonic/duplicate delivery: id %d after %d", f.id, prev)
			}
			if _, dup := got[f.id]; dup {
				t.Fatalf("id %d delivered twice", f.id)
			}
			prev = f.id
			var data struct {
				EventKey string `json:"eventKey"`
			}
			json.Unmarshal([]byte(f.data), &data)
			got[f.id] = data.EventKey
		case <-deadline:
			t.Fatalf("got %d/%d events", len(got), want)
		}
	}
	if len(got) != want {
		t.Fatalf("unique ids = %d, want %d", len(got), want)
	}
}

// ---- severity filter ---------------------------------------------------------

func TestStreamSeverityFilterValidation(t *testing.T) {
	srv, _ := newTestServer(t, 0)

	bad := []string{
		"severity=",                           // empty value
		"severity=%20%20",                     // whitespace-only value
		"severity=critical&severity=critical", // duplicate value
		"severity=a&severity=b&severity=c&severity=d&severity=e", // 5 > max 4
	}
	for _, q := range bad {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		code, frames := openSSEQuery(ctx, t, srv.URL, "ch", q, 0)
		cancel()
		if code != http.StatusBadRequest {
			t.Fatalf("query %q: want 400 before SSE, got %d", q, code)
		}
		if frames != nil {
			t.Fatalf("query %q: 400 must not start an SSE stream", q)
		}
	}

	good := []string{
		"severity=critical",
		"severity=critical&severity=warning&severity=info&severity=debug",
	}
	for _, q := range good {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		code, frames := openSSEQuery(ctx, t, srv.URL, "ch", q, 0)
		if code != http.StatusOK {
			cancel()
			t.Fatalf("query %q: want 200, got %d", q, code)
		}
		// Idle filtered stream still heartbeats like an unfiltered one.
		select {
		case f := <-frames:
			if !f.comment {
				cancel()
				t.Fatalf("query %q: expected heartbeat on empty channel, got %+v", q, f)
			}
		case <-time.After(5 * time.Second):
			cancel()
			t.Fatalf("query %q: no heartbeat", q)
		}
		cancel()
	}
}

// checkpointIDs parses a checkpoint frame and asserts id == data.throughId.
func checkpointIDs(t *testing.T, f frame) (int64, int64) {
	t.Helper()
	if f.event != "checkpoint" {
		t.Fatalf("want checkpoint frame, got %+v", f)
	}
	var body struct {
		ThroughID int64 `json:"throughId"`
	}
	if err := json.Unmarshal([]byte(f.data), &body); err != nil {
		t.Fatalf("checkpoint data not JSON: %q", f.data)
	}
	if f.id != body.ThroughID {
		t.Fatalf("checkpoint frame id %d != data.throughId %d", f.id, body.ThroughID)
	}
	return f.id, body.ThroughID
}

func TestStreamSeverityFilterHistoryAndLive(t *testing.T) {
	srv, _ := newTestServer(t, 0)
	ch := "filt"
	// ids: 1 info, 2 critical, 3 info, 4 warning, 5 info
	postBatch(t, srv.URL, ch, []map[string]any{
		oneEvent("e1", "info", "skip 1"),
		oneEvent("e2", "critical", "show 2"),
		oneEvent("e3", "info", "skip 3"),
		oneEvent("e4", "warning", "show 4"),
		oneEvent("e5", "info", "skip 5"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	code, frames := openSSEQuery(ctx, t, srv.URL, ch, "severity=critical&severity=warning", 0)
	if code != 200 {
		t.Fatalf("stream code %d", code)
	}

	// History collapses skipped runs: ckpt(1), event(2), ckpt(3), event(4),
	// and the trailing skipped record flushes as ckpt(5) before going idle.
	f := waitEvent(t, frames, 3*time.Second)
	if id, _ := checkpointIDs(t, f); id != 1 {
		t.Fatalf("first checkpoint through %d, want 1", id)
	}
	f = waitEvent(t, frames, 3*time.Second)
	if f.event != "event" || f.id != 2 || !strings.Contains(f.data, "show 2") {
		t.Fatalf("frame 2 = %+v", f)
	}
	f = waitEvent(t, frames, 3*time.Second)
	if id, _ := checkpointIDs(t, f); id != 3 {
		t.Fatalf("checkpoint through %d, want 3", id)
	}
	f = waitEvent(t, frames, 3*time.Second)
	if f.event != "event" || f.id != 4 || !strings.Contains(f.data, "show 4") {
		t.Fatalf("frame 4 = %+v", f)
	}
	f = waitEvent(t, frames, 3*time.Second)
	if id, _ := checkpointIDs(t, f); id != 5 {
		t.Fatalf("trailing checkpoint through %d, want 5", id)
	}

	// Live: an unwatched publish collapses to a checkpoint, a watched one is
	// a normal event frame, in publish order.
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("e6", "info", "skip 6")})
	f = waitEvent(t, frames, 3*time.Second)
	if id, _ := checkpointIDs(t, f); id != 6 {
		t.Fatalf("live checkpoint through %d, want 6", id)
	}
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("e7", "critical", "show 7")})
	f = waitEvent(t, frames, 3*time.Second)
	if f.event != "event" || f.id != 7 || !strings.Contains(f.data, "show 7") {
		t.Fatalf("live frame = %+v", f)
	}
}

func TestStreamSeverityFilterResume(t *testing.T) {
	srv, _ := newTestServer(t, 0)
	ch := "filt-resume"
	postBatch(t, srv.URL, ch, []map[string]any{
		oneEvent("a", "critical", "show 1"), // id 1
		oneEvent("b", "info", "skip 2"),     // id 2
		oneEvent("c", "debug", "skip 3"),    // id 3
	})

	ctx1, cancel1 := context.WithTimeout(context.Background(), 8*time.Second)
	_, frames1 := openSSEQuery(ctx1, t, srv.URL, ch, "severity=critical", 0)
	f := waitEvent(t, frames1, 3*time.Second)
	if f.event != "event" || f.id != 1 {
		t.Fatalf("history event = %+v", f)
	}
	f = waitEvent(t, frames1, 3*time.Second)
	through, _ := checkpointIDs(t, f)
	if through != 3 {
		t.Fatalf("checkpoint through %d, want 3", through)
	}
	cancel1() // network drop; client saved cursor = 3 from the checkpoint

	// Published while disconnected: 4 skip, 5 show, 6 skip.
	postBatch(t, srv.URL, ch, []map[string]any{
		oneEvent("d", "info", "skip 4"),
		oneEvent("e", "critical", "show 5"),
		oneEvent("f", "info", "skip 6"),
	})

	ctx2, cancel2 := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel2()
	code, frames2 := openSSEQuery(ctx2, t, srv.URL, ch, "severity=critical", 3)
	if code != 200 {
		t.Fatalf("resume from checkpoint cursor: code %d", code)
	}
	// Only the watched event surfaces; skipped ids arrive folded in
	// checkpoints; nothing at or below the cursor is re-covered.
	f = waitEvent(t, frames2, 3*time.Second)
	if id, _ := checkpointIDs(t, f); id != 4 {
		t.Fatalf("resume checkpoint through %d, want 4", id)
	}
	f = waitEvent(t, frames2, 3*time.Second)
	if f.event != "event" || f.id != 5 || !strings.Contains(f.data, "show 5") {
		t.Fatalf("resume event = %+v", f)
	}
	f = waitEvent(t, frames2, 3*time.Second)
	if id, _ := checkpointIDs(t, f); id != 6 {
		t.Fatalf("resume trailing checkpoint through %d, want 6", id)
	}
	select {
	case extra := <-frames2:
		if !extra.comment {
			t.Fatalf("duplicate/extra frame after resume: %+v", extra)
		}
	case <-time.After(1500 * time.Millisecond):
	}
}

func TestStreamSeverityFilterQuietChannelAdvances(t *testing.T) {
	srv, _ := newTestServer(t, 3) // tiny retention to prove cursor freshness
	ch := "quiet"
	// Fill the whole retained window with unwatched records: ids 1..3.
	postBatch(t, srv.URL, ch, []map[string]any{
		oneEvent("q1", "info", "noise 1"),
		oneEvent("q2", "info", "noise 2"),
		oneEvent("q3", "info", "noise 3"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	_, frames := openSSEQuery(ctx, t, srv.URL, ch, "severity=critical", 0)
	f := waitEvent(t, frames, 3*time.Second)
	through, _ := checkpointIDs(t, f)
	if through != 3 {
		t.Fatalf("quiet channel checkpoint through %d, want 3", through)
	}

	// More unwatched records keep advancing the checkpoint, pushing the
	// oldest ids out of the retention window as they go. Each publish is
	// drained before the next so the checkpoints arrive one at a time.
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("q4", "info", "noise 4")})
	f = waitEvent(t, frames, 3*time.Second)
	if cursor, _ := checkpointIDs(t, f); cursor != 4 {
		t.Fatalf("checkpoint cursor = %d, want 4", cursor)
	}
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("q5", "info", "noise 5")})
	f = waitEvent(t, frames, 3*time.Second)
	cursor, _ := checkpointIDs(t, f)
	if cursor != 5 {
		t.Fatalf("checkpoint cursor = %d, want 5", cursor)
	}
	cancel()

	// Reconnect with the checkpoint cursor: NOT a 410, even though ids 1-2
	// have aged out, because the cursor itself moved past them.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel2()
	code, frames2 := openSSEQuery(ctx2, t, srv.URL, ch, "severity=critical", cursor)
	if code != 200 {
		t.Fatalf("reconnect at checkpoint cursor: want 200, got %d", code)
	}
	// A watched event after reconnect is delivered normally.
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("q6", "critical", "real trip")})
	f = waitEvent(t, frames2, 3*time.Second)
	if f.event != "event" || f.id != 6 || !strings.Contains(f.data, "real trip") {
		t.Fatalf("post-reconnect frame = %+v", f)
	}
}

func TestStreamSeverityFilterGoneFullWindow(t *testing.T) {
	srv, _ := newTestServer(t, 2)
	ch := "filt-gone"
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("a", "info", "1")}) // id 1
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("b", "info", "2")}) // id 2
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("c", "info", "3")}) // id 3 -> window 2,3

	// Expiry is judged against the channel's full retention window, filter
	// or not: cursor 1 < earliest retained 2 -> 410 before any SSE byte.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	code, frames := openSSEQuery(ctx, t, srv.URL, ch, "severity=critical", 1)
	if code != http.StatusGone {
		t.Fatalf("filtered expired cursor: want 410, got %d", code)
	}
	if frames != nil {
		t.Fatal("410 must not start an SSE stream")
	}
}

func TestConcurrentPublishesFilteredCoverage(t *testing.T) {
	srv, _ := newTestServer(t, 0)
	ch := "concurrent-filtered"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, frames := openSSEQuery(ctx, t, srv.URL, ch, "severity=critical&severity=warning", 0)

	const publishers = 6
	const perPub = 10
	sevs := []string{"critical", "info", "warning", "debug"}
	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perPub; i++ {
				key := fmt.Sprintf("p%d-e%d", p, i)
				sev := sevs[(p+i)%len(sevs)]
				code, raw := postBatch(t, srv.URL, ch, []map[string]any{oneEvent(key, sev, key)})
				if code != 200 {
					t.Errorf("publish %s: %d %s", key, code, raw)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	// Every id must be covered exactly once, in publish order: watched ids
	// as event frames, unwatched runs folded into checkpoints. Frame ids
	// (event id / checkpoint throughId) therefore strictly increase and an
	// event frame always continues exactly one past the previous coverage.
	want := int64(publishers * perPub)
	var covered, events int64
	deadline := time.After(10 * time.Second)
	for covered < want {
		select {
		case f := <-frames:
			if f.comment {
				continue
			}
			if f.id <= covered {
				t.Fatalf("id coverage regressed/overlapped: frame %+v after %d", f, covered)
			}
			switch f.event {
			case "event":
				if f.id != covered+1 {
					t.Fatalf("event id %d not contiguous after coverage %d", f.id, covered)
				}
				events++
			case "checkpoint":
				checkpointIDs(t, f)
			default:
				t.Fatalf("unexpected frame type %+v", f)
			}
			covered = f.id
		case <-deadline:
			t.Fatalf("coverage stalled at %d/%d (events=%d)", covered, want, events)
		}
	}
	if events == 0 || events == want {
		t.Fatalf("expected a mix of events and checkpoints, got %d/%d events", events, want)
	}
}
