// Command smoke is the one-shot end-to-end verifier. It waits for the
// service to become healthy, then exercises six sections and folds their
// results into a bitmask exit code:
//
//	1  publishing / idempotent replay / 409 zero-write
//	2  SSE live delivery and idle heartbeat
//	4  SSE resume with Last-Event-ID (exactly-once, gapless)
//	8  expired cursor -> HTTP 410 with earliestAvailableId
//	128  severity filter: 400s, filtered resume via checkpoints,
//	     quiet-channel cursor advancement, filtered 410
//
// Build failures and `go test` are aggregated by the verify entrypoint
// (bits 32 and 16 respectively).
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	bitPublish = 1
	bitLive    = 2
	bitResume  = 4
	bitGone    = 8
	bitTests   = 16
	bitBuild   = 32
	bitFilter  = 128
)

type event struct {
	EventKey string `json:"eventKey"`
	Time     string `json:"time"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

type appendResult struct {
	ID     int64 `json:"id"`
	Replay bool  `json:"replay"`
}

type postResp struct {
	Results []appendResult `json:"results"`
}

type conflictBody struct {
	Error             string `json:"error"`
	EarliestAvailable int64  `json:"earliestAvailableId"`
	Items             []struct {
		Index    int    `json:"index"`
		EventKey string `json:"eventKey"`
	} `json:"items"`
}

// sseEvent is one dispatched SSE frame.
type sseEvent struct {
	ID      int64
	Event   string
	Data    string
	Comment bool
}

func main() {
	base := flag.String("base", envOr("BASE_URL", "http://localhost:8080"), "service base URL")
	flag.Parse()

	failed := 0
	run := func(name string, bit int, fn func() error) {
		fmt.Printf("\n=== smoke: %s ===\n", name)
		if err := fn(); err != nil {
			fmt.Printf("[FAIL] %s: %v\n", name, err)
			failed |= bit
			return
		}
		fmt.Printf("[ OK ] %s\n", name)
	}

	if err := waitHealthy(*base, 60*time.Second); err != nil {
		fmt.Printf("[FATAL] service never became healthy: %v\n", err)
		os.Exit(bitBuild | bitPublish | bitLive | bitResume | bitGone | bitFilter)
	}
	fmt.Println("service is healthy")

	run("publish + replay + 409 zero-write", bitPublish, func() error { return checkPublish(*base) })
	run("SSE live delivery + heartbeat", bitLive, func() error { return checkLive(*base) })
	run("SSE resume Last-Event-ID exactly-once", bitResume, func() error { return checkResume(*base) })
	run("expired cursor -> 410", bitGone, func() error { return checkGone(*base) })
	run("SSE severity filter: validation + filtered resume", bitFilter, func() error { return checkFilterResume(*base) })
	run("SSE severity filter: quiet channel advances cursor", bitFilter, func() error { return checkFilterQuiet(*base) })

	fmt.Println()
	if failed != 0 {
		fmt.Printf("SMOKE FAILED, aggregated exit code %d\n", failed)
	} else {
		fmt.Println("SMOKE PASSED")
	}
	os.Exit(failed)
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func waitHealthy(base string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(base + "/health")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 && strings.Contains(string(body), "ok") {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout after %s", timeout)
}

func postEvents(base, channel string, evs []event) (int, postResp, conflictBody, []byte) {
	body, _ := json.Marshal(evs)
	resp, err := http.Post(base+"/events/"+channel, "application/json", bytes.NewReader(body))
	if err != nil {
		return -1, postResp{}, conflictBody{}, []byte(err.Error())
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var pr postResp
	_ = json.Unmarshal(raw, &pr)
	var cb conflictBody
	_ = json.Unmarshal(raw, &cb)
	return resp.StatusCode, pr, cb, raw
}

func mkEvent(key, sev, msg string) event {
	return event{
		EventKey: key,
		Time:     time.Now().UTC().Format(time.RFC3339),
		Severity: sev,
		Message:  msg,
	}
}

func checkPublish(base string) error {
	ch := fmt.Sprintf("smoke-pub-%d", time.Now().UnixNano())

	// Empty and oversized batches must be rejected.
	if code, _, _, _ := postEvents(base, ch, nil); code != http.StatusBadRequest {
		return fmt.Errorf("empty batch: want 400, got %d", code)
	}
	big := make([]event, 51)
	for i := range big {
		big[i] = mkEvent(fmt.Sprintf("big-%d", i), "info", "x")
	}
	if code, _, _, _ := postEvents(base, ch, big); code != http.StatusBadRequest {
		return fmt.Errorf("51-event batch: want 400, got %d", code)
	}

	// First batch: two fresh keys.
	e1 := mkEvent("trip-1", "critical", "beam dump A")
	e2 := mkEvent("warn-1", "warning", "magnet temp high")
	code, first, _, raw := postEvents(base, ch, []event{e1, e2})
	if code != http.StatusOK {
		return fmt.Errorf("first batch: want 200, got %d: %s", code, raw)
	}
	if len(first.Results) != 2 {
		return fmt.Errorf("want 2 results, got %d", len(first.Results))
	}
	if first.Results[0].Replay || first.Results[1].Replay {
		return fmt.Errorf("fresh keys must not be marked replay: %+v", first.Results)
	}
	if first.Results[1].ID != first.Results[0].ID+1 {
		return fmt.Errorf("ids in a batch must be consecutive and ordered: %+v", first.Results)
	}
	id1, id2 := first.Results[0].ID, first.Results[1].ID

	// Identical retry replays the original ids, allocates nothing new.
	code, retry, _, raw := postEvents(base, ch, []event{e1, e2})
	if code != http.StatusOK {
		return fmt.Errorf("retry batch: want 200, got %d: %s", code, raw)
	}
	if retry.Results[0].ID != id1 || retry.Results[1].ID != id2 {
		return fmt.Errorf("retry must replay original ids: first=%+v retry=%+v", first.Results, retry.Results)
	}
	if !retry.Results[0].Replay || !retry.Results[1].Replay {
		return fmt.Errorf("replayed results must carry replay=true: %+v", retry.Results)
	}

	// Changed content -> 409, locatable items, zero writes: a subsequent
	// identical retry must still replay the original ids.
	conflicting := e1
	conflicting.Message = "beam dump A (edited)"
	code, _, cb, raw := postEvents(base, ch, []event{conflicting, e2})
	if code != http.StatusConflict {
		return fmt.Errorf("conflict batch: want 409, got %d: %s", code, raw)
	}
	if len(cb.Items) != 1 || cb.Items[0].EventKey != "trip-1" || cb.Items[0].Index != 0 {
		return fmt.Errorf("409 must pinpoint index/key, got: %s", raw)
	}
	code, after, _, raw := postEvents(base, ch, []event{e1, e2})
	if code != http.StatusOK || after.Results[0].ID != id1 || after.Results[1].ID != id2 {
		return fmt.Errorf("after 409 nothing may have changed: code=%d ids=%+v raw=%s", code, after.Results, raw)
	}

	// A 50-event batch is accepted.
	fifty := make([]event, 50)
	for i := range fifty {
		fifty[i] = mkEvent(fmt.Sprintf("fifty-%d", i), "info", "bulk")
	}
	if code, fiftyResp, _, raw := postEvents(base, ch, fifty); code != http.StatusOK ||
		len(fiftyResp.Results) != 50 || fiftyResp.Results[0].Replay {
		return fmt.Errorf("50-event batch: want 200/50 fresh, got %d %+v %s", code, fiftyResp.Results, raw)
	}
	return nil
}

func checkLive(base string) error {
	ch := fmt.Sprintf("smoke-live-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	frames, err := openStream(ctx, base, ch, 0) // 0 = no Last-Event-ID
	if err != nil {
		return err
	}

	// Heartbeat must arrive while idle within ~5s.
	select {
	case f := <-frames:
		if !f.Comment {
			return fmt.Errorf("expected idle heartbeat comment first on empty channel, got %+v", f)
		}
	case <-time.After(6 * time.Second):
		return fmt.Errorf("no heartbeat within 6s of idle time")
	}

	// Live event after subscribe.
	ev := mkEvent("live-1", "critical", "stopping signal")
	code, pr, _, raw := postEvents(base, ch, []event{ev})
	if code != 200 {
		return fmt.Errorf("publish: %d %s", code, raw)
	}
	select {
	case f := <-frames:
		if f.Event != "event" || f.ID != pr.Results[0].ID || !strings.Contains(f.Data, "stopping signal") {
			return fmt.Errorf("unexpected live frame: %+v", f)
		}
	case <-time.After(5 * time.Second):
		return fmt.Errorf("live event not delivered within 5s")
	}
	return nil
}

func checkResume(base string) error {
	ch := fmt.Sprintf("smoke-resume-%d", time.Now().UnixNano())

	// History before connecting.
	var historyIDs []int64
	for i := 0; i < 3; i++ {
		_, pr, _, _ := postEvents(base, ch, []event{mkEvent(fmt.Sprintf("h-%d", i), "warning", fmt.Sprintf("history %d", i))})
		historyIDs = append(historyIDs, pr.Results[0].ID)
	}

	ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Second)
	frames1, err := openStream(ctx1, base, ch, 0)
	if err != nil {
		cancel1()
		return err
	}
	seen := map[int64]bool{}
	var lastID int64
	for len(seen) < 3 {
		select {
		case f := <-frames1:
			if f.Comment {
				continue
			}
			if seen[f.ID] {
				cancel1()
				return fmt.Errorf("duplicate id %d in history", f.ID)
			}
			seen[f.ID] = true
			lastID = f.ID
		case <-time.After(5 * time.Second):
			cancel1()
			return fmt.Errorf("timeout reading history, got %v", seen)
		}
	}
	for i, id := range historyIDs {
		if !seen[id] {
			cancel1()
			return fmt.Errorf("missing history id %d", id)
		}
		_ = i
	}
	cancel1() // "network drop"

	// Events published while disconnected must not be lost.
	var gapIDs []int64
	for i := 0; i < 2; i++ {
		_, pr, _, _ := postEvents(base, ch, []event{mkEvent(fmt.Sprintf("gap-%d", i), "critical", fmt.Sprintf("gap %d", i))})
		gapIDs = append(gapIDs, pr.Results[0].ID)
	}

	// Reconnect exactly like an EventSource: Last-Event-ID = last seen.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	frames2, err := openStream(ctx2, base, ch, lastID)
	if err != nil {
		return err
	}
	for _, want := range gapIDs {
		select {
		case f := <-frames2:
			if f.Comment {
				continue
			}
			if f.ID != want {
				return fmt.Errorf("resume out of order: want %d, got %d", want, f.ID)
			}
			if f.ID <= lastID {
				return fmt.Errorf("resume redelivered history id %d (<= cursor %d)", f.ID, lastID)
			}
			lastID = f.ID
		case <-time.After(5 * time.Second):
			return fmt.Errorf("missing gap event id %d after resume", want)
		}
	}

	// Seamless: a live event published after resume is delivered once.
	_, pr, _, _ := postEvents(base, ch, []event{mkEvent("after-resume", "warning", "post resume")})
	liveID := pr.Results[0].ID
	select {
	case f := <-frames2:
		if f.ID != liveID {
			return fmt.Errorf("post-resume live event: want %d got %d", liveID, f.ID)
		}
	case <-time.After(5 * time.Second):
		return fmt.Errorf("post-resume live event not delivered")
	}
	return nil
}

func checkGone(base string) error {
	ch := fmt.Sprintf("smoke-gone-%d", time.Now().UnixNano())

	_, first, _, _ := postEvents(base, ch, []event{mkEvent("anchor", "info", "anchor")})
	oldCursor := first.Results[0].ID

	// Publish on this channel until retention pushes the anchor out.
	var earliest int64
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		_, _, _, _ = postEvents(base, ch, []event{mkEvent(
			fmt.Sprintf("filler-%d", time.Now().UnixNano()), "info", "filler")})
		code, cb, raw := getStreamStatus(base, ch, oldCursor)
		switch code {
		case http.StatusGone:
			if cb.EarliestAvailable <= oldCursor {
				return fmt.Errorf("earliestAvailableId %d must exceed expired cursor %d", cb.EarliestAvailable, oldCursor)
			}
			earliest = cb.EarliestAvailable
		case http.StatusOK:
			continue
		default:
			return fmt.Errorf("unexpected status %d probing cursor: %s", code, raw)
		}

		// Resync from earliestAvailableId must succeed and only expose the
		// retained window.
		ctx2, cancel2 := context.WithTimeout(context.Background(), 6*time.Second)
		frames2, err := openStream(ctx2, base, ch, earliest)
		if err != nil {
			cancel2()
			return fmt.Errorf("resync from earliestAvailableId failed: %w", err)
		}
		select {
		case f := <-frames2:
			if f.Comment {
				cancel2()
				return fmt.Errorf("expected retained event on resync, got heartbeat")
			}
			if f.ID < earliest {
				cancel2()
				return fmt.Errorf("resync exposed id %d below earliestAvailableId %d", f.ID, earliest)
			}
			cancel2()
			return nil
		case <-time.After(5 * time.Second):
			cancel2()
			return fmt.Errorf("no event delivered after resync")
		}
	}
	return fmt.Errorf("retention window never crossed (publishing could not age out id %d)", oldCursor)
}

// nextFrame returns the next non-comment SSE frame.
func nextFrame(frames <-chan sseEvent, timeout time.Duration) (sseEvent, error) {
	for {
		select {
		case f, ok := <-frames:
			if !ok {
				return sseEvent{}, fmt.Errorf("stream closed while waiting for frame")
			}
			if f.Comment {
				continue
			}
			return f, nil
		case <-time.After(timeout):
			return sseEvent{}, fmt.Errorf("timed out after %s waiting for frame", timeout)
		}
	}
}

// mustCheckpoint asserts f is a checkpoint frame whose frame id equals
// data.throughId, and returns that id.
func mustCheckpoint(f sseEvent) (int64, error) {
	if f.Event != "checkpoint" {
		return 0, fmt.Errorf("want checkpoint frame, got %+v", f)
	}
	var body struct {
		ThroughID int64 `json:"throughId"`
	}
	if err := json.Unmarshal([]byte(f.Data), &body); err != nil {
		return 0, fmt.Errorf("checkpoint data not JSON: %q", f.Data)
	}
	if body.ThroughID != f.ID {
		return 0, fmt.Errorf("checkpoint frame id %d != data.throughId %d", f.ID, body.ThroughID)
	}
	return f.ID, nil
}

// checkFilterResume covers the filtered-stream contract: invalid filters are
// 400s, unwatched history collapses into checkpoints, and a reconnect from a
// checkpoint cursor resumes gaplessly with every id covered at most once.
func checkFilterResume(base string) error {
	ch := fmt.Sprintf("smoke-filt-%d", time.Now().UnixNano())

	// Illegal filters must be rejected before any SSE byte is written.
	for _, q := range []string{
		"severity=",
		"severity=critical&severity=critical",
		"severity=a&severity=b&severity=c&severity=d&severity=e",
	} {
		code, _, raw := getStreamStatusQuery(base, ch, q, 0)
		if code != http.StatusBadRequest {
			return fmt.Errorf("filter %q: want 400, got %d: %s", q, code, raw)
		}
	}

	const query = "severity=critical&severity=warning"
	n := 0
	postSev := func(sev string) (int64, error) {
		n++
		code, pr, _, raw := postEvents(base, ch, []event{mkEvent(
			fmt.Sprintf("f-%d-%d", time.Now().UnixNano(), n), sev, fmt.Sprintf("m%d", n))})
		if code != http.StatusOK || len(pr.Results) != 1 {
			return 0, fmt.Errorf("post severity %s: status %d: %s", sev, code, raw)
		}
		return pr.Results[0].ID, nil
	}

	// Mixed history: skip, show, skip, show, skip.
	sevs := []string{"info", "critical", "info", "warning", "info"}
	ids := make([]int64, 0, len(sevs))
	for _, sev := range sevs {
		id, err := postSev(sev)
		if err != nil {
			return err
		}
		ids = append(ids, id)
	}

	ctx1, cancel1 := context.WithTimeout(context.Background(), 15*time.Second)
	frames1, err := openStreamQuery(ctx1, base, ch, query, 0)
	if err != nil {
		cancel1()
		return err
	}
	// Skipped runs fold into checkpoints around the watched events.
	wantSeq := []struct {
		kind string // "checkpoint" or "event"
		id   int64
	}{
		{"checkpoint", ids[0]},
		{"event", ids[1]},
		{"checkpoint", ids[2]},
		{"event", ids[3]},
		{"checkpoint", ids[4]},
	}
	var covered int64
	for _, w := range wantSeq {
		f, err := nextFrame(frames1, 5*time.Second)
		if err != nil {
			cancel1()
			return err
		}
		if f.ID <= covered {
			cancel1()
			return fmt.Errorf("frame id %d overlaps already-covered %d", f.ID, covered)
		}
		switch w.kind {
		case "checkpoint":
			if _, err := mustCheckpoint(f); err != nil {
				cancel1()
				return err
			}
		default:
			if f.Event != "event" {
				cancel1()
				return fmt.Errorf("want event frame for id %d, got %+v", w.id, f)
			}
		}
		if f.ID != w.id {
			cancel1()
			return fmt.Errorf("%s: want id %d, got %+v", w.kind, w.id, f)
		}
		covered = f.ID
	}
	cancel1() // "network drop": client keeps covered (= ids[4]) as cursor

	// More mixed events land while disconnected.
	gapSkip, err := postSev("info")
	if err != nil {
		return err
	}
	gapShow, err := postSev("critical")
	if err != nil {
		return err
	}

	// Resume from the checkpoint cursor: the skipped gap event arrives only
	// as a checkpoint, the watched one as an event, then live continues.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel2()
	frames2, err := openStreamQuery(ctx2, base, ch, query, covered)
	if err != nil {
		return err
	}
	f, err := nextFrame(frames2, 5*time.Second)
	if err != nil {
		return err
	}
	if through, err := mustCheckpoint(f); err != nil {
		return err
	} else if through != gapSkip {
		return fmt.Errorf("resume checkpoint through %d, want %d", through, gapSkip)
	}
	f, err = nextFrame(frames2, 5*time.Second)
	if err != nil {
		return err
	}
	if f.Event != "event" || f.ID != gapShow {
		return fmt.Errorf("resume event: want id %d, got %+v", gapShow, f)
	}
	liveShow, err := postSev("warning")
	if err != nil {
		return err
	}
	f, err = nextFrame(frames2, 5*time.Second)
	if err != nil {
		return err
	}
	if f.Event != "event" || f.ID != liveShow {
		return fmt.Errorf("post-resume live event: want id %d, got %+v", liveShow, f)
	}
	return nil
}

// checkFilterQuiet proves the original motivation for checkpoints: on a
// channel quiet in the watched severities, the client's resume cursor still
// tracks the newest id, so a reconnect after retention trimmed everything it
// ever received is a 200 — while a genuinely expired cursor stays a 410.
func checkFilterQuiet(base string) error {
	ch := fmt.Sprintf("smoke-quiet-%d", time.Now().UnixNano())
	const query = "severity=critical"
	n := 0
	postNoise := func() (int64, error) {
		n++
		code, pr, _, raw := postEvents(base, ch, []event{mkEvent(
			fmt.Sprintf("q-%d-%d", time.Now().UnixNano(), n), "info", "noise")})
		if code != http.StatusOK || len(pr.Results) != 1 {
			return 0, fmt.Errorf("post noise: status %d: %s", code, raw)
		}
		return pr.Results[0].ID, nil
	}

	anchor, err := postNoise()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	frames, err := openStreamQuery(ctx, base, ch, query, 0)
	if err != nil {
		return err
	}
	// The lone unwatched record arrives as a checkpoint, not an alert.
	f, err := nextFrame(frames, 5*time.Second)
	if err != nil {
		return err
	}
	through, err := mustCheckpoint(f)
	if err != nil {
		return err
	}
	if through != anchor {
		return fmt.Errorf("first checkpoint through %d, want %d", through, anchor)
	}
	cursor := anchor

	// Publish unwatched noise until retention pushes the anchor out. Every
	// publish must move the open stream's checkpoint to the new id.
	deadline := time.Now().Add(30 * time.Second)
	for {
		code, cb, raw := getStreamStatusQuery(base, ch, query, anchor)
		switch code {
		case http.StatusGone:
			if cb.EarliestAvailable <= anchor {
				return fmt.Errorf("filtered 410: earliestAvailableId %d must exceed expired cursor %d",
					cb.EarliestAvailable, anchor)
			}
		case http.StatusOK:
			// Anchor still retained: keep filling the window.
		default:
			return fmt.Errorf("probing filtered anchor cursor: status %d: %s", code, raw)
		}
		if code == http.StatusGone {
			break
		}
		id, err := postNoise()
		if err != nil {
			return err
		}
		f, err := nextFrame(frames, 5*time.Second)
		if err != nil {
			return err
		}
		through, err := mustCheckpoint(f)
		if err != nil {
			return err
		}
		if through != id {
			return fmt.Errorf("checkpoint through %d, want %d", through, id)
		}
		cursor = id
		if time.Now().After(deadline) {
			return fmt.Errorf("anchor id %d never aged out of the retention window", anchor)
		}
	}

	// Reconnect with the last checkpoint cursor: NOT a 410, because the
	// cursor advanced past everything the filter skipped.
	code, _, raw := getStreamStatusQuery(base, ch, query, cursor)
	if code != http.StatusOK {
		return fmt.Errorf("reconnect at checkpoint cursor %d: want 200, got %d: %s", cursor, code, raw)
	}

	// And a watched event after that reconnect is a normal event frame.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	frames2, err := openStreamQuery(ctx2, base, ch, query, cursor)
	if err != nil {
		return err
	}
	code, pr, _, raw := postEvents(base, ch, []event{mkEvent(
		fmt.Sprintf("q-trip-%d", time.Now().UnixNano()), "critical", "real trip")})
	if code != http.StatusOK || len(pr.Results) != 1 {
		return fmt.Errorf("post trip: status %d: %s", code, raw)
	}
	f, err = nextFrame(frames2, 5*time.Second)
	if err != nil {
		return err
	}
	if f.Event != "event" || f.ID != pr.Results[0].ID || !strings.Contains(f.Data, "real trip") {
		return fmt.Errorf("post-reconnect trip frame = %+v", f)
	}
	return nil
}

// getStreamStatus opens the SSE endpoint with a cursor and returns just the
// HTTP status (draining/closing immediately). A 200 response is cancelled at
// once; body is read minimally to obtain error JSON for 410.
func getStreamStatus(base, channel string, after int64) (int, conflictBody, []byte) {
	return getStreamStatusQuery(base, channel, "", after)
}

// getStreamStatusQuery is getStreamStatus with an extra raw query string.
func getStreamStatusQuery(base, channel, query string, after int64) (int, conflictBody, []byte) {
	url := base + "/streams/" + channel
	if query != "" {
		url += "?" + query
	}
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	if after > 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(after, 10))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1, conflictBody{}, []byte(err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		// Live SSE stream: the status alone is what the probe needs; do not
		// block reading an endless body.
		return http.StatusOK, conflictBody{}, nil
	}
	raw, _ := io.ReadAll(resp.Body)
	var cb conflictBody
	_ = json.Unmarshal(raw, &cb)
	return resp.StatusCode, cb, raw
}

// openStream opens an SSE GET and returns a channel of dispatched frames.
// after==0 omits Last-Event-ID entirely. A non-200 status (e.g. 410) is
// returned as an error containing the response body.
func openStream(ctx context.Context, base, channel string, after int64) (<-chan sseEvent, error) {
	return openStreamQuery(ctx, base, channel, "", after)
}

// openStreamQuery is openStream with an extra raw query string (e.g. the
// repeatable severity filter).
func openStreamQuery(ctx context.Context, base, channel, query string, after int64) (<-chan sseEvent, error) {
	url := base + "/streams/" + channel
	if query != "" {
		url += "?" + query
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if after > 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(after, 10))
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("stream status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	out := make(chan sseEvent, 32)
	go func() {
		defer close(out)
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		var cur sseEvent
		var dataLines []string
		flush := func() {
			if cur.Event == "" && cur.ID == 0 && len(dataLines) == 0 {
				return
			}
			cur.Data = strings.Join(dataLines, "\n")
			select {
			case out <- cur:
			case <-ctx.Done():
			}
			cur = sseEvent{}
			dataLines = nil
		}
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				flush()
			case strings.HasPrefix(line, ":"):
				select {
				case out <- sseEvent{Comment: true}:
				case <-ctx.Done():
					return
				}
			case strings.HasPrefix(line, "id:"):
				cur.ID, _ = strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, "id:")), 10, 64)
			case strings.HasPrefix(line, "event:"):
				cur.Event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			case strings.HasPrefix(line, "data:"):
				dataLines = append(dataLines, strings.TrimPrefix(line, "data:"))
				if len(dataLines) > 0 {
					dataLines[len(dataLines)-1] = strings.TrimPrefix(dataLines[len(dataLines)-1], " ")
				}
			}
		}
	}()
	return out, nil
}
