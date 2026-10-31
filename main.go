// arashonline collects v2ray configs from subscription links, tests them with
// Xray-core, ranks them, and writes ready-to-import subscription files.
//
// Pipeline:
//  1. fetch every URL in sourcelist.txt, decode, parse, de-duplicate
//  2. alive test through gstatic generate_204, keep ping <= max-ping, sort by ping
//  3. country lookup + download speed test, apply the speed filter
//  4. rename remarks, write arashonline.txt (by ping) and
//     arashonline-highspeed.txt (top N by speed)
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ───────────────────────────── settings ─────────────────────────────

type settings struct {
	sources      string
	xrayBin      string
	outDir       string
	pingURL      string
	speedURL     string
	workers      int
	speedWorkers int
	speedSecs    int
	topN         int
	maxPing      time.Duration
	minMbps      float64
	maxMbps      float64
}

func parseFlags() settings {
	var s settings
	var maxPingMs int
	flag.StringVar(&s.sources, "sources", "sourcelist.txt", "file with subscription URLs, one per line")
	flag.StringVar(&s.xrayBin, "xray", "xray", "path to the xray binary")
	flag.StringVar(&s.outDir, "out", ".", "directory for the output files")
	flag.StringVar(&s.pingURL, "ping-url", "http://www.gstatic.com/generate_204", "alive-test URL (must answer 204)")
	flag.StringVar(&s.speedURL, "speed-url", "https://speed.cloudflare.com/__down?bytes=50000000", "download speed-test URL")
	flag.IntVar(&s.workers, "workers", 30, "parallel xray processes for the ping stage")
	flag.IntVar(&s.speedWorkers, "speed-workers", 4, "parallel speed tests (keep low, they share bandwidth)")
	flag.IntVar(&s.speedSecs, "speed-secs", 8, "max seconds of download per config")
	flag.IntVar(&s.topN, "top", 50, "how many configs go to arashonline-highspeed.txt")
	flag.IntVar(&maxPingMs, "max-ping", 3000, "drop configs slower than this many ms")
	flag.Float64Var(&s.minMbps, "min-speed", 0, "drop configs slower than this many Mbps (0 = off)")
	flag.Float64Var(&s.maxMbps, "max-speed", 50, "drop configs faster than this many Mbps (0 = off)")
	flag.Parse()
	s.maxPing = time.Duration(maxPingMs) * time.Millisecond
	return s
}

func main() {
	log.SetFlags(log.Ltime)
	s := parseFlags()

	if _, err := exec.LookPath(s.xrayBin); err != nil {
		log.Fatalf("xray binary not found (%q): %v", s.xrayBin, err)
	}
	sources, err := readSources(s.sources)
	if err != nil {
		log.Fatal(err)
	}
	if len(sources) == 0 {
		log.Fatalf("%s has no subscription URLs", s.sources)
	}

	nodes := collect(sources)
	log.Printf("collected %d unique configs from %d sources", len(nodes), len(sources))
	if len(nodes) == 0 {
		log.Fatal("no configs found")
	}

	alive := stagePing(s, nodes)
	log.Printf("alive with ping <= %dms: %d", s.maxPing.Milliseconds(), len(alive))

	final := stageSpeed(s, alive)
	log.Printf("passed speed filter: %d", len(final))
	if len(final) == 0 {
		log.Fatal("no configs survived; leaving existing output files untouched")
	}

	sort.SliceStable(final, func(i, j int) bool { return final[i].PingMs < final[j].PingMs })
	if err := writeOutputs(s, final); err != nil {
		log.Fatal(err)
	}
}

// ───────────────────────────── fetching ─────────────────────────────

var httpClient = &http.Client{Timeout: 45 * time.Second}

func readSources(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(strings.TrimPrefix(sc.Text(), "\ufeff"))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, line)
	}
	return out, sc.Err()
}

func fetch(u string) ([]byte, error) {
	if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
		return nil, errors.New("not an http(s) URL")
	}
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; arashonline)")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 32<<20))
}

var schemes = []string{"vmess://", "vless://", "trojan://", "ss://"}

// extractLinks accepts a plain-text or base64 subscription body.
func extractLinks(body []byte) []string {
	text := strings.TrimSpace(string(body))
	if !strings.Contains(text, "://") {
		if dec, ok := decodeB64(text); ok {
			text = dec
		}
	}
	var out []string
	for _, line := range strings.FieldsFunc(text, func(r rune) bool { return r == '\n' || r == '\r' }) {
		line = strings.TrimSpace(line)
		lower := strings.ToLower(line)
		for _, sc := range schemes {
			if strings.HasPrefix(lower, sc) {
				out = append(out, line)
				break
			}
		}
	}
	return out
}

func decodeB64(s string) (string, bool) {
	s = strings.Map(func(r rune) rune {
		if r == ' ' || r == '\n' || r == '\r' || r == '\t' {
			return -1
		}
		return r
	}, s)
	s = strings.TrimRight(s, "=")
	for _, enc := range []*base64.Encoding{base64.RawStdEncoding, base64.RawURLEncoding} {
		if b, err := enc.DecodeString(s); err == nil {
			return string(b), true
		}
	}
	return "", false
}

func collect(sources []string) []*Node {
	var (
		mu    sync.Mutex
		wg    sync.WaitGroup
		links []string
	)
	sem := make(chan struct{}, 8)
	for _, u := range sources {
		wg.Add(1)
		sem <- struct{}{}
		go func(u string) {
			defer wg.Done()
			defer func() { <-sem }()
			body, err := fetch(u)
			if err != nil {
				log.Printf("source failed: %s: %v", u, err)
				return
			}
			ls := extractLinks(body)
			log.Printf("source ok: %s: %d links", u, len(ls))
			mu.Lock()
			links = append(links, ls...)
			mu.Unlock()
		}(u)
	}
	wg.Wait()

	seen := make(map[string]bool)
	var nodes []*Node
	bad := 0
	for _, l := range links {
		n, err := parseLink(l)
		if err != nil {
			bad++
			continue
		}
		if seen[n.key] {
			continue
		}
		seen[n.key] = true
		nodes = append(nodes, n)
	}
	if bad > 0 {
		log.Printf("skipped %d unparsable or unsupported links", bad)
	}
	return nodes
}

// ───────────────────────────── parsing ─────────────────────────────

// Node is one proxy config plus its test results.
type Node struct {
	Base     string         // original link without remark (vless/trojan/ss)
	VMess    map[string]any // decoded vmess JSON (vmess only)
	Host     string
	Port     int
	Outbound map[string]any // Xray outbound used for testing
	key      string         // de-duplication key

	PingMs  int
	Mbps    float64
	Country string
	Line    string // final link with the new remark
}

func (n *Node) withRemark(remark string) string {
	if n.VMess != nil {
		m := make(map[string]any, len(n.VMess)+1)
		for k, v := range n.VMess {
			m[k] = v
		}
		m["ps"] = remark
		return "vmess://" + base64.StdEncoding.EncodeToString(marshal(m))
	}
	return n.Base + "#" + url.PathEscape(remark)
}

func parseLink(link string) (*Node, error) {
	i := strings.Index(link, "://")
	if i < 0 {
		return nil, errors.New("no scheme")
	}
	switch strings.ToLower(link[:i]) {
	case "vmess":
		return parseVMess(link)
	case "vless":
		return parseURL("vless", link)
	case "trojan":
		return parseURL("trojan", link)
	case "ss":
		return parseSS(link)
	}
	return nil, errors.New("unsupported scheme")
}

func parseVMess(link string) (*Node, error) {
	raw, ok := decodeB64(stripFragment(link)[len("vmess://"):])
	if !ok {
		return nil, errors.New("vmess: bad base64")
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	host, id := field(m, "add"), field(m, "id")
	port, err := strconv.Atoi(field(m, "port"))
	if host == "" || id == "" || err != nil || port < 1 || port > 65535 {
		return nil, errors.New("vmess: missing fields")
	}
	aid, _ := strconv.Atoi(field(m, "aid"))
	scy := first(field(m, "scy"), "auto")

	st := stream{
		network:    field(m, "net"),
		host:       field(m, "host"),
		path:       field(m, "path"),
		sni:        field(m, "sni"),
		alpn:       field(m, "alpn"),
		fp:         field(m, "fp"),
		headerType: field(m, "type"),
		insecure:   truthy(field(m, "allowInsecure")),
	}
	if st.network == "grpc" {
		st.service = st.path
		st.mode = st.headerType // for gRPC, "type" carries gun/multi
	}
	if field(m, "tls") == "tls" {
		st.security = "tls"
		st.sni = first(st.sni, st.host)
	}

	out := map[string]any{
		"tag":      "proxy",
		"protocol": "vmess",
		"settings": map[string]any{"vnext": []any{map[string]any{
			"address": host,
			"port":    port,
			"users":   []any{map[string]any{"id": id, "alterId": aid, "security": scy}},
		}}},
		"streamSettings": st.build(),
	}

	// de-dup key ignores the remark
	cp := make(map[string]any, len(m))
	for k, v := range m {
		if k != "ps" {
			cp[k] = v
		}
	}
	return &Node{VMess: m, Host: host, Port: port, Outbound: out, key: "vmess:" + string(marshal(cp))}, nil
}

func parseURL(scheme, link string) (*Node, error) {
	base := stripFragment(link)
	u, err := url.Parse(base)
	if err != nil {
		return nil, err
	}
	host := u.Hostname()
	port, perr := strconv.Atoi(u.Port())
	if host == "" || perr != nil || port < 1 || port > 65535 || u.User == nil {
		return nil, errors.New(scheme + ": bad address or credentials")
	}
	cred := u.User.Username()
	if pw, ok := u.User.Password(); ok {
		cred += ":" + pw
	}
	q := u.Query()
	st := stream{
		network:    q.Get("type"),
		security:   strings.ToLower(q.Get("security")),
		sni:        first(q.Get("sni"), q.Get("peer")),
		host:       q.Get("host"),
		path:       q.Get("path"),
		alpn:       q.Get("alpn"),
		fp:         q.Get("fp"),
		service:    q.Get("serviceName"),
		mode:       q.Get("mode"),
		headerType: q.Get("headerType"),
		pbk:        q.Get("pbk"),
		sid:        q.Get("sid"),
		spx:        q.Get("spx"),
		insecure:   truthy(first(q.Get("allowInsecure"), q.Get("insecure"))),
	}

	out := map[string]any{"tag": "proxy", "protocol": scheme}
	switch scheme {
	case "vless":
		user := map[string]any{"id": cred, "encryption": first(q.Get("encryption"), "none")}
		if f := q.Get("flow"); f != "" {
			user["flow"] = f
		}
		out["settings"] = map[string]any{"vnext": []any{map[string]any{
			"address": host, "port": port, "users": []any{user},
		}}}
	case "trojan":
		if st.security == "" {
			st.security = "tls"
		}
		out["settings"] = map[string]any{"servers": []any{map[string]any{
			"address": host, "port": port, "password": cred,
		}}}
	}
	out["streamSettings"] = st.build()
	return &Node{Base: base, Host: host, Port: port, Outbound: out, key: base}, nil
}

func parseSS(link string) (*Node, error) {
	base := stripFragment(link)
	rest := base[len("ss://"):]
	if i := strings.Index(rest, "?"); i >= 0 {
		if strings.Contains(rest[i:], "plugin=") {
			return nil, errors.New("ss: plugins unsupported")
		}
		rest = rest[:i]
	}
	rest = strings.TrimSuffix(rest, "/")

	var creds, hostport string
	if at := strings.LastIndex(rest, "@"); at >= 0 {
		creds, hostport = rest[:at], rest[at+1:]
		if un, err := url.PathUnescape(creds); err == nil {
			creds = un
		}
		if dec, ok := decodeB64(creds); ok && strings.Contains(dec, ":") {
			creds = dec
		}
	} else {
		dec, ok := decodeB64(rest)
		if !ok {
			return nil, errors.New("ss: bad base64")
		}
		at := strings.LastIndex(dec, "@")
		if at < 0 {
			return nil, errors.New("ss: bad format")
		}
		creds, hostport = dec[:at], dec[at+1:]
	}

	method, pass, ok := strings.Cut(creds, ":")
	host, ps, err := net.SplitHostPort(hostport)
	if !ok || err != nil {
		return nil, errors.New("ss: bad credentials or address")
	}
	port, err := strconv.Atoi(ps)
	if err != nil || port < 1 || port > 65535 || host == "" {
		return nil, errors.New("ss: bad port")
	}
	out := map[string]any{
		"tag":      "proxy",
		"protocol": "shadowsocks",
		"settings": map[string]any{"servers": []any{map[string]any{
			"address": host, "port": port, "method": method, "password": pass,
		}}},
	}
	return &Node{Base: base, Host: host, Port: port, Outbound: out, key: base}, nil
}

// stream holds transport/security options shared by every protocol.
type stream struct {
	network, security, sni, host, path, alpn, fp string
	service, mode, headerType, pbk, sid, spx     string
	insecure                                     bool
}

func (st stream) build() map[string]any {
	network := strings.ToLower(st.network)
	switch network {
	case "", "raw":
		network = "tcp"
	case "h2":
		network = "http"
	case "splithttp":
		network = "xhttp"
	}
	out := map[string]any{"network": network}

	switch network {
	case "tcp":
		if st.headerType == "http" {
			req := map[string]any{"path": strings.Split(first(st.path, "/"), ",")}
			if st.host != "" {
				req["headers"] = map[string]any{"Host": strings.Split(st.host, ",")}
			}
			out["tcpSettings"] = map[string]any{"header": map[string]any{"type": "http", "request": req}}
		}
	case "ws":
		w := map[string]any{}
		if st.path != "" {
			w["path"] = st.path
		}
		if st.host != "" {
			w["headers"] = map[string]any{"Host": st.host}
		}
		out["wsSettings"] = w
	case "grpc":
		out["grpcSettings"] = map[string]any{"serviceName": st.service, "multiMode": st.mode == "multi"}
	case "http":
		h := map[string]any{}
		if st.path != "" {
			h["path"] = st.path
		}
		if st.host != "" {
			h["host"] = strings.Split(st.host, ",")
		}
		out["httpSettings"] = h
	case "httpupgrade":
		h := map[string]any{}
		if st.path != "" {
			h["path"] = st.path
		}
		if st.host != "" {
			h["host"] = st.host
		}
		out["httpupgradeSettings"] = h
	case "xhttp":
		h := map[string]any{}
		if st.path != "" {
			h["path"] = st.path
		}
		if st.host != "" {
			h["host"] = st.host
		}
		if st.mode != "" {
			h["mode"] = st.mode
		}
		out["xhttpSettings"] = h
	case "kcp":
		k := map[string]any{"header": map[string]any{"type": first(st.headerType, "none")}}
		if st.path != "" {
			k["seed"] = st.path
		}
		out["kcpSettings"] = k
	}

	switch st.security {
	case "tls":
		t := map[string]any{"allowInsecure": st.insecure}
		if st.sni != "" {
			t["serverName"] = st.sni
		}
		if st.fp != "" {
			t["fingerprint"] = st.fp
		}
		if st.alpn != "" {
			t["alpn"] = strings.Split(st.alpn, ",")
		}
		out["security"] = "tls"
		out["tlsSettings"] = t
	case "reality":
		out["security"] = "reality"
		out["realitySettings"] = map[string]any{
			"serverName":  st.sni,
			"fingerprint": first(st.fp, "chrome"),
			"publicKey":   st.pbk,
			"shortId":     st.sid,
			"spiderX":     st.spx,
		}
	}
	return out
}

// ───────────────────────────── helpers ─────────────────────────────

func stripFragment(s string) string {
	if i := strings.Index(s, "#"); i >= 0 {
		return s[:i]
	}
	return s
}

func first(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func truthy(s string) bool { return s == "1" || strings.EqualFold(s, "true") }

func field(m map[string]any, k string) string {
	switch v := m[k].(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(v)
	case json.Number:
		return v.String()
	default:
		return fmt.Sprint(v)
	}
}

// marshal is json.Marshal without HTML escaping.
func marshal(v any) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
	return bytes.TrimSpace(buf.Bytes())
}

func flagEmoji(cc string) string {
	cc = strings.ToUpper(strings.TrimSpace(cc))
	if len(cc) != 2 || cc[0] < 'A' || cc[0] > 'Z' || cc[1] < 'A' || cc[1] > 'Z' {
		return "🌐"
	}
	return string([]rune{rune(0x1F1E6 + int(cc[0]-'A')), rune(0x1F1E6 + int(cc[1]-'A'))})
}

func parallel(nodes []*Node, workers int, fn func(*Node)) {
	if workers < 1 {
		workers = 1
	}
	ch := make(chan *Node)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range ch {
				fn(n)
			}
		}()
	}
	for _, n := range nodes {
		ch <- n
	}
	close(ch)
	wg.Wait()
}

func progress(stage string, done *int32, total int) {
	n := atomic.AddInt32(done, 1)
	if n%100 == 0 || int(n) == total {
		log.Printf("%s: %d/%d", stage, n, total)
	}
}

// ───────────────────────────── xray process ─────────────────────────────

var portCounter uint32

type proxy struct {
	cmd  *exec.Cmd
	port int
	file string
	done chan struct{}
}

// startProxy launches xray with a local SOCKS5 inbound routed to the node.
func startProxy(bin string, n *Node) (*proxy, error) {
	port := 10000 + int(atomic.AddUint32(&portCounter, 1)%20000)
	cfg := map[string]any{
		"log": map[string]any{"loglevel": "none"},
		"inbounds": []any{map[string]any{
			"listen":   "127.0.0.1",
			"port":     port,
			"protocol": "socks",
			"settings": map[string]any{"auth": "noauth", "udp": false},
		}},
		"outbounds": []any{n.Outbound},
	}
	f, err := os.CreateTemp("", "xray-*.json")
	if err != nil {
		return nil, err
	}
	_, werr := f.Write(marshal(cfg))
	f.Close()
	if werr != nil {
		os.Remove(f.Name())
		return nil, werr
	}

	cmd := exec.Command(bin, "run", "-c", f.Name())
	if err := cmd.Start(); err != nil {
		os.Remove(f.Name())
		return nil, err
	}
	p := &proxy{cmd: cmd, port: port, file: f.Name(), done: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		close(p.done)
	}()

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-p.done:
			p.stop()
			return nil, errors.New("xray exited (invalid config?)")
		default:
		}
		if c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
			c.Close()
			return p, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	p.stop()
	return nil, errors.New("xray start timeout")
}

func (p *proxy) stop() {
	_ = p.cmd.Process.Kill()
	<-p.done
	_ = os.Remove(p.file)
}

func (p *proxy) client(timeout time.Duration) (*http.Client, *http.Transport) {
	tr := &http.Transport{
		Proxy:              http.ProxyURL(&url.URL{Scheme: "socks5", Host: fmt.Sprintf("127.0.0.1:%d", p.port)}),
		DisableCompression: true,
		MaxIdleConns:       2,
	}
	c := &http.Client{
		Transport:     tr,
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return c, tr
}

// ───────────────────────────── stage 1: ping ─────────────────────────────

func timedGet(c *http.Client, target string) (time.Duration, error) {
	t0 := time.Now()
	resp, err := c.Get(target)
	if err != nil {
		return 0, err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	d := time.Since(t0)
	if resp.StatusCode != http.StatusNoContent {
		return 0, fmt.Errorf("status %d", resp.StatusCode)
	}
	return d, nil
}

// measurePing: the first request opens the connection (must succeed, so the
// node counts as alive); the next two reuse it, so the best of those is the
// real round-trip through the proxy.
func measurePing(p *proxy, target string, limit time.Duration) (time.Duration, bool) {
	c, tr := p.client(2 * limit)
	defer tr.CloseIdleConnections()

	firstTry, err := timedGet(c, target)
	if err != nil {
		return 0, false
	}
	best := time.Duration(0)
	for i := 0; i < 2; i++ {
		if d, err := timedGet(c, target); err == nil && (best == 0 || d < best) {
			best = d
		}
	}
	if best == 0 {
		best = firstTry
	}
	return best, true
}

func stagePing(s settings, nodes []*Node) []*Node {
	var (
		mu    sync.Mutex
		alive []*Node
		done  int32
	)
	parallel(nodes, s.workers, func(n *Node) {
		defer progress("ping", &done, len(nodes))
		p, err := startProxy(s.xrayBin, n)
		if err != nil {
			return
		}
		defer p.stop()
		d, ok := measurePing(p, s.pingURL, s.maxPing)
		if !ok || d > s.maxPing {
			return
		}
		n.PingMs = int(d.Milliseconds())
		mu.Lock()
		alive = append(alive, n)
		mu.Unlock()
	})
	sort.SliceStable(alive, func(i, j int) bool { return alive[i].PingMs < alive[j].PingMs })
	return alive
}

// ───────────────────────────── stage 2: speed ─────────────────────────────

// lookupCountry returns the exit country code as seen through the proxy.
func lookupCountry(p *proxy) string {
	c, tr := p.client(10 * time.Second)
	defer tr.CloseIdleConnections()
	for _, u := range []string{"https://www.cloudflare.com/cdn-cgi/trace", "https://ipinfo.io/country"} {
		resp, err := c.Get(u)
		if err != nil {
			continue
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		txt := string(b)
		if i := strings.Index(txt, "loc="); i >= 0 {
			if f := strings.Fields(txt[i+4:]); len(f) > 0 {
				return strings.ToUpper(f[0])
			}
		}
		if t := strings.TrimSpace(txt); len(t) == 2 {
			return strings.ToUpper(t)
		}
	}
	return ""
}

// measureSpeed downloads for up to secs seconds and returns Mbps (0 = failed).
func measureSpeed(p *proxy, target string, secs int) float64 {
	c, tr := p.client(0)
	defer tr.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(secs+8)*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return 0
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0
	}

	buf := make([]byte, 64<<10)
	var total int64
	var start time.Time
	limit := time.Duration(secs) * time.Second
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if start.IsZero() {
				start = time.Now()
			}
			total += int64(n)
		}
		if err != nil || (!start.IsZero() && time.Since(start) >= limit) {
			break
		}
	}
	if start.IsZero() || total < 256<<10 {
		return 0
	}
	elapsed := time.Since(start).Seconds()
	if elapsed < 0.2 {
		elapsed = 0.2
	}
	return float64(total) * 8 / elapsed / 1e6
}

func stageSpeed(s settings, nodes []*Node) []*Node {
	var done int32
	parallel(nodes, s.speedWorkers, func(n *Node) {
		defer progress("speed", &done, len(nodes))
		p, err := startProxy(s.xrayBin, n)
		if err != nil {
			return
		}
		defer p.stop()
		n.Country = lookupCountry(p)
		n.Mbps = measureSpeed(p, s.speedURL, s.speedSecs)
	})

	var out []*Node
	for _, n := range nodes {
		if n.Mbps <= 0 || n.Mbps < s.minMbps {
			continue
		}
		if s.maxMbps > 0 && n.Mbps > s.maxMbps {
			continue
		}
		out = append(out, n)
	}
	return out
}

// ───────────────────────────── output ─────────────────────────────

func writeLines(path string, lines []string) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// writeOutputs expects nodes already sorted by ping (lowest first).
func writeOutputs(s settings, nodes []*Node) error {
	lines := make([]string, len(nodes))
	for i, n := range nodes {
		remark := fmt.Sprintf("%d | %s | %s | %dms | %.1fMbps",
			i+1, flagEmoji(n.Country), net.JoinHostPort(n.Host, strconv.Itoa(n.Port)), n.PingMs, n.Mbps)
		n.Line = n.withRemark(remark)
		lines[i] = n.Line
	}
	if err := writeLines(filepath.Join(s.outDir, "arashonline.txt"), lines); err != nil {
		return err
	}

	bySpeed := append([]*Node(nil), nodes...)
	sort.SliceStable(bySpeed, func(i, j int) bool { return bySpeed[i].Mbps > bySpeed[j].Mbps })
	if len(bySpeed) > s.topN {
		bySpeed = bySpeed[:s.topN]
	}
	top := make([]string, len(bySpeed))
	for i, n := range bySpeed {
		top[i] = n.Line
	}
	log.Printf("wrote arashonline.txt (%d) and arashonline-highspeed.txt (%d)", len(lines), len(top))
	return writeLines(filepath.Join(s.outDir, "arashonline-highspeed.txt"), top)
}
