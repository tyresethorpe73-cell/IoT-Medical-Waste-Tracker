package main

import (
	"database/sql"
	"embed"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

// --- embed templates ---
//
//go:embed templates/*
var templateFS embed.FS
var tpl = template.Must(template.ParseFS(templateFS, "templates/*.html"))

var db *sql.DB

// ========== LIVE (in-memory only; NOT persisted) ==========
type liveReading struct {
	BinID     int       `json:"bin_id"`
	Temp      *float64  `json:"temp_c"`
	Humidity  *float64  `json:"humidity_pct"`
	DoorOpen  *int      `json:"door_open"` // 1=open, 0=closed (live only)
	LastSeen  time.Time `json:"-"`         // internal
	LastSeenS string    `json:"last_seen"` // RFC3339 UTC for dashboard
}

var (
	liveMu    sync.RWMutex
	liveByBin = map[int]liveReading{} // bin_id -> latest live reading
)

// ========== SSE (push updates to dashboard) ==========
type sseMsg struct {
	Type string      `json:"type"` // "telemetry" | "snapshot"
	Data interface{} `json:"data"`
}

var (
	sseMu      sync.RWMutex
	sseClients = map[chan []byte]struct{}{}
)

func sseBroadcast(msg sseMsg) {
	b, _ := json.Marshal(msg)
	b = append([]byte("data: "), b...)
	b = append(b, []byte("\n\n")...)

	sseMu.RLock()
	defer sseMu.RUnlock()
	for ch := range sseClients {
		select {
		case ch <- b:
		default:
			// slow client; drop message
		}
	}
}

// --- CORS helper (same-file UI or cross-origin) ---
func allowCORS(w http.ResponseWriter, r *http.Request) bool {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	return false
}

func handleBinsLiveSSE(w http.ResponseWriter, r *http.Request) {
	if allowCORS(w, r) {
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // proxy/CDN friendliness

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	// new client channel (buffered)
	ch := make(chan []byte, 64)

	// register
	sseMu.Lock()
	sseClients[ch] = struct{}{}
	sseMu.Unlock()

	// cleanup on exit
	defer func() {
		sseMu.Lock()
		delete(sseClients, ch)
		sseMu.Unlock()
		close(ch)
	}()

	ctx := r.Context()

	// initial comment + flush
	_, _ = w.Write([]byte(": connected\n\n"))
	flusher.Flush()

	// send initial snapshot so client sees something immediately
	go func() {
		liveMu.RLock()
		type snapRow struct {
			BinID      int      `json:"bin_id"`
			Temp       *float64 `json:"temp_c"`
			Humidity   *float64 `json:"humidity_pct"`
			DoorOpen   *int     `json:"door_open"`
			DoorStatus string   `json:"door_status"`
			LastSeen   string   `json:"last_seen"`
		}
		var all []snapRow
		for _, lr := range liveByBin {
			all = append(all, snapRow{
				BinID: lr.BinID, Temp: lr.Temp, Humidity: lr.Humidity,
				DoorOpen: lr.DoorOpen, DoorStatus: doorText(lr.DoorOpen), LastSeen: lr.LastSeenS,
			})
		}
		liveMu.RUnlock()
		msg := sseMsg{Type: "snapshot", Data: map[string]any{"bins": all, "ts": utcNow().Format(time.RFC3339)}}
		b, _ := json.Marshal(msg)
		ch <- append(append([]byte("data: "), b...), []byte("\n\n")...)
	}()

	heartbeat := time.NewTicker(20 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			_, _ = w.Write([]byte(": ping\n\n"))
			flusher.Flush()
		case b := <-ch:
			_, _ = w.Write(b)
			flusher.Flush()
		}
	}
}

func utcNow() time.Time { return time.Now().UTC() }

// doorText converts 0/1 pointer to user label
func doorText(v *int) string {
	if v == nil {
		return ""
	}
	if *v == 1 {
		return "Open"
	}
	return "Closed"
}

// ========== MAIN ==========
func main() {
	var err error
	// ✅ UTC everywhere
	dsn := "tracker:StrongLocalPass!123@tcp(127.0.0.1:3306)/iot_medical_waste_tracker?parseTime=true&loc=UTC"

	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	if err = db.Ping(); err != nil {
		log.Fatal("❌ Database connection failed:", err)
	}
	if _, err := db.Exec(`SET time_zone = '+00:00'`); err != nil {
		log.Fatal("❌ Failed to set MySQL time_zone to UTC:", err)
	}
	fmt.Println("✅ Connected to MySQL successfully (UTC mode)!")

	// Chart JSON for dashboard (register ONCE)
	http.HandleFunc("/admin/chart.json", handleAdminChartJSON)

	// Reference data
	http.HandleFunc("/streams", handleListStreams)                 // GET
	http.HandleFunc("/departments", handleListDepartments)         // GET
	http.HandleFunc("/departments/classes", handleListDepartments) // alias
	http.HandleFunc("/bins", handleListBins)                       // GET ?stream_code=..
	http.HandleFunc("/bins/summary", handleBinsSummary)            // GET (includes live T/H/Door)

	// Admin dashboard
	http.HandleFunc("/admin", handleAdmin)
	http.HandleFunc("/admin/export.csv", handleAdminExport)

	// Employees (GET list, POST create)
	http.HandleFunc("/employees", handleEmployees)

	// Auth & UUID lifecycle
	http.HandleFunc("/auth/pin", handleAuthPIN)                       // POST {pin}
	http.HandleFunc("/uuid/create/from-pin", handleUUIDCreateFromPIN) // POST {pin, stream_code}
	http.HandleFunc("/uuid/create/legacy", handleUUIDCreate)          // POST {emp_id, dept_id, stream_code, [bin_id]}
	http.HandleFunc("/bin/consume", handleBinConsume)                 // POST {bin_id, uuid}

	// 🔹 Telemetry (realtime in memory + daily averages in DB)
	http.HandleFunc("/bin/telemetry", handleBinTelemetry)  // POST {bin_id, temp, humidity, door_open}
	http.HandleFunc("/bins/live.json", handleBinsLiveJSON) // GET snapshot (polling)
	http.HandleFunc("/bins/live.sse", handleBinsLiveSSE)   // GET Server-Sent Events (push)
	http.HandleFunc("/bins/live.peek", handleBinsLivePeek) // GET simple smoke test

	// Tiny client JS to wire up SSE on any page
	http.HandleFunc("/static/live.js", handleLiveJS)

	// Demo route
	http.HandleFunc("/generate_uuid", handleGenerateUUID)

	fmt.Println("🚀 Server running at http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

/* ---------- helpers ---------- */

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
func bad(w http.ResponseWriter, code int, msg string) {
	http.Error(w, msg, code)
}

// buildFilters builds a WHERE clause + args from URL params for admin views.
func buildFilters(r *http.Request) (string, []any) {
	where := " WHERE 1=1 "
	args := []any{}

	// department filter (now via employees join in admin queries)
	if d := r.URL.Query().Get("dept"); d != "" {
		where += " AND d.dept_id = ? "
		args = append(args, d)
	}

	// status filter (UTC)
	switch r.URL.Query().Get("status") {
	case "active":
		where += " AND ul.is_used = 0 AND (ul.expires_at IS NULL OR UTC_TIMESTAMP() < ul.expires_at) "
	case "used":
		where += " AND ul.is_used = 1 "
	case "expired":
		where += " AND ul.is_used = 0 AND ul.expires_at IS NOT NULL AND UTC_TIMESTAMP() > ul.expires_at "
	}

	// recent days window based on generated_at (rolling)
	if days := r.URL.Query().Get("days"); days != "" {
		where += " AND ul.generated_at >= (UTC_TIMESTAMP() - INTERVAL ? DAY) "
		args = append(args, days)
	}

	return where, args
}

/* ---------- reference data ---------- */

// One row per stream plus an esp32_mac sample (first non-empty via MAX()).
func handleListStreams(w http.ResponseWriter, r *http.Request) {
	if allowCORS(w, r) {
		return
	}
	rows, err := db.Query(`
		SELECT
			ws.waste_stream_id,
			ws.code,
			ws.name,
			ws.hazardous,
			COALESCE(NULLIF(MAX(b.esp32_mac), ''), '') AS esp32_mac
		FROM waste_streams ws
		LEFT JOIN bins b ON b.waste_stream_id = ws.waste_stream_id
		GROUP BY ws.waste_stream_id, ws.code, ws.name, ws.hazardous
		ORDER BY ws.hazardous DESC, ws.name
	`)
	if err != nil {
		bad(w, 500, "db")
		return
	}
	defer rows.Close()

	type row struct {
		ID        int    `json:"id"`
		Code      string `json:"code"`
		Name      string `json:"name"`
		Hazardous bool   `json:"hazardous"`
		ESP32MAC  string `json:"esp32_mac"`
	}
	var out []row
	for rows.Next() {
		var rr row
		var hz int
		if err := rows.Scan(&rr.ID, &rr.Code, &rr.Name, &hz, &rr.ESP32MAC); err != nil {
			bad(w, 500, "scan")
			return
		}
		rr.Hazardous = hz == 1
		out = append(out, rr)
	}
	writeJSON(w, out)
}

// Include dept code if present; fallback if column missing. Sort by dept_id ASC.
func handleListDepartments(w http.ResponseWriter, r *http.Request) {
	if allowCORS(w, r) {
		return
	}
	type row struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
		Code string `json:"code"`
	}

	rows, err := db.Query(`
		SELECT dept_id, dept_name, COALESCE(dept_code, '')
		FROM departments
		ORDER BY dept_id ASC
	`)
	if err != nil {
		// Fallback if dept_code doesn't exist
		rows, err = db.Query(`
			SELECT dept_id, dept_name
			FROM departments
			ORDER BY dept_id ASC
		`)
		if err != nil {
			bad(w, 500, "db")
			return
		}
		defer rows.Close()

		var out []row
		for rows.Next() {
			var rr row
			if err := rows.Scan(&rr.ID, &rr.Name); err != nil {
				bad(w, 500, "scan")
				return
			}
			rr.Code = ""
			out = append(out, rr)
		}
		writeJSON(w, map[string]any{"departments": out})
		return
	}
	defer rows.Close()

	var out []row
	for rows.Next() {
		var rr row
		if err := rows.Scan(&rr.ID, &rr.Name, &rr.Code); err != nil {
			bad(w, 500, "scan")
			return
		}
		out = append(out, rr)
	}
	writeJSON(w, map[string]any{"departments": out})
}

func handleListBins(w http.ResponseWriter, r *http.Request) {
	if allowCORS(w, r) {
		return
	}
	q := `SELECT b.bin_id, b.bin_name, b.location, ws.code
	      FROM bins b
	      JOIN waste_streams ws ON ws.waste_stream_id=b.waste_stream_id
	      WHERE 1=1`
	args := []any{}

	if streamCode := r.URL.Query().Get("stream_code"); streamCode != "" {
		q += " AND ws.code = ?"
		args = append(args, streamCode)
	}
	q += " ORDER BY b.bin_name"

	rows, err := db.Query(q, args...)
	if err != nil {
		bad(w, 500, "db")
		return
	}
	defer rows.Close()

	type row struct {
		BinID    int    `json:"bin_id"`
		BinName  string `json:"bin_name"`
		Location string `json:"location"`
		Stream   string `json:"stream_code"`
	}
	var out []row
	for rows.Next() {
		var rr row
		_ = rows.Scan(&rr.BinID, &rr.BinName, &rr.Location, &rr.Stream)
		out = append(out, rr)
	}
	writeJSON(w, out)
}

/* ---------- /bins/summary: per WASTE STREAM + latest LIVE (Temp/Humidity/Door/LastSeen) ---------- */
func handleBinsSummary(w http.ResponseWriter, r *http.Request) {
	if allowCORS(w, r) {
		return
	}
	// Load all streams and bins in one query, then pick the latest live reading per stream.
	rows, err := db.Query(`
		SELECT
			ws.waste_stream_id,
			ws.name,
			b.bin_id,
			COALESCE(b.esp32_mac, '') AS esp32_mac
		FROM waste_streams ws
		LEFT JOIN bins b ON b.waste_stream_id = ws.waste_stream_id
		ORDER BY ws.waste_stream_id, b.bin_id
	`)
	if err != nil {
		bad(w, 500, "db")
		return
	}
	defer rows.Close()

	type streamRow struct {
		ID              int      `json:"id"`
		Name            string   `json:"name"`
		ESP32MAC        string   `json:"esp32_mac"`
		TempC           *float64 `json:"temp_c"`
		Humidity        *float64 `json:"humidity_pct"`
		DoorOpen        *int     `json:"door_open"`
		DoorStatus      string   `json:"door_status"`
		DoorStatusCamel string   `json:"doorStatus"` // alias for FE that expects camelCase
		LastSeen        string   `json:"last_seen"`
	}

	byStream := map[int]*streamRow{}

	liveMu.RLock()
	defer liveMu.RUnlock()

	for rows.Next() {
		var sid int
		var sname, mac string
		var binID sql.NullInt64
		if err := rows.Scan(&sid, &sname, &binID, &mac); err != nil {
			bad(w, 500, "scan")
			return
		}

		sr, ok := byStream[sid]
		if !ok {
			sr = &streamRow{ID: sid, Name: sname, ESP32MAC: "", TempC: nil, Humidity: nil, DoorOpen: nil, DoorStatus: "", DoorStatusCamel: "", LastSeen: ""}
			byStream[sid] = sr
		}
		// keep any non-empty MAC (or overwrite with a real one later)
		if sr.ESP32MAC == "" && mac != "" {
			sr.ESP32MAC = mac
		}

		// If this stream has a bin, check for a live reading and keep the most recent
		if binID.Valid {
			if lr, ok2 := liveByBin[int(binID.Int64)]; ok2 {
				if sr.LastSeen == "" {
					sr.TempC = lr.Temp
					sr.Humidity = lr.Humidity
					sr.DoorOpen = lr.DoorOpen
					sr.DoorStatus = doorText(lr.DoorOpen)
					sr.DoorStatusCamel = sr.DoorStatus
					sr.LastSeen = lr.LastSeenS
					if mac != "" {
						sr.ESP32MAC = mac
					}
				} else {
					prev, _ := time.Parse(time.RFC3339, sr.LastSeen)
					if lr.LastSeen.After(prev) {
						sr.TempC = lr.Temp
						sr.Humidity = lr.Humidity
						sr.DoorOpen = lr.DoorOpen
						sr.DoorStatus = doorText(lr.DoorOpen)
						sr.DoorStatusCamel = sr.DoorStatus
						sr.LastSeen = lr.LastSeenS
						if mac != "" {
							sr.ESP32MAC = mac
						}
					}
				}
			}
		}
	}

	// Flatten to ordered slice (by stream id for stable UI)
	ids := make([]int, 0, len(byStream))
	for id := range byStream {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	out := make([]streamRow, 0, len(ids))
	for _, id := range ids {
		out = append(out, *byStream[id])
	}

	writeJSON(w, map[string]any{"classes": out})
}

/* ---------- auth ---------- */

type pinReq struct {
	PIN string `json:"pin"`
}
type pinResp struct {
	EmpID  int    `json:"emp_id"`
	Name   string `json:"name"`
	DeptID int    `json:"dept_id"`
}

func handleAuthPIN(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		bad(w, 405, "POST only")
		return
	}
	var req pinReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		bad(w, 400, "bad json")
		return
	}
	if len(req.PIN) != 5 {
		bad(w, 400, "need 5-digit pin")
		return
	}

	var empID, deptID int
	var name string
	err := db.QueryRow(`
		SELECT emp_id, emp_name, dept_id
		FROM employees
		WHERE emp_pin = ?
		LIMIT 1
	`, req.PIN).Scan(&empID, &name, &deptID)
	if err == sql.ErrNoRows {
		bad(w, 401, "invalid pin")
		return
	}
	if err != nil {
		bad(w, 500, "db")
		return
	}
	writeJSON(w, pinResp{EmpID: empID, Name: name, DeptID: deptID})
}

/* ---------- employees (GET list, POST create) ---------- */

type empCreateReq struct {
	Name   string `json:"name"`
	DeptID int    `json:"dept_id"`
	PIN    string `json:"pin"` // 5 digits
}
type empCreateResp struct {
	EmpID  int    `json:"emp_id"`
	Name   string `json:"name"`
	DeptID int    `json:"dept_id"`
}
type empListRow struct {
	EmpID  int    `json:"emp_id"`
	Name   string `json:"name"`
	DeptID int    `json:"dept_id"`
	Dept   string `json:"dept"`
}

func handleEmployees(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		rows, err := db.Query(`
			SELECT e.emp_id, e.emp_name, e.dept_id, d.dept_name
			FROM employees e
			JOIN departments d ON d.dept_id = e.dept_id
			ORDER BY d.dept_name, e.emp_name
		`)
		if err != nil {
			bad(w, 500, "db(employees)")
			return
		}
		defer rows.Close()

		var out []empListRow
		for rows.Next() {
			var rr empListRow
			if err := rows.Scan(&rr.EmpID, &rr.Name, &rr.DeptID, &rr.Dept); err != nil {
				bad(w, 500, "scan")
				return
			}
			out = append(out, rr)
		}
		writeJSON(w, map[string]any{"employees": out})
		return

	case http.MethodPost:
		var req empCreateReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			bad(w, 400, "bad json")
			return
		}
		if req.Name == "" || req.DeptID == 0 || len(req.PIN) != 5 {
			bad(w, 400, "need name, dept_id, 5-digit pin")
			return
		}
		var tmp int
		if err := db.QueryRow(`SELECT 1 FROM departments WHERE dept_id=?`, req.DeptID).Scan(&tmp); err != nil {
			if err == sql.ErrNoRows {
				bad(w, 400, "unknown department")
				return
			}
			bad(w, 500, "db(departments)")
			return
		}

		if err := db.QueryRow(`SELECT 1 FROM employees WHERE emp_pin=?`, req.PIN).Scan(&tmp); err == nil {
			bad(w, 409, "pin already in use")
			return
		} else if err != sql.ErrNoRows {
			bad(w, 500, "db(employees)")
			return
		}

		res, err := db.Exec(`INSERT INTO employees (emp_name, dept_id, emp_pin) VALUES (?, ?, ?)`,
			req.Name, req.DeptID, req.PIN)
		if err != nil {
			bad(w, 500, "insert")
			return
		}
		id64, _ := res.LastInsertId()

		writeJSON(w, empCreateResp{EmpID: int(id64), Name: req.Name, DeptID: req.DeptID})
		return

	default:
		bad(w, 405, "GET or POST only")
		return
	}
}

/* ---------- uuid create (LEGACY body) ---------- */

type createReq struct {
	EmpID      int    `json:"emp_id"`
	DeptID     int    `json:"dept_id"`
	StreamCode string `json:"stream_code"`
	BinID      *int   `json:"bin_id,omitempty"`
}
type createResp struct {
	UUID      string    `json:"uuid"`
	BinID     int       `json:"bin_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

func handleUUIDCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		bad(w, 405, "POST only")
		return
	}
	var req createReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		bad(w, 400, "bad json")
		return
	}
	if req.EmpID == 0 || req.DeptID == 0 || req.StreamCode == "" {
		bad(w, 400, "missing fields")
		return
	}

	var tmp int
	if err := db.QueryRow(`SELECT 1 FROM employees WHERE emp_id=? AND dept_id=?`, req.EmpID, req.DeptID).Scan(&tmp); err != nil {
		if err == sql.ErrNoRows {
			bad(w, 400, "employee not in department")
			return
		}
		bad(w, 500, "db")
		return
	}

	var streamID int
	if err := db.QueryRow(`SELECT waste_stream_id FROM waste_streams WHERE code=?`, req.StreamCode).Scan(&streamID); err != nil {
		if err == sql.ErrNoRows {
			bad(w, 400, "unknown stream code")
			return
		}
		bad(w, 500, "db")
		return
	}

	var binID int
	if req.BinID == nil {
		err := db.QueryRow(`SELECT bin_id FROM bins WHERE waste_stream_id=? ORDER BY bin_id LIMIT 1`, streamID).Scan(&binID)
		if err == sql.ErrNoRows {
			bad(w, 404, "no bin for that stream")
			return
		}
		if err != nil {
			bad(w, 500, "db")
			return
		}
	} else {
		err := db.QueryRow(`SELECT 1 FROM bins WHERE bin_id=? AND waste_stream_id=?`, *req.BinID, streamID).Scan(&tmp)
		if err == sql.ErrNoRows {
			bad(w, 400, "bin mismatch for stream")
			return
		}
		if err != nil {
			bad(w, 500, "db")
			return
		}
		binID = *req.BinID
	}

	code := uuid.New().String()
	exp := utcNow().Add(72 * time.Hour)

	_, err := db.Exec(`
		INSERT INTO uuid_logs (uuid_value, emp_id, bin_id, generated_at, expires_at, is_used)
		VALUES (?, ?, ?, UTC_TIMESTAMP(), ?, 0)`,
		code, req.EmpID, binID, exp)
	if err != nil {
		bad(w, 500, "insert")
		return
	}

	writeJSON(w, createResp{UUID: code, BinID: binID, ExpiresAt: exp})
}

/* ---------- uuid create (PIN + stream_code) ---------- */

type createFromPinReq struct {
	PIN        string `json:"pin"`
	StreamCode string `json:"stream_code"`
}
type createFromPinResp struct {
	UUID      string    `json:"uuid"`
	BinID     int       `json:"bin_id"`
	EmpName   string    `json:"emp_name"`
	DeptID    int       `json:"dept_id"`
	PIN       string    `json:"pin"`
	ExpiresAt time.Time `json:"expires_at"`
}

func handleUUIDCreateFromPIN(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		bad(w, 405, "POST only")
		return
	}

	var req createFromPinReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		bad(w, 400, "bad json")
		return
	}
	if len(req.PIN) != 5 || req.StreamCode == "" {
		bad(w, 400, "need 5-digit pin and stream_code")
		return
	}

	var empID, deptID int
	var empName string
	err := db.QueryRow(`
		SELECT emp_id, emp_name, dept_id
		FROM employees
		WHERE emp_pin = ?
		LIMIT 1
	`, req.PIN).Scan(&empID, &empName, &deptID)
	if err == sql.ErrNoRows {
		bad(w, 401, "invalid pin")
		return
	}
	if err != nil {
		bad(w, 500, "db (employees)")
		return
	}

	var streamID int
	err = db.QueryRow(`
		SELECT waste_stream_id
		FROM waste_streams
		WHERE code = ?
		LIMIT 1
	`, req.StreamCode).Scan(&streamID)
	if err == sql.ErrNoRows {
		bad(w, 400, "unknown stream_code")
		return
	}
	if err != nil {
		bad(w, 500, "db (waste_streams)")
		return
	}

	var binID int
	err = db.QueryRow(`
		SELECT bin_id
		FROM bins
		WHERE waste_stream_id = ?
		ORDER BY RAND()
		LIMIT 1
	`, streamID).Scan(&binID)
	if err == sql.ErrNoRows {
		bad(w, 404, "no bin available for that stream")
		return
	}
	if err != nil {
		log.Printf("bins query error: %v", err)
		bad(w, 500, "db (bins)")
		return
	}

	code := uuid.New().String()
	exp := utcNow().Add(5 * time.Minute)

	_, err = db.Exec(`
		INSERT INTO uuid_logs (uuid_value, emp_id, bin_id, generated_at, expires_at, is_used)
		VALUES (?, ?, ?, UTC_TIMESTAMP(), ?, 0)
	`, code, empID, binID, exp)
	if err != nil {
		bad(w, 500, "insert (uuid_logs)")
		return
	}

	writeJSON(w, createFromPinResp{
		UUID:      code,
		BinID:     binID,
		EmpName:   empName,
		DeptID:    deptID,
		PIN:       req.PIN,
		ExpiresAt: exp,
	})
}

/* ---------- consume at bin ---------- */

type consumeReq struct {
	BinID int    `json:"bin_id"`
	UUID  string `json:"uuid"`
}
type okResp struct {
	OK bool `json:"ok"`
}

func handleBinConsume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		bad(w, 405, "POST only")
		return
	}
	var req consumeReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		bad(w, 400, "bad json")
		return
	}
	if req.BinID == 0 || req.UUID == "" {
		bad(w, 400, "need bin_id and uuid")
		return
	}

	var isUsed int
	var expires time.Time
	err := db.QueryRow(`
		SELECT is_used, IFNULL(expires_at, UTC_TIMESTAMP())
		FROM uuid_logs WHERE uuid_value=? AND bin_id=? LIMIT 1`, req.UUID, req.BinID).
		Scan(&isUsed, &expires)
	if err == sql.ErrNoRows {
		bad(w, 404, "uuid not found for this bin")
		return
	}
	if err != nil {
		bad(w, 500, "db")
		return
	}
	if isUsed == 1 {
		bad(w, 409, "already used")
		return
	}
	if utcNow().After(expires.UTC()) {
		bad(w, 410, "expired")
		return
	}

	_, err = db.Exec(`UPDATE uuid_logs SET is_used=1, used_at=UTC_TIMESTAMP() WHERE uuid_value=? AND bin_id=?`, req.UUID, req.BinID)
	if err != nil {
		bad(w, 500, "update")
		return
	}

	writeJSON(w, okResp{OK: true})
}

/* ---------- telemetry (LIVE + DAILY AVG) ---------- */

// Body: {"bin_id":2,"temp":30.2,"humidity":58.3,"door_open":0}
type TelemetryReq struct {
	BinID    int      `json:"bin_id"`
	Temp     *float64 `json:"temp"`
	Humidity *float64 `json:"humidity"`
	DoorOpen *int     `json:"door_open"` // stored live only
}

func handleBinTelemetry(w http.ResponseWriter, r *http.Request) {
	if allowCORS(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		bad(w, 405, "POST only")
		return
	}

	var req TelemetryReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		bad(w, http.StatusBadRequest, "bad json")
		return
	}
	if req.BinID <= 0 {
		bad(w, http.StatusBadRequest, "missing bin_id")
		return
	}

	// Log what we got
	log.Printf("[TELEM] bin=%d temp=%v hum=%v door=%v", req.BinID, req.Temp, req.Humidity, req.DoorOpen)

	// (A) Update LIVE snapshot IN MEMORY ONLY (not in DB)
	now := utcNow()
	lr := liveReading{
		BinID:     req.BinID,
		Temp:      req.Temp,
		Humidity:  req.Humidity,
		DoorOpen:  req.DoorOpen,
		LastSeen:  now,
		LastSeenS: now.Format(time.RFC3339),
	}
	liveMu.Lock()
	liveByBin[req.BinID] = lr
	liveMu.Unlock()

	// 🔴 Push to all live dashboards
	sseBroadcast(sseMsg{
		Type: "telemetry",
		Data: map[string]any{
			"bin_id":       lr.BinID,
			"temp_c":       lr.Temp,
			"humidity_pct": lr.Humidity,
			"door_open":    lr.DoorOpen,
			"door_status":  doorText(lr.DoorOpen),
			"last_seen":    lr.LastSeenS,
		},
	})

	// (B) Upsert DAILY AGGREGATES into DB (UTC day)
	if req.Temp != nil || req.Humidity != nil {
		_, err := db.Exec(`
			INSERT INTO bin_daily_stats (bin_id, day_utc, sum_temp, sum_humid, samples)
			VALUES (?, UTC_DATE(), COALESCE(?,0), COALESCE(?,0),
			        CASE WHEN (? IS NULL AND ? IS NULL) THEN 0 ELSE 1 END)
			ON DUPLICATE KEY UPDATE
			  sum_temp  = sum_temp  + COALESCE(VALUES(sum_temp),0),
			  sum_humid = sum_humid + COALESCE(VALUES(sum_humid),0),
			  samples   = samples   + CASE
			                            WHEN (VALUES(sum_temp)=0 AND VALUES(sum_humid)=0)
			                            THEN 0 ELSE 1 END
		`, req.BinID, req.Temp, req.Humidity, req.Temp, req.Humidity)
		if err != nil {
			bad(w, 500, err.Error())
			return
		}
	}

	writeJSON(w, map[string]any{"ok": true})
}

/* ---------- LIVE snapshot for dashboard polling ---------- */

func handleBinsLiveJSON(w http.ResponseWriter, r *http.Request) {
	if allowCORS(w, r) {
		return
	}

	// Join live map with static bin metadata for a nice table on the UI
	rows, err := db.Query(`
		SELECT b.bin_id, b.bin_name, b.location, ws.code
		FROM bins b
		JOIN waste_streams ws ON ws.waste_stream_id = b.waste_stream_id
		ORDER BY b.bin_name
	`)
	if err != nil {
		bad(w, 500, "db")
		return
	}
	defer rows.Close()

	type outRow struct {
		BinID           int      `json:"bin_id"`
		BinName         string   `json:"bin_name"`
		Location        string   `json:"location"`
		StreamCode      string   `json:"stream_code"`
		Temp            *float64 `json:"temp_c"`
		Humidity        *float64 `json:"humidity_pct"`
		DoorOpen        *int     `json:"door_open"`
		DoorStatus      string   `json:"door_status"`
		DoorStatusCamel string   `json:"doorStatus"`
		LastSeenUTC     string   `json:"last_seen"`
	}

	liveMu.RLock()
	defer liveMu.RUnlock()

	var out []outRow
	for rows.Next() {
		var id int
		var name, loc, code string
		if err := rows.Scan(&id, &name, &loc, &code); err != nil {
			continue
		}

		row := outRow{
			BinID:      id,
			BinName:    name,
			Location:   loc,
			StreamCode: code,
		}
		if lr, ok := liveByBin[id]; ok {
			row.Temp = lr.Temp
			row.Humidity = lr.Humidity
			row.DoorOpen = lr.DoorOpen
			row.DoorStatus = doorText(lr.DoorOpen)
			row.DoorStatusCamel = row.DoorStatus
			row.LastSeenUTC = lr.LastSeenS
		} else {
			row.Temp = nil
			row.Humidity = nil
			row.DoorOpen = nil
			row.DoorStatus = ""
			row.DoorStatusCamel = ""
			row.LastSeenUTC = ""
		}
		out = append(out, row)
	}

	writeJSON(w, map[string]any{
		"bins": out,
		"ts":   utcNow().Format(time.RFC3339),
	})
}

/* ---------- simple smoke test for live cache ---------- */
func handleBinsLivePeek(w http.ResponseWriter, r *http.Request) {
	if allowCORS(w, r) {
		return
	}
	liveMu.RLock()
	defer liveMu.RUnlock()
	type row struct {
		BinID int      `json:"bin_id"`
		Temp  *float64 `json:"temp_c"`
		Hum   *float64 `json:"humidity_pct"`
		Door  *int     `json:"door_open"`
		Seen  string   `json:"last_seen"`
	}
	out := []row{}
	for _, lr := range liveByBin {
		out = append(out, row{lr.BinID, lr.Temp, lr.Humidity, lr.DoorOpen, lr.LastSeenS})
	}
	writeJSON(w, map[string]any{"bins": out, "ts": utcNow().Format(time.RFC3339)})
}

/* ---------- serve tiny client JS to wire SSE ---------- */

func handleLiveJS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	// This script auto-updates any row: <tr data-binid="2"> with .temp .hum .lid .last cells.
	js := `
(function(){
  if (!window.EventSource) { console.warn("SSE not supported; consider polling /bins/live.json"); return; }
  const src = new EventSource("/bins/live.sse");
  function setText(el, v){ if(!el) return; el.textContent = (v===null||v===undefined||v==="") ? "—" : v; }
  function doorLabel(v){ return v===1 || v==="1" ? "OPEN" : (v===0 || v==="0" ? "CLOSED" : "—"); }

  function apply(row, data){
    if(!row) return;
    setText(row.querySelector(".temp"), data.temp_c);
    setText(row.querySelector(".hum"), data.humidity_pct);
    setText(row.querySelector(".lid"), doorLabel(data.door_open));
    setText(row.querySelector(".last"), data.last_seen);
  }

  src.onmessage = function(evt){
    try{
      const msg = JSON.parse(evt.data);
      if(msg.Type==="snapshot" || msg.type==="snapshot"){
        const bins = (msg.Data && msg.Data.bins) || (msg.data && msg.data.bins) || [];
        bins.forEach(function(b){
          const row = document.querySelector("[data-binid='"+ b.bin_id +"']");
          apply(row, b);
        });
      } else if (msg.Type==="telemetry" || msg.type==="telemetry") {
        const d = msg.Data || msg.data || {};
        const row = document.querySelector("[data-binid='"+ d.bin_id +"']");
        apply(row, d);
        document.dispatchEvent(new CustomEvent("bin-telemetry", {detail:d}));
      }
    }catch(e){ console.error("live.js parse", e); }
  };
})();
`
	_, _ = w.Write([]byte(js))
}

/* ---------- demo ---------- */

func handleGenerateUUID(w http.ResponseWriter, r *http.Request) {
	id := uuid.New().String()
	fmt.Fprintf(w, "Generated UUID: %s", id)
}

/* ---------- admin dashboard ---------- */

type adminCounts struct {
	Employees   int
	Departments int
	Bins        int
	ActiveUUIDs int
}

type adminRow struct {
	UUID        string
	IsUsed      bool
	Expired     bool
	Employee    string
	Department  string
	Stream      string
	BinName     string
	GeneratedAt string
	UsedAt      string
	ExpiresAt   string
}

func handleAdmin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")

	var counts adminCounts
	if err := db.QueryRow(`SELECT COUNT(*) FROM employees`).Scan(&counts.Employees); err != nil {
		bad(w, 500, "db")
		return
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM departments`).Scan(&counts.Departments); err != nil {
		bad(w, 500, "db")
		return
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM bins`).Scan(&counts.Bins); err != nil {
		bad(w, 500, "db")
		return
	}
	if err := db.QueryRow(`
		SELECT COUNT(*) FROM uuid_logs
		WHERE is_used=0 AND (expires_at IS NULL OR UTC_TIMESTAMP() < expires_at)
	`).Scan(&counts.ActiveUUIDs); err != nil {
		bad(w, 500, "db")
		return
	}

	lim := 200
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			if n > 5000 {
				n = 5000
			}
			lim = n
		}
	}
	where, args := buildFilters(r)

	q := `
		SELECT
			ul.uuid_value,
			ul.is_used,
			IF(ul.expires_at IS NOT NULL AND UTC_TIMESTAMP() > ul.expires_at, 1, 0) AS expired,
			e.emp_name,
			d.dept_name,
			ws.code,
			b.bin_name,
			DATE_FORMAT(ul.generated_at, '%Y-%m-%d %H:%i:%s') AS gen_at,
			IFNULL(DATE_FORMAT(ul.used_at, '%Y-%m-%d %H:%i:%s'), '') AS used_at,
			IFNULL(DATE_FORMAT(ul.expires_at, '%Y-%m-%d %H:%i:%s'), '') AS exp_at
		FROM uuid_logs ul
		JOIN employees e   ON e.emp_id  = ul.emp_id
		JOIN departments d ON d.dept_id = e.dept_id
		JOIN bins b        ON b.bin_id  = ul.bin_id
		JOIN waste_streams ws ON ws.waste_stream_id = b.waste_stream_id
	` + where + `
		ORDER BY ul.uuid_id DESC
		LIMIT ?
	`
	args = append(args, lim)

	rows, err := db.Query(q, args...)
	if err != nil {
		bad(w, 500, "db")
		return
	}
	defer rows.Close()

	var recent []adminRow
	for rows.Next() {
		var ar adminRow
		var isUsedInt, expiredInt int
		if err := rows.Scan(&ar.UUID, &isUsedInt, &expiredInt, &ar.Employee, &ar.Department, &ar.Stream, &ar.BinName, &ar.GeneratedAt, &ar.UsedAt, &ar.ExpiresAt); err != nil {
			bad(w, 500, "scan")
			return
		}
		ar.IsUsed = isUsedInt == 1
		ar.Expired = expiredInt == 1
		recent = append(recent, ar)
	}

	data := struct {
		Counts adminCounts
		Recent []adminRow
	}{
		Counts: counts,
		Recent: recent,
	}

	if err := tpl.ExecuteTemplate(w, "admin.html", data); err != nil {
		bad(w, 500, "render")
		return
	}
}

func handleAdminExport(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", `attachment; filename="uuid_logs.csv"`)

	where, args := buildFilters(r)

	q := `
		SELECT
			ul.uuid_value, ul.is_used,
			e.emp_name, d.dept_name, ws.code, b.bin_name,
			DATE_FORMAT(ul.generated_at, '%Y-%m-%d %H:%i:%s') AS gen_at,
			IFNULL(DATE_FORMAT(ul.used_at, '%Y-%m-%d %H:%i:%s'), '') AS used_at,
			IFNULL(DATE_FORMAT(ul.expires_at, '%Y-%m-%d %H:%i:%s'), '') AS exp_at
		FROM uuid_logs ul
		JOIN employees e   ON e.emp_id  = ul.emp_id
		JOIN departments d ON d.dept_id = e.dept_id
		JOIN bins b        ON b.bin_id  = ul.bin_id
		JOIN waste_streams ws ON ws.waste_stream_id = b.waste_stream_id
	` + where + `
		ORDER BY ul.uuid_id DESC
		LIMIT 5000
	`

	rows, err := db.Query(q, args...)
	if err != nil {
		bad(w, 500, "db")
		return
	}
	defer rows.Close()

	cw := csv.NewWriter(w)
	defer cw.Flush()
	_ = cw.Write([]string{"uuid", "status", "employee", "department", "stream", "bin", "generated_at", "used_at", "expires_at"})

	for rows.Next() {
		var uuidVal, empName, deptName, streamCode, binName, genAt, usedAt, expAt string
		var isUsed int
		if err := rows.Scan(&uuidVal, &isUsed, &empName, &deptName, &streamCode, &binName, &genAt, &usedAt, &expAt); err != nil {
			bad(w, 500, "scan")
			return
		}
		status := "active"
		if isUsed == 1 {
			status = "used"
		}
		_ = cw.Write([]string{uuidVal, status, empName, deptName, streamCode, binName, genAt, usedAt, expAt})
	}
}

/* ---------- chart data for admin ---------- */

// handleAdminChartJSON returns daily counts for last N days.
// Params: ?days=14 (default, max 180)  &dept=ID (optional)
func handleAdminChartJSON(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")

	days := 14
	if d := r.URL.Query().Get("days"); d != "" {
		if n, err := strconv.Atoi(d); err == nil && n > 0 && n <= 180 {
			days = n
		}
	}

	var deptCond string
	var argsGen []any
	var argsUsed []any
	if dept := r.URL.Query().Get("dept"); dept != "" {
		deptCond = " AND d.dept_id = ? "
		argsGen = append(argsGen, dept)
		argsUsed = append(argsUsed, dept)
	}

	qGen := `
		SELECT DATE(ul.generated_at) AS d, COUNT(*) AS c
		FROM uuid_logs ul
		JOIN employees e   ON e.emp_id  = ul.emp_id
		JOIN departments d ON d.dept_id = e.dept_id
		WHERE ul.generated_at >= (UTC_TIMESTAMP() - INTERVAL ? DAY)
	` + deptCond + `
		GROUP BY DATE(ul.generated_at)
		ORDER BY d
	`
	argsGen = append([]any{days}, argsGen...)
	rowsGen, err := db.Query(qGen, argsGen...)
	if err != nil {
		bad(w, 500, "db")
		return
	}
	defer rowsGen.Close()

	qUsed := `
		SELECT DATE(ul.used_at) AS d, COUNT(*) AS c
		FROM uuid_logs ul
		JOIN employees e   ON e.emp_id  = ul.emp_id
		JOIN departments d ON d.dept_id = e.dept_id
		WHERE ul.is_used = 1
		  AND ul.used_at >= (UTC_TIMESTAMP() - INTERVAL ? DAY)
	` + deptCond + `
		GROUP BY DATE(ul.used_at)
		ORDER BY d
	`
	argsUsed = append([]any{days}, argsUsed...)
	rowsUsed, err := db.Query(qUsed, argsUsed...)
	if err != nil {
		bad(w, 500, "db")
		return
	}
	defer rowsUsed.Close()

	genMap := map[string]int{}
	for rowsGen.Next() {
		var d string
		var c int
		_ = rowsGen.Scan(&d, &c)
		genMap[d] = c
	}
	usedMap := map[string]int{}
	for rowsUsed.Next() {
		var d string
		var c int
		_ = rowsUsed.Scan(&d, &c)
		usedMap[d] = c
	}

	labels := []string{}
	genSeries := []int{}
	usedSeries := []int{}

	todayUTC := utcNow().Truncate(24 * time.Hour)
	start := todayUTC.AddDate(0, 0, -days+1)
	for d := start; !d.After(todayUTC); d = d.AddDate(0, 0, 1) {
		key := d.Format("2006-01-02")
		labels = append(labels, key)
		genSeries = append(genSeries, genMap[key])
		usedSeries = append(usedSeries, usedMap[key])
	}

	writeJSON(w, map[string]any{
		"labels":    labels,
		"generated": genSeries,
		"used":      usedSeries,
		//tytrere
	})
}
