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
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

// --- embed templates ---
//
//go:embed templates/*
var templateFS embed.FS
var tpl = template.Must(template.ParseFS(templateFS, "templates/*.html"))

var db *sql.DB
var pinRegex = regexp.MustCompile(`^\d{5}$`)

// ================= TIMEZONE HELPERS =================
// Jamaica is UTC-5 (no DST in your setup). If you ever change DST handling,
// replace this with time.LoadLocation("America/Jamaica") on systems that support it.
var jamaicaLoc = time.FixedZone("JAM", -5*3600)

const tzFromUTC = "+00:00"
const tzToJAM = "-05:00"

// local "today" (midnight) in Jamaica time
func jamTodayMidnight() time.Time {
	now := time.Now().In(jamaicaLoc)
	y, m, d := now.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, jamaicaLoc)
}

// ========== LIVE (in-memory only; NOT persisted) ==========
type liveReading struct {
	BinID     int       `json:"bin_id"`
	Temp      *float64  `json:"temp_c"`
	Humidity  *float64  `json:"humidity_pct"`
	DoorOpen  *int      `json:"door_open"`
	LastSeen  time.Time `json:"-"`
	LastSeenS string    `json:"last_seen"`
}

var (
	liveMu    sync.RWMutex
	liveByBin = map[int]liveReading{}
)

// ========== SSE (push live updates to dashboard) ==========
type sseMsg struct {
	Type string      `json:"type"` // telemetry | snapshot | counts
	Data interface{} `json:"data"`
}

var (
	sseClients = map[chan []byte]struct{}{}
	sseMu      sync.RWMutex
)

func sseBroadcast(msg sseMsg) {
	b, _ := json.Marshal(msg)

	out := []byte("event: " + msg.Type + "\n")
	out = append(out, []byte("data: "+string(b)+"\n\n")...)

	sseMu.RLock()
	for ch := range sseClients {
		select {
		case ch <- out:
		default:
		}
	}
	sseMu.RUnlock()
}

// ========== ACTIVE UUID COUNTS (LIVE) ==========

func getActiveUUIDCount() int {
	var n int
	err := db.QueryRow(`
        SELECT COUNT(*)
        FROM uuid_logs
        WHERE action = 'clock_in'
          AND closed_at IS NULL
          AND (expires_at IS NULL OR UTC_TIMESTAMP() < expires_at)
    `).Scan(&n)
	if err != nil {
		return 0
	}
	return n
}

type countMsg struct {
	Active int `json:"active"`
}

func broadcastActiveUUIDCount() {
	sseBroadcast(sseMsg{
		Type: "counts",
		Data: countMsg{Active: getActiveUUIDCount()},
	})
}

// --- CORS helper ---
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

func utcNow() time.Time { return time.Now().UTC() }

func doorText(v *int) string {
	if v == nil {
		return ""
	}
	if *v == 1 {
		return "Open"
	}
	return "Closed"
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func bad(w http.ResponseWriter, code int, msg string) {
	http.Error(w, msg, code)
}

// ---------- LIVE SSE ----------
func handleBinsLiveSSE(w http.ResponseWriter, r *http.Request) {
	if allowCORS(w, r) {
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	ch := make(chan []byte, 64)

	sseMu.Lock()
	sseClients[ch] = struct{}{}
	sseMu.Unlock()

	defer func() {
		sseMu.Lock()
		delete(sseClients, ch)
		sseMu.Unlock()
		close(ch)
	}()

	ctx := r.Context()

	_, _ = w.Write([]byte(": connected\n\n"))
	flusher.Flush()

	// initial snapshot
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
				BinID:      lr.BinID,
				Temp:       lr.Temp,
				Humidity:   lr.Humidity,
				DoorOpen:   lr.DoorOpen,
				DoorStatus: doorText(lr.DoorOpen),
				LastSeen:   lr.LastSeenS,
			})
		}
		liveMu.RUnlock()

		msg := sseMsg{
			Type: "snapshot",
			Data: map[string]any{
				"bins": all,
				"ts":   utcNow().Format(time.RFC3339),
			},
		}

		b, _ := json.Marshal(msg)
		// IMPORTANT: send as a standard message (no custom "event:" line here)
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

/* ---------- buildFilters ---------- */

func buildFilters(r *http.Request) (string, []any) {
	where := " WHERE 1=1 "
	args := []any{}

	if d := r.URL.Query().Get("dept"); d != "" {
		where += " AND d.dept_id = ? "
		args = append(args, d)
	}

	switch r.URL.Query().Get("status") {

	case "active":
		where += `
      AND ul.action = 'clock_in'
      AND ul.closed_at IS NULL
      AND (ul.expires_at IS NULL OR UTC_TIMESTAMP() < ul.expires_at)
    `

	case "used":
		where += `
      AND ul.action IN ('consume','override')
    `

	case "expired":
		where += `
      AND ul.action = 'clock_in'
      AND ul.expires_at IS NOT NULL
      AND UTC_TIMESTAMP() > ul.expires_at
    `
	}

	if d := r.URL.Query().Get("dept"); d != "" {
		where += `
        AND (
            (ul.uuid_type = 'SESSION' AND d.dept_id = ?)
            OR
            (ul.uuid_type = 'CREDENTIAL' AND md.dept_id = ?)
        )
    `
		args = append(args, d, d)
	}

	return where, args
}

/* ---------- reference data ---------- */

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

/* ---------- DEPARTMENTS (GET + POST) ---------- */

type deptCreateReq struct {
	Name string `json:"name"`
	Code string `json:"code"`
}

type deptCreateResp struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Code string `json:"code"`
}

func handleListDepartments(w http.ResponseWriter, r *http.Request) {
	if allowCORS(w, r) {
		return
	}

	switch r.Method {

	case http.MethodGet:
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
			// fallback: no dept_code column
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
		return

	case http.MethodPost:
		var req deptCreateReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			bad(w, 400, "bad json")
			return
		}

		req.Name = strings.TrimSpace(req.Name)
		req.Code = strings.TrimSpace(req.Code)

		if req.Name == "" {
			bad(w, 400, "department name required")
			return
		}

		res, err := db.Exec(`
			INSERT INTO departments (dept_name, dept_code)
			VALUES (?, NULLIF(?, ''))
		`, req.Name, req.Code)
		if err != nil {
			bad(w, 500, "insert failed")
			return
		}

		id64, _ := res.LastInsertId()

		writeJSON(w, deptCreateResp{
			ID:   int(id64),
			Name: req.Name,
			Code: req.Code,
		})
		return

	default:
		bad(w, 405, "GET or POST only")
		return
	}
}

/* ---------- DEPARTMENTS ADMIN JSON (OPTIONAL) ---------- */

type deptAdminRow struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Code string `json:"code"`
}

func handleDepartmentsAdminJSON(w http.ResponseWriter, r *http.Request) {
	if allowCORS(w, r) {
		return
	}
	if r.Method != http.MethodGet {
		bad(w, 405, "GET only")
		return
	}

	rows, err := db.Query(`
		SELECT dept_id, dept_name, COALESCE(dept_code,'')
		FROM departments
		ORDER BY dept_id DESC
	`)
	if err != nil {
		// fallback: no dept_code column
		rows, err = db.Query(`
			SELECT dept_id, dept_name
			FROM departments
			ORDER BY dept_id DESC
		`)
		if err != nil {
			bad(w, 500, "db(departments)")
			return
		}
		defer rows.Close()

		out := []deptAdminRow{}
		for rows.Next() {
			var rr deptAdminRow
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

	out := []deptAdminRow{}
	for rows.Next() {
		var rr deptAdminRow
		if err := rows.Scan(&rr.ID, &rr.Name, &rr.Code); err != nil {
			bad(w, 500, "scan")
			return
		}
		out = append(out, rr)
	}

	writeJSON(w, map[string]any{"departments": out})
}

/* ---------- DEPARTMENTS DELETE ---------- */

type deptDeleteReq struct {
	DeptID int `json:"dept_id"`
}

func handleDepartmentDelete(w http.ResponseWriter, r *http.Request) {
	if allowCORS(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		bad(w, 405, "POST only")
		return
	}

	var req deptDeleteReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		bad(w, 400, "bad json")
		return
	}
	if req.DeptID <= 0 {
		bad(w, 400, "dept_id required")
		return
	}

	// Safety: prevent delete if employees reference this department
	var refs int
	if err := db.QueryRow(`SELECT COUNT(*) FROM employees WHERE dept_id=?`, req.DeptID).Scan(&refs); err != nil {
		bad(w, 500, "db(check employees)")
		return
	}
	if refs > 0 {
		bad(w, 409, "cannot delete: department has employees")
		return
	}

	res, err := db.Exec(`DELETE FROM departments WHERE dept_id=? LIMIT 1`, req.DeptID)
	if err != nil {
		bad(w, 500, "db(delete department)")
		return
	}

	aff, _ := res.RowsAffected()
	if aff == 0 {
		bad(w, 404, "department not found")
		return
	}

	writeJSON(w, map[string]any{"ok": true})
}

/* ---------- BINS (LIST) ---------- */

func handleListBins(w http.ResponseWriter, r *http.Request) {
	if allowCORS(w, r) {
		return
	}

	q := `
		SELECT b.bin_id, b.bin_name, b.location, ws.code
		FROM bins b
		JOIN waste_streams ws ON ws.waste_stream_id=b.waste_stream_id
		WHERE 1=1
	`
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
		if err := rows.Scan(&rr.BinID, &rr.BinName, &rr.Location, &rr.Stream); err == nil {
			out = append(out, rr)
		}
	}

	writeJSON(w, out)
}

/* ---------- /bins/summary ---------- */

func handleBinsSummary(w http.ResponseWriter, r *http.Request) {
	if allowCORS(w, r) {
		return
	}

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
		DoorStatusCamel string   `json:"doorStatus"`
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
			sr = &streamRow{
				ID:              sid,
				Name:            sname,
				ESP32MAC:        "",
				TempC:           nil,
				Humidity:        nil,
				DoorOpen:        nil,
				DoorStatus:      "",
				DoorStatusCamel: "",
				LastSeen:        "",
			}
			byStream[sid] = sr
		}

		if sr.ESP32MAC == "" && mac != "" {
			sr.ESP32MAC = mac
		}

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

/* ---------- AUTH ---------- */

type pinReq struct {
	PIN string `json:"pin"`
}

type pinResp struct {
	Valid        bool   `json:"valid"`
	Role         string `json:"role"`
	EmployeeID   int    `json:"employee_id"`
	EmployeeName string `json:"employee_name"`
	SessionUUID  string `json:"session_uuid"`
}

func handleAuthPIN(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var req pinReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	if !pinRegex.MatchString(req.PIN) {
		http.Error(w, "need 5-digit pin", http.StatusBadRequest)
		return
	}

	// 1️⃣ Resolve PIN → user
	var userType string
	var userID int

	err := db.QueryRow(`
        SELECT user_type, user_id
        FROM pin_registry
        WHERE pin = ?
        LIMIT 1
    `, req.PIN).Scan(&userType, &userID)

	if err == sql.ErrNoRows || userType != "employee" {
		http.Error(w, "Access denied", http.StatusUnauthorized)
		return
	}
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	// 2️⃣ Load employee
	var empID, deptID int
	var empName string

	err = db.QueryRow(`
        SELECT emp_id, emp_name, dept_id
        FROM employees
        WHERE emp_id = ?
          AND is_active = 1
          AND is_hidden = 0
        LIMIT 1
    `, userID).Scan(&empID, &empName, &deptID)

	if err == sql.ErrNoRows {
		http.Error(w, "Access denied", http.StatusUnauthorized)
		return
	}
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	// ===========================
	// 🔒 BEGIN ATOMIC TRANSACTION
	// ===========================

	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "db error", 500)
		return
	}

	// 🔒 Lock employee row
	_, err = tx.Exec(`
        SELECT emp_id
        FROM employees
        WHERE emp_id = ?
        FOR UPDATE
    `, empID)

	if err != nil {
		tx.Rollback()
		http.Error(w, "lock failed", 500)
		return
	}

	// 🔎 Check for active session (lock it if exists)
	var existingUUID string

	err = tx.QueryRow(`
        SELECT uuid_value
        FROM uuid_logs
        WHERE emp_id = ?
          AND action = 'clock_in'
          AND closed_at IS NULL
        LIMIT 1
        FOR UPDATE
    `, empID).Scan(&existingUUID)

	if err == nil {
		tx.Rollback()
		http.Error(w, "already clocked in", http.StatusConflict)
		return
	}

	if err != sql.ErrNoRows {
		tx.Rollback()
		http.Error(w, "db error", 500)
		return
	}

	// ===========================
	// 🟢 NO ACTIVE SESSION → CLOCK IN
	// ===========================

	sessionUUID := uuid.New().String()
	expires := utcNow().Add(8 * time.Hour)

	_, err = tx.Exec(`
        INSERT INTO uuid_logs (
            uuid_value,
            emp_id,
            generated_at,
            expires_at,
            action,
            uuid_type
        )
        VALUES (?, ?, UTC_TIMESTAMP(), ?, 'clock_in', 'SESSION')
    `, sessionUUID, empID, expires)

	if err != nil {
		tx.Rollback()

		// If unique constraint hit, treat as already clocked in
		if strings.Contains(err.Error(), "one_open_session") {
			http.Error(w, "already clocked in", 409)
			return
		}

		http.Error(w, "clock in failed", 500)
		return
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "commit failed", 500)
		return
	}

	broadcastActiveUUIDCount()

	writeJSON(w, pinResp{
		Valid:        true,
		Role:         "employee",
		EmployeeID:   empID,
		EmployeeName: empName,
		SessionUUID:  sessionUUID,
	})
}

/* ---------- EMPLOYEES ---------- */

type empCreateReq struct {
	Name   string `json:"name"`
	DeptID int    `json:"dept_id"`
	PIN    string `json:"pin"`
}

type empCreateResp struct {
	EmpID  int    `json:"emp_id"`
	Name   string `json:"name"`
	DeptID int    `json:"dept_id"`
}

type empStatusReq struct {
	EmpID    int `json:"emp_id"`
	IsActive int `json:"is_active"` // 1 = active, 0 = inactive
}

type empListRow struct {
	EmpID    int    `json:"emp_id"`
	Name     string `json:"name"`
	DeptID   int    `json:"dept_id"`
	Dept     string `json:"dept"`
	PIN      string `json:"pin"`
	IsActive int    `json:"is_active"`
	IsHidden int    `json:"is_hidden"`
}

func handleEmployees(w http.ResponseWriter, r *http.Request) {
	switch r.Method {

	case http.MethodGet:

		rows, err := db.Query(`
SELECT e.emp_id,
       e.emp_name,
       e.dept_id,
       d.dept_name,
       e.pin,
       e.is_active,
       e.is_hidden
			FROM employees e
			JOIN departments d ON d.dept_id = e.dept_id
			WHERE e.is_hidden = 0
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

			if err := rows.Scan(
				&rr.EmpID,
				&rr.Name,
				&rr.DeptID,
				&rr.Dept,
				&rr.PIN,
				&rr.IsActive,
				&rr.IsHidden,
			); err != nil {
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

		if req.Name == "" || req.DeptID == 0 || !pinRegex.MatchString(req.PIN) {
			bad(w, 400, "need name, dept_id, and 5-digit numeric pin")
			return
		}
		tx, err := db.Begin()
		if err != nil {
			bad(w, 500, "tx")
			return
		}

		res, err := tx.Exec(`
			INSERT INTO employees (emp_name, dept_id, pin, is_active, is_hidden)
			VALUES (?, ?, ?, 1, 0)
		`, req.Name, req.DeptID, req.PIN)

		if err != nil {
			tx.Rollback()
			bad(w, 409, "pin already in use")
			return
		}

		id64, _ := res.LastInsertId()

		_, err = tx.Exec(`
			INSERT INTO pin_registry (pin, user_type, user_id)
			VALUES (?, 'employee', ?)
		`, req.PIN, id64)

		if err != nil {
			tx.Rollback()
			bad(w, 409, "pin already registered")
			return
		}

		if err := tx.Commit(); err != nil {
			bad(w, 500, "commit")
			return
		}

		writeJSON(w, empCreateResp{
			EmpID:  int(id64),
			Name:   req.Name,
			DeptID: req.DeptID,
		})
		return

	default:
		bad(w, 405, "GET or POST only")
	}
}

func handleEmployeeStatus(w http.ResponseWriter, r *http.Request) {
	log.Printf("EMP STATUS HIT → method=%s path=%s", r.Method, r.URL.Path)

	if allowCORS(w, r) {
		return
	}

	if r.Method != http.MethodPost {
		bad(w, 405, "POST only")
		return
	}

	var req empStatusReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		bad(w, 400, "bad json")
		return
	}

	if req.EmpID <= 0 || (req.IsActive != 0 && req.IsActive != 1) {
		bad(w, 400, "invalid request")
		return
	}

	res, err := db.Exec(`
		UPDATE employees
		SET is_active = ?
		WHERE emp_id = ?
	`, req.IsActive, req.EmpID)

	if err != nil {
		bad(w, 500, "db(update employee status)")
		return
	}

	aff, _ := res.RowsAffected()
	if aff == 0 {
		bad(w, 404, "employee not found")
		return
	}

	writeJSON(w, map[string]bool{"ok": true})
}

type empHideReq struct {
	EmpID int `json:"emp_id"`
}
type empChangePinReq struct {
	EmpID int    `json:"emp_id"`
	PIN   string `json:"pin"`
}

func handleEmployeeHide(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		bad(w, 405, "POST only")
		return
	}

	var req empHideReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		bad(w, 400, "bad json")
		return
	}

	tx, err := db.Begin()
	if err != nil {
		bad(w, 500, "tx")
		return
	}

	// 1️⃣ mark employee hidden + inactive
	_, err = tx.Exec(`
        UPDATE employees
        SET is_hidden = 1,
            is_active = 0,
            current_uuid = NULL,
            uuid_expires_at = NULL
        WHERE emp_id = ?
    `, req.EmpID)

	if err != nil {
		tx.Rollback()
		bad(w, 500, "update failed")
		return
	}

	// 2️⃣ remove pin from registry
	_, err = tx.Exec(`
        DELETE FROM pin_registry
        WHERE user_type = 'employee'
          AND user_id = ?
    `, req.EmpID)

	if err != nil {
		tx.Rollback()
		bad(w, 500, "registry delete failed")
		return
	}

	if err := tx.Commit(); err != nil {
		bad(w, 500, "commit")
		return
	}

	writeJSON(w, map[string]bool{"ok": true})
}

func handleHiddenEmployees(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodGet {
		bad(w, 405, "GET only")
		return
	}

	rows, err := db.Query(`
SELECT e.emp_id,
       e.emp_name,
       e.dept_id,
       d.dept_name,
       e.pin,
       e.is_active,
       e.is_hidden
    FROM employees e
    JOIN departments d ON d.dept_id = e.dept_id
WHERE e.is_hidden = 1
    ORDER BY d.dept_name, e.emp_name
`)

	if err != nil {
		bad(w, 500, "db")
		return
	}
	defer rows.Close()

	var out []empListRow

	for rows.Next() {
		var rr empListRow
		if err := rows.Scan(
			&rr.EmpID,
			&rr.Name,
			&rr.DeptID,
			&rr.Dept,
			&rr.PIN,
			&rr.IsActive,
			&rr.IsHidden,
		); err != nil {
			continue
		}
		out = append(out, rr)
	}

	writeJSON(w, map[string]any{"employees": out})
}

func handleEmployeeChangePin(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		bad(w, 405, "POST only")
		return
	}

	var req empChangePinReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		bad(w, 400, "bad json")
		return
	}

	if req.EmpID <= 0 || !pinRegex.MatchString(req.PIN) {
		bad(w, 400, "need emp_id and 5-digit pin")
		return
	}

	tx, err := db.Begin()
	if err != nil {
		bad(w, 500, "tx")
		return
	}

	_, err = tx.Exec(`
    UPDATE employees
    SET pin = ?
    WHERE emp_id = ?
`, req.PIN, req.EmpID)

	if err != nil {
		tx.Rollback()
		bad(w, 409, "pin in use")
		return
	}

	_, err = tx.Exec(`
    UPDATE pin_registry
    SET pin = ?
    WHERE user_type = 'employee'
      AND user_id = ?
`, req.PIN, req.EmpID)

	if err != nil {
		tx.Rollback()
		bad(w, 500, "registry update failed")
		return
	}

	if err := tx.Commit(); err != nil {
		bad(w, 500, "commit")
		return
	}

	writeJSON(w, map[string]bool{"ok": true})
}

func handleEmployeeUUIDHistory(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodGet {
		bad(w, 405, "GET only")
		return
	}

	empIDStr := r.URL.Query().Get("emp_id")
	empID, err := strconv.Atoi(empIDStr)
	if err != nil || empID <= 0 {
		bad(w, 400, "invalid emp_id")
		return
	}

	rows, err := db.Query(`
        SELECT uuid_value, generated_at, expires_at, closed_at
        FROM uuid_logs
        WHERE emp_id = ?
        ORDER BY generated_at DESC
    `, empID)

	if err != nil {
		bad(w, 500, "db")
		return
	}
	defer rows.Close()

	type row struct {
		UUID      string     `json:"uuid"`
		Generated time.Time  `json:"generated_at"`
		Expires   *time.Time `json:"expires_at"`
		Closed    *time.Time `json:"closed_at"`
	}

	var out []row
	for rows.Next() {
		var rr row
		if err := rows.Scan(&rr.UUID, &rr.Generated, &rr.Expires, &rr.Closed); err != nil {
			continue
		}
		out = append(out, rr)
	}

	writeJSON(w, map[string]any{"uuids": out})
}

/* ---------- UUID CREATE (legacy) ---------- */

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
	err := db.QueryRow(`
SELECT 1 FROM employees
WHERE emp_id=?
  AND dept_id=?
  AND is_active=1
  AND is_hidden=0
	`, req.EmpID, req.DeptID).Scan(&tmp)

	if err == sql.ErrNoRows {
		bad(w, 400, "employee not in department")
		return
	}
	if err != nil {
		bad(w, 500, "db")
		return
	}

	var streamID int
	err = db.QueryRow(`
		SELECT waste_stream_id
		FROM waste_streams
		WHERE code=?
	`, req.StreamCode).Scan(&streamID)

	if err == sql.ErrNoRows {
		bad(w, 400, "unknown stream code")
		return
	}
	if err != nil {
		bad(w, 500, "db")
		return
	}

	var binID int
	if req.BinID == nil {
		err := db.QueryRow(`
			SELECT bin_id FROM bins
			WHERE waste_stream_id=?
			ORDER BY bin_id LIMIT 1
		`, streamID).Scan(&binID)

		if err == sql.ErrNoRows {
			bad(w, 404, "no bin for that stream")
			return
		}
		if err != nil {
			bad(w, 500, "db")
			return
		}

	} else {
		err := db.QueryRow(`
			SELECT 1 FROM bins
			WHERE bin_id=? AND waste_stream_id=?
		`, *req.BinID, streamID).Scan(&tmp)

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

	// 🔒 Prevent multiple active sessions (legacy endpoint)
	var existingUUID string
	var existingExpiry sql.NullTime

	err = db.QueryRow(`
    SELECT uuid_value, expires_at
    FROM uuid_logs
    WHERE emp_id = ?
      AND action = 'clock_in'
      AND closed_at IS NULL
    ORDER BY generated_at DESC
    LIMIT 1
`, req.EmpID).Scan(&existingUUID, &existingExpiry)

	if err == nil {
		if existingExpiry.Valid && utcNow().Before(existingExpiry.Time) {
			bad(w, 409, "already clocked in")
			return
		}
	}

	code := uuid.New().String()
	exp := utcNow().Add(8 * time.Hour)

	// ✅ Transaction: uuid_logs + uuid_daily_stats
	tx, err := db.Begin()
	if err != nil {
		bad(w, 500, "tx")
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
INSERT INTO uuid_logs (
  uuid_value,
  emp_id,
  bin_id,
  generated_at,
  expires_at,
  is_used,
  action,
  uuid_type
)
VALUES (?, ?, ?, UTC_TIMESTAMP(), ?, 0, 'clock_in', 'SESSION')

	`, code, req.EmpID, binID, exp)
	if err != nil {
		bad(w, 500, "insert")
		return
	}

	if err := tx.Commit(); err != nil {
		bad(w, 500, "commit")
		return
	}

	broadcastActiveUUIDCount()
	writeJSON(w, createResp{UUID: code, BinID: binID, ExpiresAt: exp})
}

/* ---------- UUID CREATE (via PIN) ---------- */

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

	if !pinRegex.MatchString(req.PIN) || req.StreamCode == "" {
		bad(w, 400, "need 5-digit pin and stream_code")
		return
	}

	var empID, deptID int
	var empName string

	var userType string
	var userID int

	err := db.QueryRow(`
    SELECT user_type, user_id
    FROM pin_registry
    WHERE pin = ?
    LIMIT 1
`, req.PIN).Scan(&userType, &userID)

	if err == sql.ErrNoRows {
		time.Sleep(500 * time.Millisecond)
		http.Error(w, "Access denied", http.StatusUnauthorized)
		return
	}
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	if userType != "employee" {
		http.Error(w, "Access denied", http.StatusUnauthorized)
		return
	}

	err = db.QueryRow(`
    SELECT emp_id, emp_name, dept_id
    FROM employees
    WHERE emp_id = ?
      AND is_active = 1
      AND is_hidden = 0
    LIMIT 1
`, userID).Scan(&empID, &empName, &deptID)

	if err == sql.ErrNoRows {
		bad(w, 401, "invalid pin")
		return
	}
	if err != nil {
		bad(w, 500, "db")
		return
	}

	// 🔒 Prevent multiple active sessions
	var existingUUID string
	var existingExpiry sql.NullTime

	err = db.QueryRow(`
SELECT uuid_value, expires_at
FROM uuid_logs
WHERE emp_id = ?
  AND action = 'clock_in'
  AND closed_at IS NULL
  AND (expires_at IS NULL OR UTC_TIMESTAMP() < expires_at)
    ORDER BY generated_at DESC
    LIMIT 1
`, empID).Scan(&existingUUID, &existingExpiry)

	if err == nil {
		// If still valid → block new session
		if existingExpiry.Valid && utcNow().Before(existingExpiry.Time) {
			bad(w, 409, "already clocked in")
			return
		}
	}

	var streamID int
	err = db.QueryRow(`
		SELECT waste_stream_id
		FROM waste_streams
		WHERE code = ?
		LIMIT 1
	`, req.StreamCode).Scan(&streamID)

	if err != nil {
		bad(w, 400, "unknown stream_code")
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

	if err != nil {
		bad(w, 404, "no bin for that stream")
		return
	}

	code := uuid.New().String()
	exp := utcNow().Add(8 * time.Hour)

	_, err = db.Exec(`
INSERT INTO uuid_logs (
  uuid_value,
  emp_id,
  bin_id,
  generated_at,
  expires_at,
  is_used,
  action,
  uuid_type
)
VALUES (?, ?, ?, UTC_TIMESTAMP(), ?, 0, 'clock_in', 'SESSION')
`, code, empID, binID, exp)

	if err != nil {
		bad(w, 500, "insert")
		return
	}

	broadcastActiveUUIDCount()

	writeJSON(w, createFromPinResp{
		UUID:      code,
		BinID:     binID,
		EmpName:   empName,
		DeptID:    deptID,
		ExpiresAt: exp,
	})
}

/* ---------- CONSUME (BIN CONFIRMATION) ---------- */

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

	// 🔵 1️⃣ Try employee session first
	var empID int
	var expiresAt sql.NullTime

	err := db.QueryRow(`
        SELECT emp_id, expires_at
        FROM uuid_logs
        WHERE uuid_value = ?
          AND action = 'clock_in'
          AND closed_at IS NULL
        LIMIT 1
    `, req.UUID).Scan(&empID, &expiresAt)

	if err == nil {

		if expiresAt.Valid && utcNow().After(expiresAt.Time) {
			bad(w, 409, "uuid expired")
			return
		}

		_, err = db.Exec(`
            INSERT INTO uuid_logs (
                uuid_value,
                emp_id,
                bin_id,
                is_used,
                used_at,
                action,
                uuid_type
            )
            VALUES (?, ?, ?, 1, UTC_TIMESTAMP(), 'consume', 'SESSION')
        `, req.UUID, empID, req.BinID)

		if err != nil {
			bad(w, 500, "insert consume failed")
			return
		}

		writeJSON(w, okResp{OK: true})
		return
	}

	// 🟣 2️⃣ Try manager UUID
	var managerID int

	err = db.QueryRow(`
        SELECT mu.manager_id
        FROM manager_uuids mu
        JOIN managers m ON m.manager_id = mu.manager_id
        WHERE mu.uuid_value = ?
          AND mu.is_revoked = 0
          AND m.is_active = 1
        LIMIT 1
    `, req.UUID).Scan(&managerID)

	if err == nil {

		_, err = db.Exec(`
            INSERT INTO uuid_logs (
                uuid_value,
                emp_id,
                bin_id,
                is_used,
                used_at,
                action,
                uuid_type
            )
VALUES (?, NULL, ?, 1, UTC_TIMESTAMP(), 'override', 'CREDENTIAL')
        `, req.UUID, req.BinID)

		if err != nil {
			bad(w, 500, "manager insert failed")
			return
		}

		writeJSON(w, okResp{OK: true})
		return
	}

	// ❌ If neither employee nor manager
	bad(w, 409, "invalid uuid")
}

/* ---------- TELEMETRY ---------- */

type TelemetryReq struct {
	BinID    int      `json:"bin_id"`
	Temp     *float64 `json:"temp"`
	Humidity *float64 `json:"humidity"`
	DoorOpen *int     `json:"door_open"`
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
		bad(w, 400, "bad json")
		return
	}

	if req.BinID <= 0 {
		bad(w, 400, "missing bin_id")
		return
	}

	log.Printf("[TELEM] bin=%d temp=%v hum=%v door=%v",
		req.BinID, req.Temp, req.Humidity, req.DoorOpen)

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

	if req.Temp != nil || req.Humidity != nil {
		_, err := db.Exec(`
			INSERT INTO bin_daily_stats
			(bin_id, day_utc, sum_temp, sum_humid, samples)
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

/* ---------- LIVE JSON (polling fallback) ---------- */

func handleBinsLiveJSON(w http.ResponseWriter, r *http.Request) {
	if allowCORS(w, r) {
		return
	}

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
		}

		out = append(out, row)
	}

	writeJSON(w, map[string]any{
		"bins": out,
		"ts":   utcNow().Format(time.RFC3339),
	})
}

/* ---------- LIVE PEEK ---------- */

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

	var out []row
	for _, lr := range liveByBin {
		out = append(out, row{
			BinID: lr.BinID,
			Temp:  lr.Temp,
			Hum:   lr.Humidity,
			Door:  lr.DoorOpen,
			Seen:  lr.LastSeenS,
		})
	}

	writeJSON(w, map[string]any{
		"bins": out,
		"ts":   utcNow().Format(time.RFC3339),
	})
}

/* ---------- LIVE JS ---------- */

func handleLiveJS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/javascript")

	js := `
// ===============================
//   LIVE.JS (SSE HANDLER SCRIPT)
// ===============================

(function(){

  console.log("🔥 LIVE.JS EXECUTED");

  if (!window.EventSource) {
    console.warn("EventSource not supported");
    return;
  }

  const src = new EventSource("/bins/live.sse");

  // ===== ACTIVE UUID COUNTS =====
  function updateCounts(d){
    const el = document.getElementById("active-uuid-count");
    if (el && d && typeof d.active === "number") {
      el.textContent = d.active;
      console.log("Updated Active UUID:", d.active);
    }
  }

  // Custom named event: "counts"
  src.addEventListener("counts", function(evt){
    try {
      const msg = JSON.parse(evt.data);
      updateCounts(msg.data);
    } catch(e){
      console.error("counts event failed", e);
    }
  });

  src.onmessage = function(evt){
    try {
      const payload = JSON.parse(evt.data);
      document.dispatchEvent(new CustomEvent("bin-sse", { detail: payload }));
    } catch(e){
      console.error("message parse failed", e);
    }
  };

})();`

	_, _ = w.Write([]byte(js))
}

/* ---------- DEMO UUID ---------- */

func handleGenerateUUID(w http.ResponseWriter, r *http.Request) {
	id := uuid.New().String()
	fmt.Fprintf(w, "Generated UUID: %s", id)
}

/* ---------- ADMIN PAGE ---------- */

type adminCounts struct {
	Employees   int `json:"employees"`
	Departments int `json:"departments"`
	Bins        int `json:"bins"`
	ActiveUUIDs int `json:"active_uuids"`
}

type adminRow struct {
	UUID        string `json:"uuid"`
	Action      string `json:"action"`
	UUIDType    string `json:"uuid_type"`
	Employee    string `json:"employee"`
	Department  string `json:"department"`
	BinName     string `json:"bin_name"`
	GeneratedAt string `json:"generated_at"`
	ExpiresAt   string `json:"expires_at"`
}

func formatActionLabel(action string) string {
	switch action {
	case "clock_in":
		return "Shift Started"
	case "consume":
		return "Waste Disposed"
	case "clock_out":
		return "Shift Ended"
	case "override":
		return "Manager Disposal Authorized"
	default:
		return action
	}
}

func handleAdmin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")

	var counts adminCounts

	if err := db.QueryRow(`SELECT COUNT(*) FROM employees`).Scan(&counts.Employees); err != nil {
		bad(w, 500, "db1")
		return
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM departments`).Scan(&counts.Departments); err != nil {
		bad(w, 500, "db2")
		return
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM bins`).Scan(&counts.Bins); err != nil {
		bad(w, 500, "db3")
		return
	}

	counts.ActiveUUIDs = getActiveUUIDCount()

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
    ul.action,
    IFNULL(ul.uuid_type,'SESSION'),

    CASE
        WHEN ul.uuid_type = 'CREDENTIAL' THEN m.name
        ELSE e.emp_name
    END AS person_name,

    CASE
        WHEN ul.uuid_type = 'CREDENTIAL' THEN IFNULL(md.dept_name, 'Manager')
        ELSE IFNULL(d.dept_name, '')
    END AS department_name,

    IFNULL(b.bin_name, ''),

IFNULL(
  DATE_FORMAT(CONVERT_TZ(ul.generated_at, '+00:00', '-05:00'),
              '%Y-%m-%d %l:%i %p'),
  ''
),

    IFNULL(
        DATE_FORMAT(CONVERT_TZ(ul.expires_at, '+00:00', '-05:00'),
                    '%Y-%m-%d %l:%i %p'),
        ''
    )

FROM uuid_logs ul
LEFT JOIN employees e   ON e.emp_id  = ul.emp_id
LEFT JOIN departments d ON d.dept_id = e.dept_id
LEFT JOIN manager_uuids mu ON mu.uuid_value = ul.uuid_value
LEFT JOIN managers m ON m.manager_id = mu.manager_id
LEFT JOIN departments md ON md.dept_id = m.dept_id
LEFT JOIN bins b ON b.bin_id = ul.bin_id


` + where + `
ORDER BY ul.uuid_id DESC
LIMIT ?
`

	args = append(args, lim)

	rows, err := db.Query(q, args...)
	if err != nil {
		bad(w, 500, err.Error())
		return
	}
	defer rows.Close()

	var recent []adminRow

	for rows.Next() {
		var ar adminRow

		if err := rows.Scan(
			&ar.UUID,
			&ar.Action,
			&ar.UUIDType,
			&ar.Employee,
			&ar.Department,
			&ar.BinName,
			&ar.GeneratedAt,
			&ar.ExpiresAt,
		); err != nil {

			bad(w, 500, "scan")
			return
		}

		ar.Action = formatActionLabel(ar.Action)
		if ar.UUIDType == "CREDENTIAL" {
			ar.UUIDType = "🟣 MGR"
		} else {
			ar.UUIDType = "🔵 EMP"
		}

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

/* ---------- EXPORT CSV ---------- */

func handleAdminExport(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", `attachment; filename="UHWI_UUID_logs.csv"`)

	where, args := buildFilters(r)

	q := `
SELECT
    ul.uuid_value,
    ul.action,
    IFNULL(ul.uuid_type,'SESSION'),

    CASE
        WHEN ul.uuid_type = 'CREDENTIAL' THEN m.name
        ELSE e.emp_name
    END AS person_name,

IFNULL(
  CASE
      WHEN ul.uuid_type = 'CREDENTIAL' THEN md.dept_name
      ELSE d.dept_name
  END,
  ''
) AS department_name,

IFNULL(b.bin_name,''),

    DATE_FORMAT(CONVERT_TZ(ul.generated_at, '+00:00', '-05:00'),
                '%Y-%m-%d %l:%i %p'),

    IFNULL(
        DATE_FORMAT(CONVERT_TZ(ul.used_at, '+00:00', '-05:00'),
                    '%Y-%m-%d %l:%i %p'),
        ''
    ),

    IFNULL(
        DATE_FORMAT(CONVERT_TZ(ul.expires_at, '+00:00', '-05:00'),
                    '%Y-%m-%d %l:%i %p'),
        ''
    )

FROM uuid_logs ul

LEFT JOIN employees e   ON e.emp_id  = ul.emp_id
LEFT JOIN departments d ON d.dept_id = e.dept_id

LEFT JOIN manager_uuids mu ON mu.uuid_value = ul.uuid_value
LEFT JOIN managers m ON m.manager_id = mu.manager_id
LEFT JOIN departments md ON md.dept_id = m.dept_id

LEFT JOIN bins b ON b.bin_id = ul.bin_id

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

	_ = cw.Write([]string{
		"uuid",
		"activity",
		"role",
		"name",
		"department",
		"bin",
		"event_time",
		"used_at",
		"expires_at",
	})

	for rows.Next() {

		var uuidVal, action, uuidType, name, dept, binName string
		var eventTime, usedAt, expiresAt string

		if err := rows.Scan(
			&uuidVal,
			&action,
			&uuidType,
			&name,
			&dept,
			&binName,
			&eventTime,
			&usedAt,
			&expiresAt,
		); err != nil {
			bad(w, 500, "scan")
			return
		}

		role := "🔵 EMP"
		if uuidType == "CREDENTIAL" {
			role = "🟣 MGR"
		}

		action = formatActionLabel(action)

		_ = cw.Write([]string{
			uuidVal,
			action,
			role,
			name,
			dept,
			binName,
			eventTime,
			usedAt,
			expiresAt,
		})
	}
}

/* ---------- CHART DATA (BAR GRAPH: generated/used/expired) ---------- */
func handleAdminChartJSON(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")

	days := 14
	if d := r.URL.Query().Get("days"); d != "" {
		if n, err := strconv.Atoi(d); err == nil && n > 0 && n <= 180 {
			days = n
		}
	}

	start := jamTodayMidnight().AddDate(0, 0, -days+1)

	rows, err := db.Query(`
SELECT
    DATE(CONVERT_TZ(generated_at,'+00:00','-05:00')) AS day,
SUM(CASE WHEN action = 'clock_in' THEN 1 ELSE 0 END) AS generated_cnt,
SUM(CASE WHEN action IN ('consume','override') THEN 1 ELSE 0 END) AS used_cnt,
SUM(
  CASE
    WHEN action = 'clock_in'
     AND expires_at IS NOT NULL
     AND UTC_TIMESTAMP() > expires_at
    THEN 1 ELSE 0
  END
) AS expired_cnt
  FROM uuid_logs
  GROUP BY DATE(CONVERT_TZ(generated_at,'+00:00','-05:00'))
  ORDER BY DATE(CONVERT_TZ(generated_at,'+00:00','-05:00'))
`)
	if err != nil {
		bad(w, 500, err.Error())
		return
	}
	defer rows.Close()

	type rec struct {
		g int
		u int
		x int
	}

	m := map[string]rec{}
	activeByDay := map[string]int{}

	for rows.Next() {
		var day time.Time
		var g, u, x int

		if err := rows.Scan(&day, &g, &u, &x); err != nil {
			bad(w, 500, "scan(chart)")
			return
		}

		k := day.Format("2006-01-02")
		m[k] = rec{g: g, u: u, x: x}
	}
	rows2, err := db.Query(`
  SELECT
    d.day,
    (
      SELECT COUNT(*)
      FROM uuid_logs ul
      WHERE
        DATE(CONVERT_TZ(ul.generated_at,'+00:00','-05:00')) <= d.day
AND ul.action = 'clock_in'
AND ul.closed_at IS NULL
        AND (
          ul.expires_at IS NULL
          OR DATE(CONVERT_TZ(ul.expires_at,'+00:00','-05:00')) > d.day
        )
    ) AS active_cnt
  FROM (
    SELECT DISTINCT
      DATE(CONVERT_TZ(generated_at,'+00:00','-05:00')) AS day
    FROM uuid_logs
  ) d
`)
	if err != nil {
		bad(w, 500, err.Error())
		return
	}
	defer rows2.Close()

	for rows2.Next() {
		var day time.Time
		var cnt int
		if err := rows2.Scan(&day, &cnt); err != nil {
			bad(w, 500, "scan(active)")
			return
		}
		activeByDay[day.Format("2006-01-02")] = cnt
	}

	labels := []string{}
	active := []int{}
	used := []int{}
	expired := []int{}

	today := jamTodayMidnight()

	for d := start; !d.After(today); d = d.AddDate(0, 0, 1) {
		k := d.Format("2006-01-02")
		r := m[k]

		labels = append(labels, k)
		a := activeByDay[k]
		active = append(active, a)

		used = append(used, r.u)
		expired = append(expired, r.x)
	}

	writeJSON(w, map[string]any{
		"title":   "UUID Daily Statistics",
		"labels":  labels,
		"active":  active,
		"used":    used,
		"expired": expired,
	})
}

/* ---------- ADMIN BINS JSON + DELETE (NEW) ---------- */

type binAdminRow struct {
	BinID      int    `json:"bin_id"`
	BinName    string `json:"bin_name"`
	Location   string `json:"location"`
	StreamCode string `json:"stream_code"`
	StreamName string `json:"stream_name"`
}

func handleBinsAdminJSON(w http.ResponseWriter, r *http.Request) {
	if allowCORS(w, r) {
		return
	}
	if r.Method != http.MethodGet {
		bad(w, 405, "GET only")
		return
	}

	rows, err := db.Query(`
		SELECT
			b.bin_id,
			b.bin_name,
			COALESCE(b.location,'') AS location,
			ws.code,
			ws.name
		FROM bins b
		JOIN waste_streams ws ON ws.waste_stream_id = b.waste_stream_id
		ORDER BY b.bin_id DESC
	`)
	if err != nil {
		bad(w, 500, "db(bins)")
		return
	}
	defer rows.Close()

	out := []binAdminRow{}
	for rows.Next() {
		var rr binAdminRow
		if err := rows.Scan(&rr.BinID, &rr.BinName, &rr.Location, &rr.StreamCode, &rr.StreamName); err != nil {
			bad(w, 500, "scan")
			return
		}
		out = append(out, rr)
	}

	writeJSON(w, map[string]any{"bins": out})
}

type binDeleteReq struct {
	BinID int `json:"bin_id"`
}

func handleBinDelete(w http.ResponseWriter, r *http.Request) {
	if allowCORS(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		bad(w, 405, "POST only")
		return
	}

	var req binDeleteReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		bad(w, 400, "bad json")
		return
	}
	if req.BinID <= 0 {
		bad(w, 400, "bin_id required")
		return
	}

	// Safety: prevent delete if UUID logs reference this bin
	var refs int
	if err := db.QueryRow(`SELECT COUNT(*) FROM uuid_logs WHERE bin_id=?`, req.BinID).Scan(&refs); err != nil {
		bad(w, 500, "db(check refs)")
		return
	}
	if refs > 0 {
		bad(w, 409, "cannot delete: bin has uuid_logs history")
		return
	}

	// Safety: prevent delete if bin_daily_stats references this bin
	_ = db.QueryRow(`SELECT COUNT(*) FROM bin_daily_stats WHERE bin_id=?`, req.BinID).Scan(&refs)
	if refs > 0 {
		bad(w, 409, "cannot delete: bin has daily stats history")
		return
	}

	res, err := db.Exec(`DELETE FROM bins WHERE bin_id=? LIMIT 1`, req.BinID)
	if err != nil {
		bad(w, 500, "db(delete bin)")
		return
	}

	aff, _ := res.RowsAffected()
	if aff == 0 {
		bad(w, 404, "bin not found")
		return
	}

	writeJSON(w, map[string]any{"ok": true})
}

// ====== DAILY UUID STATS ROLLUP (uuid_daily_stats) ======

// Ensures there's a row for today's date in uuid_daily_stats (Jamaica date)
func ensureStatsRowForToday() {
	day := jamTodayMidnight().Format("2006-01-02")
	_, _ = db.Exec(`
		INSERT INTO uuid_daily_stats (stat_date, generated_count, used, expired)
		VALUES (?, 0, 0, 0)
		ON DUPLICATE KEY UPDATE stat_date = stat_date
	`, day)
}

// Roll up counts for a specific Jamaica day into uuid_daily_stats
func updateStatsForDay(day time.Time) error {
	dayStr := day.Format("2006-01-02")

	// boundaries in UTC for "that Jamaica day"
	startLocal := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, jamaicaLoc)
	endLocal := startLocal.Add(24 * time.Hour)

	startUTC := startLocal.UTC()
	endUTC := endLocal.UTC()

	// ✅ GENERATED during that day (generated_at inside the UTC window)
	var generated int
	if err := db.QueryRow(`
SELECT COUNT(*)
FROM uuid_logs
WHERE action = 'clock_in'
  AND generated_at >= ?
  AND generated_at < ?
	`, startUTC, endUTC).Scan(&generated); err != nil {
		return err
	}

	// USED during that day (used_at inside the UTC window)
	var used int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM uuid_logs
WHERE action IN ('consume','override')
  AND used_at IS NOT NULL
		  AND used_at >= ?
		  AND used_at < ?
	`, startUTC, endUTC).Scan(&used); err != nil {
		return err
	}

	// EXPIRED during that day (expired_at inside the UTC window)
	var expired int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM uuid_logs
		WHERE expired_at IS NOT NULL
		  AND expired_at >= ?
		  AND expired_at < ?
	`, startUTC, endUTC).Scan(&expired); err != nil {
		return err
	}

	_, err := db.Exec(`
		INSERT INTO uuid_daily_stats (stat_date, generated_count, used, expired)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		  generated_count=VALUES(generated_count),
		  used=VALUES(used),
		  expired=VALUES(expired),
		  updated_at=CURRENT_TIMESTAMP
	`, dayStr, generated, used, expired)

	return err
}

// Marks newly-expired UUIDs once, so they can be counted safely (no double counting)
func markNewlyExpired() {
	_, _ = db.Exec(`
		UPDATE uuid_logs
		SET expired_at = UTC_TIMESTAMP()
WHERE action = 'clock_in'
  AND closed_at IS NULL
  AND expires_at IS NOT NULL
		  AND expires_at < UTC_TIMESTAMP()
		  AND expired_at IS NULL
	`)
}

// Runs forever: marks expiries + updates today/previous days
func startUUIDDailyRollupJob() {
	go func() {
		// run once at boot
		ensureStatsRowForToday()
		markNewlyExpired()

		// refresh last N days (covers restarts + late updates)
		const backfillDays = 30
		today := jamTodayMidnight()
		for i := 0; i < backfillDays; i++ {
			_ = updateStatsForDay(today.AddDate(0, 0, -i))
		}

		// then keep updating
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			ensureStatsRowForToday()
			markNewlyExpired()

			// update today + yesterday continuously
			_ = updateStatsForDay(jamTodayMidnight())
			_ = updateStatsForDay(jamTodayMidnight().AddDate(0, 0, -1))
		}
	}()
}

/* ---------- UUID CLOCK-OUT ---------- */

type clockOutReq struct {
	UUID string `json:"uuid"`
}

func handleUUIDClockOut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		bad(w, 405, "POST only")
		return
	}

	var req clockOutReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		bad(w, 400, "bad json")
		return
	}

	req.UUID = strings.TrimSpace(req.UUID)
	if req.UUID == "" {
		bad(w, 400, "uuid required")
		return
	}

	// 1️⃣ Ensure UUID exists and is aUPDATE uuid_log valid active clock-in
	var uuidID int
	err := db.QueryRow(`
SELECT uuid_id
FROM uuid_logs
WHERE uuid_value = ?
  AND action = 'clock_in'
  AND closed_at IS NULL
LIMIT 1

`, req.UUID).Scan(&uuidID)

	if err == sql.ErrNoRows {
		bad(w, 409, "uuid not active or already closed")
		return
	}
	if err != nil {
		bad(w, 500, "db")
		return
	}

	// 2️⃣ Close the session
	_, err = db.Exec(`
UPDATE uuid_logs
SET closed_at = UTC_TIMESTAMP()
WHERE uuid_id = ?
`, uuidID)

	if err != nil {
		bad(w, 500, "db(update)")
		return
	}
	broadcastActiveUUIDCount()

	writeJSON(w, map[string]bool{"ok": true})
}

/* ---------- MANAGERS (FULL ACCOUNT SYSTEM) ---------- */

type managerCreateReq struct {
	Name string `json:"name"`
	PIN  string `json:"pin"`
}

type managerStatusReq struct {
	ManagerID int `json:"manager_id"`
	IsActive  int `json:"is_active"`
}

type managerDeleteReq struct {
	ManagerID int `json:"manager_id"`
}

type managerUUIDCreateReq struct {
	PIN string `json:"pin"`
}

type managerValidateReq struct {
	UUID string `json:"uuid"`
}
type managerUUIDRevokeReq struct {
	UUID string `json:"uuid"`
}

type managerValidateResp struct {
	Valid bool `json:"valid"`
}

/* ---------- MANAGERS (GET + POST) ---------- */

func handleManagers(w http.ResponseWriter, r *http.Request) {

	switch r.Method {

	case http.MethodGet:

		rows, err := db.Query(`
SELECT manager_id, name, pin, is_active
			FROM managers
			ORDER BY name
		`)
		if err != nil {
			bad(w, 500, "db(managers)")
			return
		}
		defer rows.Close()

		type row struct {
			ManagerID int    `json:"manager_id"`
			Name      string `json:"name"`
			PIN       string `json:"pin"`
			IsActive  int    `json:"is_active"`
		}

		var out []row
		for rows.Next() {
			var rr row
			if err := rows.Scan(&rr.ManagerID, &rr.Name, &rr.PIN, &rr.IsActive); err != nil {
				bad(w, 500, "scan")
				return
			}
			out = append(out, rr)
		}

		writeJSON(w, map[string]any{"managers": out})
		return

	case http.MethodPost:

		var req managerCreateReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			bad(w, 400, "bad json")
			return
		}

		if req.Name == "" || !pinRegex.MatchString(req.PIN) {
			bad(w, 400, "need name + 5-digit pin")
			return
		}

		tx, err := db.Begin()
		if err != nil {
			bad(w, 500, "tx")
			return
		}

		res, err := tx.Exec(`
			INSERT INTO managers (name, pin)
			VALUES (?, ?)
		`, req.Name, req.PIN)

		if err != nil {
			tx.Rollback()
			bad(w, 409, "pin already in use")
			return
		}

		id64, _ := res.LastInsertId()

		_, err = tx.Exec(`
			INSERT INTO pin_registry (pin, user_type, user_id)
			VALUES (?, 'manager', ?)
		`, req.PIN, id64)

		if err != nil {
			tx.Rollback()
			bad(w, 409, "pin already registered")
			return
		}

		if err := tx.Commit(); err != nil {
			bad(w, 500, "commit")
			return
		}

		writeJSON(w, map[string]any{
			"manager_id": int(id64),
			"name":       req.Name,
		})
		return

	default:
		bad(w, 405, "GET or POST only")
	}
}

func handleManagerStatus(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		bad(w, 405, "POST only")
		return
	}

	var req managerStatusReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		bad(w, 400, "bad json")
		return
	}

	if req.ManagerID <= 0 || (req.IsActive != 0 && req.IsActive != 1) {
		bad(w, 400, "invalid request")
		return
	}

	res, err := db.Exec(`
		UPDATE managers
		SET is_active = ?
		WHERE manager_id = ?
	`, req.IsActive, req.ManagerID)

	if err != nil {
		bad(w, 500, "db(update manager status)")
		return
	}

	aff, _ := res.RowsAffected()
	if aff == 0 {
		bad(w, 404, "manager not found")
		return
	}

	writeJSON(w, map[string]bool{"ok": true})
}

/* ---------- DELETE MANAGER ---------- */

func handleManagerDelete(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		bad(w, 405, "POST only")
		return
	}

	var req managerDeleteReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		bad(w, 400, "bad json")
		return
	}

	// delete UUIDs first
	_, _ = db.Exec(`DELETE FROM manager_uuids WHERE manager_id=?`, req.ManagerID)

	// remove pin from registry
	_, _ = db.Exec(`
DELETE FROM pin_registry
WHERE user_type = 'manager'
  AND user_id = ?
`, req.ManagerID)

	_, err := db.Exec(`DELETE FROM managers WHERE manager_id=?`, req.ManagerID)
	if err != nil {
		bad(w, 500, "delete")
		return
	}

	writeJSON(w, map[string]bool{"ok": true})
}

/* ---------- CHANGE MANAGER PIN ---------- */

type managerChangePinReq struct {
	ManagerID int    `json:"manager_id"`
	PIN       string `json:"pin"`
}

func handleManagerChangePin(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		bad(w, 405, "POST only")
		return
	}

	var req managerChangePinReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		bad(w, 400, "bad json")
		return
	}

	if req.ManagerID <= 0 || !pinRegex.MatchString(req.PIN) {
		bad(w, 400, "need manager_id and 5-digit pin")
		return
	}

	tx, err := db.Begin()
	if err != nil {
		bad(w, 500, "tx")
		return
	}

	_, err = tx.Exec(`
		UPDATE managers
		SET pin = ?
		WHERE manager_id = ?
	`, req.PIN, req.ManagerID)

	if err != nil {
		tx.Rollback()
		bad(w, 409, "pin in use")
		return
	}

	_, err = tx.Exec(`
		UPDATE pin_registry
		SET pin = ?
		WHERE user_type = 'manager'
		  AND user_id = ?
	`, req.PIN, req.ManagerID)

	if err != nil {
		tx.Rollback()
		bad(w, 500, "registry update failed")
		return
	}

	if err := tx.Commit(); err != nil {
		bad(w, 500, "commit")
		return
	}

	writeJSON(w, map[string]bool{"ok": true})
}

/* ---------- CREATE MANAGER UUID (6 MONTHS) ---------- */
func handleManagerUUIDCreate(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		bad(w, 405, "POST only")
		return
	}

	var req managerUUIDCreateReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		bad(w, 400, "bad json")
		return
	}

	if !pinRegex.MatchString(req.PIN) {
		bad(w, 400, "PIN must be 5 digits")
		return
	}

	// 1️⃣ Find manager by PIN
	var userType string
	var userID int

	err := db.QueryRow(`
    SELECT user_type, user_id
    FROM pin_registry
    WHERE pin = ?
    LIMIT 1
`, req.PIN).Scan(&userType, &userID)

	if err == sql.ErrNoRows || userType != "manager" {
		bad(w, 401, "invalid pin")
		return
	}
	if err != nil {
		bad(w, 500, "db")
		return
	}

	managerID := userID

	// 2️⃣ Check if active UUID already exists
	var exists int
	err = db.QueryRow(`
        SELECT 1
        FROM manager_uuids
        WHERE manager_id = ?
          AND is_revoked = 0
          AND (expires_at IS NULL OR UTC_TIMESTAMP() < expires_at)
        LIMIT 1
    `, managerID).Scan(&exists)

	if err == nil {
		bad(w, 409, "manager already has active credential")
		return
	}

	// 3️⃣ Generate new UUID
	newUUID := uuid.New().String()
	expires := utcNow().AddDate(0, 6, 0)

	_, err = db.Exec(`
        INSERT INTO manager_uuids
        (uuid_value, manager_id, is_revoked, created_at, expires_at)
        VALUES (?, ?, 0, UTC_TIMESTAMP(), ?)
    `, newUUID, managerID, expires)

	if err != nil {
		bad(w, 500, "insert failed")
		return
	}

	writeJSON(w, map[string]any{
		"uuid":       newUUID,
		"expires_at": expires,
	})
}

/* ---------- REVOKE MANAGER UUID ---------- */

func handleManagerUUIDRevoke(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		bad(w, 405, "POST only")
		return
	}

	var req managerUUIDRevokeReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		bad(w, 400, "bad json")
		return
	}

	_, err := db.Exec(`
        UPDATE manager_uuids
        SET is_revoked = 1,
            revoked_at = UTC_TIMESTAMP()
        WHERE uuid_value = ?
    `, req.UUID)

	if err != nil {
		bad(w, 500, "db")
		return
	}

	writeJSON(w, map[string]bool{"ok": true})
}

/* ---------- VALIDATE MANAGER UUID ---------- */

func handleManagerValidate(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		bad(w, 405, "POST only")
		return
	}

	var req managerValidateReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		bad(w, 400, "bad json")
		return
	}

	var exists int
	err := db.QueryRow(`
		SELECT 1
		FROM manager_uuids mu
		JOIN managers m ON m.manager_id = mu.manager_id
WHERE mu.uuid_value = ?
  AND mu.is_revoked = 0
  AND m.is_active = 1
  AND (mu.expires_at IS NULL OR UTC_TIMESTAMP() < mu.expires_at)
		LIMIT 1
	`, req.UUID).Scan(&exists)

	if err == sql.ErrNoRows {
		writeJSON(w, managerValidateResp{Valid: false})
		return
	}

	if err != nil {
		bad(w, 500, "db")
		return
	}

	writeJSON(w, managerValidateResp{Valid: true})
}

/* ---------- LIST MANAGER UUIDS ---------- */

func handleManagerUUIDList(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodGet {
		bad(w, 405, "GET only")
		return
	}

	// Expecting path: /managers/{id}/uuids
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 3 || parts[0] != "managers" || parts[2] != "uuids" {
		bad(w, 404, "invalid path")
		return
	}

	managerID, err := strconv.Atoi(parts[1])
	if err != nil || managerID <= 0 {
		bad(w, 400, "invalid manager id")
		return
	}

	rows, err := db.Query(`
SELECT uuid_value, is_revoked, created_at, revoked_at, expires_at
FROM manager_uuids
WHERE manager_id = ?
ORDER BY created_at DESC
    `, managerID)

	if err != nil {
		bad(w, 500, "db")
		return
	}
	defer rows.Close()

	type row struct {
		UUIDValue string     `json:"uuid_value"`
		IsRevoked int        `json:"is_revoked"`
		CreatedAt time.Time  `json:"created_at"`
		RevokedAt *time.Time `json:"revoked_at"`
		ExpiresAt *time.Time `json:"expires_at"`
	}

	var out []row

	for rows.Next() {
		var rr row

		if err := rows.Scan(
			&rr.UUIDValue,
			&rr.IsRevoked,
			&rr.CreatedAt,
			&rr.RevokedAt,
			&rr.ExpiresAt,
		); err != nil {
			bad(w, 500, "scan")
			return
		}

		out = append(out, rr)
	}

	writeJSON(w, map[string]any{
		"uuids": out,
	})
}
func handleEmployeeUnhide(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		bad(w, 405, "POST only")
		return
	}

	var req empHideReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		bad(w, 400, "bad json")
		return
	}

	tx, err := db.Begin()
	if err != nil {
		bad(w, 500, "tx")
		return
	}

	// 1️⃣ Unhide employee
	_, err = tx.Exec(`
        UPDATE employees
        SET is_hidden = 0,
            is_active = 1
        WHERE emp_id = ?
    `, req.EmpID)

	if err != nil {
		tx.Rollback()
		bad(w, 500, "update failed")
		return
	}

	// 2️⃣ Get employee PIN
	var pin string
	err = tx.QueryRow(`
        SELECT pin
        FROM employees
        WHERE emp_id = ?
    `, req.EmpID).Scan(&pin)

	if err != nil {
		tx.Rollback()
		bad(w, 500, "pin lookup failed")
		return
	}

	// 3️⃣ Re-register PIN
	_, err = tx.Exec(`
        INSERT INTO pin_registry (pin, user_type, user_id)
        VALUES (?, 'employee', ?)
    `, pin, req.EmpID)

	if err != nil {
		tx.Rollback()
		bad(w, 409, "pin already in use")
		return
	}

	if err := tx.Commit(); err != nil {
		bad(w, 500, "commit")
		return
	}

	writeJSON(w, map[string]bool{"ok": true})
}

func handleManagerUUIDListAll(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodGet {
		bad(w, 405, "GET only")
		return
	}

	rows, err := db.Query(`
        SELECT uuid_value
        FROM manager_uuids mu
        JOIN managers m ON m.manager_id = mu.manager_id
        WHERE mu.is_revoked = 0
          AND m.is_active = 1
          AND (mu.expires_at IS NULL OR UTC_TIMESTAMP() < mu.expires_at)
    `)
	if err != nil {
		bad(w, 500, "db")
		return
	}
	defer rows.Close()

	var list []string

	for rows.Next() {
		var uuid string
		if err := rows.Scan(&uuid); err != nil {
			bad(w, 500, "scan")
			return
		}
		list = append(list, uuid)
	}

	if err := rows.Err(); err != nil {
		bad(w, 500, "rows error")
		return
	}

	writeJSON(w, map[string]any{
		"managers": list,
	})
}

// ========== MAIN ==========
func main() {
	var err error

	_ = godotenv.Load()

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		log.Fatal("❌ DB_DSN environment variable not set")
	}

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

	fmt.Println("✅ Connected to MySQL (UTC mode)")
	startUUIDDailyRollupJob()

	// ===== ROUTES =====
	http.HandleFunc("/admin/chart.json", handleAdminChartJSON)
	http.HandleFunc("/streams", handleListStreams)

	// departments
	http.HandleFunc("/departments", handleListDepartments)         // GET + POST
	http.HandleFunc("/departments/classes", handleListDepartments) // GET alias
	http.HandleFunc("/departments/delete", handleDepartmentDelete) // POST delete
	http.HandleFunc("/admin/departments.json", handleDepartmentsAdminJSON)

	// bins
	http.HandleFunc("/bins", handleListBins)
	http.HandleFunc("/bins/summary", handleBinsSummary)
	http.HandleFunc("/bins/live.json", handleBinsLiveJSON)
	http.HandleFunc("/bins/live.sse", handleBinsLiveSSE)
	http.HandleFunc("/bins/live.peek", handleBinsLivePeek)

	// bins admin + delete
	http.HandleFunc("/admin/bins.json", handleBinsAdminJSON)
	http.HandleFunc("/bins/delete", handleBinDelete)

	// admin
	http.HandleFunc("/admin", handleAdmin)
	http.HandleFunc("/admin/export.csv", handleAdminExport)

	// employees
	http.HandleFunc("/employees", handleEmployees)
	http.HandleFunc("/employees/status", handleEmployeeStatus)
	http.HandleFunc("/employees/hide", handleEmployeeHide)
	http.HandleFunc("/employees/unhide", handleEmployeeUnhide)
	http.HandleFunc("/employees/change-pin", handleEmployeeChangePin)
	http.HandleFunc("/employees/uuids", handleEmployeeUUIDHistory)
	http.HandleFunc("/employees/hidden", handleHiddenEmployees)

	// auth + uuid
	http.HandleFunc("/auth/pin", handleAuthPIN)
	http.HandleFunc("/uuid/create/legacy", handleUUIDCreate)
	http.HandleFunc("/uuid/clock-out", handleUUIDClockOut)

	// managers (new full system)
	http.HandleFunc("/managers/list", handleManagerUUIDListAll)
	http.HandleFunc("/managers", handleManagers)
	http.HandleFunc("/managers/uuid", handleManagerUUIDCreate)
	http.HandleFunc("/managers/", handleManagerUUIDList)
	http.HandleFunc("/managers/status", handleManagerStatus)
	http.HandleFunc("/managers/delete", handleManagerDelete)
	http.HandleFunc("/managers/change-pin", handleManagerChangePin)
	http.HandleFunc("/managers/uuid/revoke", handleManagerUUIDRevoke)
	http.HandleFunc("/managers/uuid/validate", handleManagerValidate)

	// bin module
	http.HandleFunc("/bin/consume", handleBinConsume)
	http.HandleFunc("/bin/telemetry", handleBinTelemetry)

	// health check for ESP32
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// static + demo
	http.HandleFunc("/static/live.js", handleLiveJS)
	http.HandleFunc("/generate_uuid", handleGenerateUUID)

	fmt.Println("🚀 Server running at http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
