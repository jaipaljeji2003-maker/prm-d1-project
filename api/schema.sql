-- ============================================================
-- PRM D1 Schema  (Phase 1 — exact replica of Dispatch_DB)
-- Run once:  wrangler d1 execute prm-dispatch --file=schema.sql
-- ============================================================

-- Main flight table (mirrors Dispatch_DB columns 1-for-1)
CREATE TABLE IF NOT EXISTS flights (
  key                TEXT PRIMARY KEY,   -- "2025-02-25|ARR|WS 816|06:30"
  type               TEXT NOT NULL,      -- ARR / DEP
  flight             TEXT NOT NULL,      -- "WS 816"
  time_est           TEXT,               -- ISO 8601 UTC (estimated arrival/departure)
  sched              TEXT,               -- ISO 8601 UTC (scheduled)
  origin_dest        TEXT DEFAULT '',    -- IATA code of origin (ARR) or destination (DEP)
  gate               TEXT DEFAULT '',

  -- Manual fields (NEVER overwritten by FIDS sync)
  wchr               INTEGER DEFAULT 0,
  wchc               INTEGER DEFAULT 0,
  comment            TEXT DEFAULT '',
  assignment         TEXT DEFAULT '',
  pax_assisted       INTEGER DEFAULT 0,

  -- Zone tracking
  zone_current       TEXT DEFAULT 'Unassigned',
  zone_previous      TEXT DEFAULT '',    -- initial zone (set once, never changed by sync)
  zone_prev          TEXT DEFAULT '',    -- ZonePrev: old-zone carry-over until old zone ACKs

  -- Gate change flags
  gate_changed       INTEGER DEFAULT 0,  -- BOOLEAN 0/1
  gate_chg_time      TEXT,
  gate_chg_from_zone TEXT DEFAULT '',
  gate_chg_to_zone   TEXT DEFAULT '',
  gate_chg_from_gate TEXT DEFAULT '',
  gate_chg_to_gate   TEXT DEFAULT '',

  -- Time change flags
  time_prev_est      TEXT,               -- ISO 8601 UTC
  time_changed       INTEGER DEFAULT 0,  -- BOOLEAN
  time_delta_min     INTEGER,
  time_chg_time      TEXT,

  -- Zone change flags
  zone_changed       INTEGER DEFAULT 0,  -- BOOLEAN
  zone_chg_time      TEXT,
  zone_chg_from      TEXT DEFAULT '',
  zone_chg_to        TEXT DEFAULT '',

  alert_text         TEXT DEFAULT '',

  -- Per-board ACK flags (6 boards — reset to 0 on every new change)
  dispatch_ack       INTEGER DEFAULT 0,
  piera_ack          INTEGER DEFAULT 0,
  tb_ack             INTEGER DEFAULT 0,
  t1_ack             INTEGER DEFAULT 0,
  unassigned_ack     INTEGER DEFAULT 0,
  gates_ack          INTEGER DEFAULT 0,

  -- Assignment audit
  assign_edited_by   TEXT DEFAULT '',
  assign_edited_at   TEXT,

  -- Previous WCHR/WCHC (for tracking changes)
  prev_wchr          INTEGER DEFAULT 0,
  prev_wchc          INTEGER DEFAULT 0,

  created_at         TEXT DEFAULT (datetime('now')),
  updated_at         TEXT DEFAULT (datetime('now'))
);

-- Users table (replaces USERS Google Sheet)
CREATE TABLE IF NOT EXISTS users (
  username TEXT PRIMARY KEY,
  pin      TEXT NOT NULL,
  role     TEXT NOT NULL   -- Dispatch | Lead | Mgmt
);

-- Gate -> Zone overrides (replaces Zone_Overrides sheet)
CREATE TABLE IF NOT EXISTS zone_overrides (
  gate  TEXT PRIMARY KEY,  -- normalized (uppercase, no spaces)
  zone  TEXT NOT NULL,     -- "Pier A" | "TB" | "Gates" | "T1" | "Unassigned" | "SwingDoor"
  notes TEXT DEFAULT ''
);

-- US airport IATA codes (replaces US_Codes sheet)
CREATE TABLE IF NOT EXISTS us_airport_codes (
  code TEXT PRIMARY KEY    -- 3-letter IATA, uppercase
);

-- Nightly archive table
CREATE TABLE IF NOT EXISTS archive (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  ops_date    TEXT NOT NULL,               -- "2025-02-25" (the ops day archived)
  archived_at TEXT DEFAULT (datetime('now')),
  flight_data TEXT NOT NULL                -- JSON of the flight row
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_flights_time_est   ON flights(time_est);
CREATE INDEX IF NOT EXISTS idx_flights_zone        ON flights(zone_current);
CREATE INDEX IF NOT EXISTS idx_flights_type        ON flights(type);
CREATE INDEX IF NOT EXISTS idx_archive_ops_date    ON archive(ops_date);
