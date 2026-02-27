/**
 * PRM D1 Worker  (Phase 1 — exact replica, D1 backend)
 *
 * What changed vs api/src/index.js:
 *   - Auth against D1 `users` table instead of Google Sheet
 *   - Flights read/written from D1 `flights` table instead of Dispatch_DB sheet
 *   - FIDS sync runs via Cloudflare Cron Trigger (replaces Google Apps Script)
 *
 * Same API surface → frontend (web/*.html + config.js) works unchanged.
 *
 * Required secrets (wrangler secret put):
 *   AUTH_SECRET        — HMAC signing key
 *   AERODATABOX_KEY    — RapidAPI key for AeroDataBox
 *
 * Required wrangler.toml vars:
 *   TIMEZONE           — default "America/Toronto"
 *
 * D1 binding:
 *   DB                 — prm-dispatch database
 */

// ─────────────────────────────────────────────────────────────
// § 1  CONSTANTS
// ─────────────────────────────────────────────────────────────

const DEFAULT_TZ = "America/Toronto";

// Zone canonical names (must match GAS constants exactly)
const ZONE_PIERA      = "Pier A";
const ZONE_TB         = "TB";
const ZONE_GATES      = "Gates";
const ZONE_T1         = "T1";
const ZONE_UNASSIGNED = "Unassigned";

// Fixed gate lists (mirrors GAS)
const PIERA_GATES = new Set(["B2A","B2C","B3","B4","B5","B20","B22"]);
const TB_GATES    = new Set(["A6","A7","A8","A9","A10","A11","A12","A13","A14","A15"]);
const SWING_MIN   = 15;
const SWING_MAX   = 19;
const GATES_MIN   = 23;
const GATES_MAX   = 41;

// Region codes (mirrors GAS)
const REGION_DOM  = "DOM";
const REGION_US   = "US";
const REGION_INTL = "INTL";

// Time-change threshold (minutes)
const TIME_DELTA_THRESHOLD = 20;

// Board → ACK column name
const BOARD_ACK_COL = {
  DISPATCH:   "dispatch_ack",
  PIERA:      "piera_ack",
  TB:         "tb_ack",
  T1:         "t1_ack",
  UNASSIGNED: "unassigned_ack",
  GATES:      "gates_ack",
};

// Zone label → board key
const ZONE_TO_BOARD = {
  [ZONE_PIERA]:      "PIERA",
  [ZONE_TB]:         "TB",
  [ZONE_GATES]:      "GATES",
  [ZONE_T1]:         "T1",
  [ZONE_UNASSIGNED]: "UNASSIGNED",
};

// AeroDataBox — airlines we care about
const WATCH_AIRLINES = new Set([
  "AF","BG","2T","BW","CA","MU","HU","AU","DL","LH",
  "EY","BR","F8","AZ","KL","PR","PD","S4","SV","LX",
  "TK","TS","VS","WS",
]);

const ROLE_ACCESS = {
  Dispatch: { dispatch: true,  lead: false, mgmt: false },
  Lead:     { dispatch: false, lead: true,  mgmt: false },
  Mgmt:     { dispatch: true,  lead: true,  mgmt: true  },
};

// In-memory write-through patch (best-effort, same-isolate only)
const _patches = new Map(); // key → { patch, expAt }
const PATCH_TTL_MS = 12_000;

// ─────────────────────────────────────────────────────────────
// § 2  CORE UTILITIES
// ─────────────────────────────────────────────────────────────

const json = (obj, init = {}) => {
  const h = new Headers(init.headers || {});
  h.set("content-type", "application/json; charset=utf-8");
  return new Response(JSON.stringify(obj), { ...init, headers: h });
};

const withCors = (res, origin = "*") => {
  const h = new Headers(res.headers);
  h.set("access-control-allow-origin", origin);
  h.set("access-control-allow-methods", "GET,POST,PATCH,OPTIONS");
  h.set("access-control-allow-headers", "content-type,authorization");
  h.set("access-control-max-age", "86400");
  return new Response(res.body, { status: res.status, statusText: res.statusText, headers: h });
};

const base64url = {
  enc: (buf) => {
    const b64 = btoa(String.fromCharCode(...new Uint8Array(buf)));
    return b64.replace(/\+/g,"-").replace(/\//g,"_").replace(/=+$/g,"");
  },
  encStr: (s) => base64url.enc(new TextEncoder().encode(s)),
  decToBuf: (s) => {
    s = s.replace(/-/g,"+").replace(/_/g,"/");
    while (s.length % 4) s += "=";
    const bin = atob(s);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out.buffer;
  },
  decToStr: (s) => new TextDecoder().decode(new Uint8Array(base64url.decToBuf(s))),
};

// ─────────────────────────────────────────────────────────────
// § 3  TIMEZONE UTILITIES (identical to api/src/index.js)
// ─────────────────────────────────────────────────────────────

const _dtfCache    = new Map();
const _timeOnlyFmt = new Map();

function _getTzFormatter(tz) {
  let f = _dtfCache.get(tz);
  if (!f) {
    f = new Intl.DateTimeFormat("en-CA", {
      timeZone: tz,
      year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", minute: "2-digit", second: "2-digit",
      hour12: false,
    });
    _dtfCache.set(tz, f);
  }
  return f;
}

function getTzParts(date, tz) {
  const parts = Object.fromEntries(
    _getTzFormatter(tz).formatToParts(date).map(p => [p.type, p.value])
  );
  return {
    year:   Number(parts.year),
    month:  Number(parts.month),
    day:    Number(parts.day),
    hour:   Number(parts.hour),
    minute: Number(parts.minute),
    second: Number(parts.second),
  };
}

/** DST-safe: convert local wall-clock time in `tz` → UTC Date */
function zonedTimeToUtc({ year, month, day, hour, minute, second }, tz) {
  let utc = Date.UTC(year, month - 1, day, hour, minute, second || 0);
  for (let i = 0; i < 3; i++) {
    const got = getTzParts(new Date(utc), tz);
    const want = Date.UTC(year, month - 1, day, hour, minute, second || 0);
    const have = Date.UTC(got.year, got.month - 1, got.day, got.hour, got.minute, got.second);
    const diff = want - have;
    if (diff === 0) break;
    utc += diff;
  }
  return new Date(utc);
}

function addDaysLocal(ymd, deltaDays, tz) {
  const noon = Date.UTC(ymd.year, ymd.month - 1, ymd.day, 12, 0, 0);
  const p    = getTzParts(new Date(noon + deltaDays * 86_400_000), tz);
  return { year: p.year, month: p.month, day: p.day };
}

/**
 * Compute operational window in Toronto time.
 * Ops day: 03:00 → next-day 02:59:59.999
 * Lookback cap: show at most 1 hour of past flights (default behaviour).
 *
 * Optional overrides:
 *   fromTime  "HH:MM" Toronto local — custom start (no lookback cap)
 *   toTime    "HH:MM" Toronto local — custom end
 *   opsDay    "current" | "next"  — shift to next ops day
 */
function computeOpsWindow(now = new Date(), { fromTime, toTime, opsDay } = {}) {
  const tz = DEFAULT_TZ;
  const p  = getTzParts(now, tz);

  let opDate = { year: p.year, month: p.month, day: p.day };
  if (p.hour < 3) opDate = addDaysLocal(opDate, -1, tz);

  // Shift to next ops day if requested
  if (opsDay === "next") opDate = addDaysLocal(opDate, 1, tz);

  const opsStart = zonedTimeToUtc({ ...opDate, hour: 3, minute: 0, second: 0 }, tz);
  const endDate  = addDaysLocal(opDate, 1, tz);
  const opsEnd   = zonedTimeToUtc({ ...endDate, hour: 2, minute: 59, second: 59 }, tz);
  opsEnd.setUTCMilliseconds(999);

  let start, end;

  if (fromTime) {
    const [fh, fm] = fromTime.split(":").map(Number);
    // Times < 03:00 belong to the next calendar day within this ops day
    const fromDate = fh < 3 ? addDaysLocal(opDate, 1, tz) : opDate;
    start = zonedTimeToUtc({ ...fromDate, hour: fh, minute: fm, second: 0 }, tz);
  } else if (opsDay === "next") {
    start = opsStart; // show full next ops day (no lookback cap)
  } else {
    // 1-hour lookback cap (default behaviour)
    start = opsStart;
    const lookback = new Date(now.getTime() - 60 * 60 * 1000);
    if (lookback > start) start = lookback;
  }

  if (toTime) {
    const [th, tm] = toTime.split(":").map(Number);
    const toDate = th < 3 ? addDaysLocal(opDate, 1, tz) : opDate;
    end = zonedTimeToUtc({ ...toDate, hour: th, minute: tm, second: 59 }, tz);
    end.setUTCMilliseconds(999);
  } else {
    end = opsEnd;
  }

  return {
    start,
    end,
    startISO: start.toISOString(),
    endISO:   end.toISOString(),
  };
}

/**
 * Full ops window with NO lookback cap — used for FIDS sync and archive.
 * Toronto 03:00 → next day 02:59:59.
 * After 12:00 noon (or before 03:00) also includes NEXT ops-day so the
 * "next day" toggle can show pre-loaded flights.
 */
function computeFullOpsWindow(now = new Date()) {
  const tz = DEFAULT_TZ;
  const p  = getTzParts(now, tz);

  let opDate = { year: p.year, month: p.month, day: p.day };
  if (p.hour < 3) opDate = addDaysLocal(opDate, -1, tz);

  const start  = zonedTimeToUtc({ ...opDate, hour: 3, minute: 0, second: 0 }, tz);
  const d1     = addDaysLocal(opDate, 1, tz);
  let   end    = zonedTimeToUtc({ ...d1, hour: 2, minute: 59, second: 59 }, tz);
  end.setUTCMilliseconds(999);

  // After 12:00 noon or before 03:00: also preload next ops-day
  if (p.hour >= 12 || p.hour < 3) {
    const d2 = addDaysLocal(opDate, 2, tz);
    end       = zonedTimeToUtc({ ...d2, hour: 2, minute: 59, second: 59 }, tz);
    end.setUTCMilliseconds(999);
  }

  return { start, end };
}

// ─────────────────────────────────────────────────────────────
// § 4  ZONE LOGIC  (exact replica of GAS getZoneForFlight_)
// ─────────────────────────────────────────────────────────────

function normalizeGate(gate) {
  if (!gate) return "";
  let g = String(gate).trim().toUpperCase();
  g = g.replace(/^GATE\s*/, "");
  g = g.replace(/[\s\-]+/g, "");
  return g;
}

function getGateNumber(gate) {
  const g = normalizeGate(gate);
  const m = g.match(/(\d+)/);
  return m ? parseInt(m[1], 10) : null;
}

function resolveSwingDoor(typeLabel, region) {
  if (region === REGION_US)   return ZONE_TB;
  if (region === REGION_INTL) return typeLabel === "ARR" ? ZONE_TB : ZONE_PIERA;
  if (region === REGION_DOM)  return ZONE_PIERA;
  return ZONE_TB;
}

function getRegionForPort(portCode, usMap /* Set<string> */) {
  if (!portCode) return "";
  const code = String(portCode).trim().toUpperCase();
  if (usMap.has(code))       return REGION_US;
  if (code.charAt(0) === "Y") return REGION_DOM;
  return REGION_INTL;
}

/**
 * Determine zone for a flight.
 * @param {string} typeLabel  "ARR" | "DEP"
 * @param {string} gate       raw gate string from FIDS
 * @param {string} terminal   raw terminal string from FIDS
 * @param {string} region     "DOM" | "US" | "INTL" | ""
 * @param {Map}    overrides  gate(norm) → zone
 */
function getZoneForFlight(typeLabel, gate, terminal, region, overrides) {
  const tStr = terminal != null ? String(terminal).trim().toUpperCase() : "";
  const g    = normalizeGate(gate);

  // Zone overrides win (check BEFORE gate-number rules)
  if (overrides && g && overrides.has(g)) {
    const raw   = overrides.get(g).trim();
    const token = raw.toUpperCase().replace(/\s+/g, "");
    if (token === "SWINGDOOR")  return resolveSwingDoor(typeLabel, region);
    if (token === "UNASSIGNED") return ZONE_UNASSIGNED;
    return raw;
  }

  // No gate → terminal fallback
  if (!g) {
    if (tStr === "1" || tStr === "T1") return ZONE_T1;
    return ZONE_UNASSIGNED;
  }

  const num = getGateNumber(g);

  if (PIERA_GATES.has(g))                                    return ZONE_PIERA;
  if (TB_GATES.has(g))                                       return ZONE_TB;
  if (num !== null && num >= GATES_MIN && num <= GATES_MAX)  return ZONE_GATES;
  if (num !== null && num >= SWING_MIN && num <= SWING_MAX)  return resolveSwingDoor(typeLabel, region);
  if (tStr === "1" || tStr === "T1")                         return ZONE_T1;
  return ZONE_UNASSIGNED;
}

// ─────────────────────────────────────────────────────────────
// § 5  ZONE / BOARD HELPERS
// ─────────────────────────────────────────────────────────────

function normalizeZone(z) {
  const s = (z == null) ? "" : String(z).trim();
  if (!s) return "";
  const up = s.toUpperCase().replace(/\s+/g, " ");
  if (up === "PIER A" || up === "PIERA") return ZONE_PIERA;
  if (up === "TB")                        return ZONE_TB;
  if (up === "GATE" || up === "GATES")    return ZONE_GATES;
  if (up === "T1" || up === "TERMINAL 1") return ZONE_T1;
  if (up === "UNASSIGNED")                return ZONE_UNASSIGNED;
  if (up === "ALL")                       return "ALL";
  return s;
}

function zoneToBoard(zone) {
  const z = (zone || "").trim().toUpperCase().replace(/\s+/g, "");
  if (z === "PIERA")                  return "PIERA";
  if (z === "TB")                     return "TB";
  if (z === "T1")                     return "T1";
  if (z === "UNASSIGNED")             return "UNASSIGNED";
  if (z === "GATES" || z === "GATE") return "GATES";
  return null;
}

function getBoardAck(row, board) {
  const col = BOARD_ACK_COL[(board || "").toUpperCase()];
  return col ? (row[col] === 1 || row[col] === true) : false;
}

function isTrue(v) {
  if (v === true || v === 1) return true;
  const s = String(v ?? "").trim().toLowerCase();
  return s === "true" || s === "1" || s === "yes";
}

// ─────────────────────────────────────────────────────────────
// § 6  AUTH  (HMAC tokens — identical to api/src/index.js)
// ─────────────────────────────────────────────────────────────

let _hmacKeyPromise = null;

async function getHmacKey(env) {
  if (_hmacKeyPromise) return _hmacKeyPromise;
  const secret = env.AUTH_SECRET || "";
  if (!secret) throw new Error("Missing AUTH_SECRET.");
  _hmacKeyPromise = crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"]
  );
  return _hmacKeyPromise;
}

async function signToken(env, payload) {
  const payloadB64 = base64url.encStr(JSON.stringify(payload));
  const key  = await getHmacKey(env);
  const sig  = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(payloadB64));
  return `${payloadB64}.${base64url.enc(sig)}`;
}

async function verifyToken(env, token) {
  token = String(token || "").trim();
  const [payloadB64, sigB64] = token.split(".");
  if (!payloadB64 || !sigB64) return { ok: false, error: "Invalid token." };

  const key = await getHmacKey(env);
  const ok  = await crypto.subtle.verify(
    "HMAC", key,
    base64url.decToBuf(sigB64),
    new TextEncoder().encode(payloadB64)
  );
  if (!ok) return { ok: false, error: "Invalid token." };

  let payload;
  try { payload = JSON.parse(base64url.decToStr(payloadB64)); }
  catch { return { ok: false, error: "Invalid token." }; }

  if (!payload?.expAt || Date.now() > payload.expAt)
    return { ok: false, error: "Session expired. Please login again." };

  const access = ROLE_ACCESS[payload.role] || null;
  if (!access) return { ok: false, error: `Invalid role: ${payload.role}` };

  return { ok: true, user: payload, access };
}

function getBearer(req) {
  const h = req.headers.get("authorization") || "";
  const m = h.match(/^\s*Bearer\s+(.+)\s*$/i);
  return m ? m[1] : "";
}

async function requireAuth(req, env, app) {
  const token = getBearer(req);
  if (!token) throw new Error("Missing Authorization token");
  const v = await verifyToken(env, token);
  if (!v.ok) throw new Error(v.error || "Unauthorized");
  if (app) {
    if (!v.access[String(app).toLowerCase()])
      throw new Error(`No access to ${app}`);
  }
  return v;
}

// ─────────────────────────────────────────────────────────────
// § 7  FIDS FETCHER  (mirrors GAS fetchFlairYYZ)
// ─────────────────────────────────────────────────────────────

function normalizeFlightNoKey(n) {
  return (n || "").toString().replace(/\s+/g, "").trim().toUpperCase();
}

function formatFlightNo(n) {
  const s = normalizeFlightNoKey(n);
  if (s.length <= 2) return s;
  return s.substring(0, 2) + " " + s.substring(2);
}

function dedupeByKey(list, keyFn) {
  const seen = new Set();
  const out  = [];
  for (const item of list) {
    const k = keyFn(item);
    if (!k || seen.has(k)) continue;
    seen.add(k);
    out.push(item);
  }
  return out;
}

async function fetchWindowPaged(url, apiKey, airlineSet) {
  const PAGE_SIZE   = 300;
  const MAX_PAGES   = 4;
  const TARGET_KEEP = 500;

  let allArr = [], allDep = [];
  let offset = 0, pages = 0;

  while (pages < MAX_PAGES) {
    const resp = await fetch(`${url}&limit=${PAGE_SIZE}&offset=${offset}`, {
      headers: {
        "X-RapidAPI-Key":  apiKey,
        "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com",
      },
    });

    if (!resp.ok) {
      const txt = await resp.text().catch(() => "");
      throw new Error(`AeroDataBox ${resp.status}: ${txt.slice(0, 200)}`);
    }

    const res = await resp.json();
    const arr = (res.arrivals   || []).filter(f => airlineSet.has(normalizeFlightNoKey(f.number).substring(0, 2)));
    const dep = (res.departures || []).filter(f => airlineSet.has(normalizeFlightNoKey(f.number).substring(0, 2)));

    allArr = allArr.concat(arr);
    allDep = allDep.concat(dep);

    if ((res.arrivals?.length ?? 0) + (res.departures?.length ?? 0) < PAGE_SIZE) break;
    if ((allArr.length + allDep.length) >= TARGET_KEEP) break;

    offset += PAGE_SIZE;
    pages++;
  }

  return { arrivals: allArr, departures: allDep };
}

/**
 * Fetch FIDS data from AeroDataBox (mirrors GAS fetchFlairYYZ).
 * Returns { arrivals: [{flight,origin,sched,est,terminal,gate},...],
 *           departures:[{flight,dest,sched,est,terminal,gate},...] }
 */
async function fetchFIDSData(env) {
  const apiKey = env.AERODATABOX_KEY || "";
  if (!apiKey) throw new Error("Missing AERODATABOX_KEY secret.");

  const tz  = DEFAULT_TZ;
  const now = new Date();
  const p   = getTzParts(now, tz);

  // Current ops-day base date (before 03:00 = yesterday)
  let opBase = { year: p.year, month: p.month, day: p.day };
  if (p.hour < 3) opBase = addDaysLocal(opBase, -1, tz);

  function fmtDate(ymd) {
    return `${ymd.year}-${String(ymd.month).padStart(2,"0")}-${String(ymd.day).padStart(2,"0")}`;
  }

  const d0 = fmtDate(opBase);
  const d1 = fmtDate(addDaysLocal(opBase, 1, tz));
  const d2 = fmtDate(addDaysLocal(opBase, 2, tz));

  const Q = "?withLeg=true&direction=Both&withCancelled=true&withCodeshared=false&withCargo=false&withPrivate=false&withLocation=false";
  const BASE = "https://aerodatabox.p.rapidapi.com/flights/airports/iata/YYZ";

  // AeroDataBox max window = 12 hours.  Split 03:00→02:59 into 3 chunks:
  //   03:00→15:00 (12h)  |  15:00→02:59 (11h59m)
  const windows = [
    `${BASE}/${d0}T03:00/${d0}T15:00${Q}`,
    `${BASE}/${d0}T15:00/${d1}T02:59${Q}`,
  ];
  // After 12:00 noon or before 03:00: also preload next ops-day
  if (p.hour >= 12 || p.hour < 3) {
    windows.push(`${BASE}/${d1}T03:00/${d1}T15:00${Q}`);
    windows.push(`${BASE}/${d1}T15:00/${d2}T02:59${Q}`);
  }

  let rawArr = [], rawDep = [];
  for (const url of windows) {
    const { arrivals, departures } = await fetchWindowPaged(url, apiKey, WATCH_AIRLINES);
    rawArr = rawArr.concat(arrivals);
    rawDep = rawDep.concat(departures);
  }

  // Dedupe
  rawArr = dedupeByKey(rawArr, f => {
    const t = f.arrival?.scheduledTime?.utc || f.arrival?.scheduledTime?.local || "";
    return `${normalizeFlightNoKey(f.number)}|${t}`;
  });
  rawDep = dedupeByKey(rawDep, f => {
    const t = f.departure?.scheduledTime?.utc || f.departure?.scheduledTime?.local || "";
    return `${normalizeFlightNoKey(f.number)}|${t}`;
  });

  // Shape
  const arrivals   = [];
  const departures = [];

  for (const f of rawArr) {
    const code = normalizeFlightNoKey(f.number).substring(0, 2);
    if (!WATCH_AIRLINES.has(code)) continue;
    const cs = (f.codeshareStatus || "").toLowerCase();
    if (cs.includes("codeshared")) continue;

    arrivals.push({
      flight:   formatFlightNo(f.number),
      origin:   f.departure?.airport?.iata || "",
      sched:    f.arrival?.scheduledTime?.local  || f.arrival?.scheduledTime?.utc  || "",
      est:      f.arrival?.revisedTime?.local    || f.arrival?.scheduledTime?.local || "",
      terminal: f.arrival?.terminal || "",
      gate:     f.arrival?.gate     || "",
    });
  }

  for (const f of rawDep) {
    const code = normalizeFlightNoKey(f.number).substring(0, 2);
    if (!WATCH_AIRLINES.has(code)) continue;
    const cs = (f.codeshareStatus || "").toLowerCase();
    if (cs.includes("codeshared")) continue;

    departures.push({
      flight:   formatFlightNo(f.number),
      dest:     f.arrival?.airport?.iata || "",
      sched:    f.departure?.scheduledTime?.local || f.departure?.scheduledTime?.utc  || "",
      est:      f.departure?.revisedTime?.local   || f.departure?.scheduledTime?.local || "",
      terminal: f.departure?.terminal || "",
      gate:     f.departure?.gate     || "",
    });
  }

  return { arrivals, departures };
}

// ─────────────────────────────────────────────────────────────
// § 8  FIDS → D1 SYNC  (mirrors GAS syncDispatchDB)
// ─────────────────────────────────────────────────────────────

/** Parse AeroDataBox local-time-with-offset string → ISO 8601 UTC string */
function parseAeroTime(s) {
  if (!s) return null;
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : d.toISOString();
}

/**
 * Build the canonical flight key.
 * Format: "YYYY-MM-DD|TYPE|FLIGHT|HH:mm"   (all in Toronto local time)
 * Matches GAS buildKey() exactly.
 */
function buildFlightKey(typeLabel, flightNo, schedIso, tz) {
  if (!flightNo || !schedIso) return "";
  const d = new Date(schedIso);
  if (isNaN(d.getTime())) return "";
  const p = getTzParts(d, tz);
  const dateStr = `${p.year}-${String(p.month).padStart(2,"0")}-${String(p.day).padStart(2,"0")}`;
  const timeStr = `${String(p.hour).padStart(2,"0")}:${String(p.minute).padStart(2,"0")}`;
  return `${dateStr}|${typeLabel}|${flightNo}|${timeStr}`;
}

function rebuildAlertText(gate_changed, gate_chg_from_gate, gate_chg_to_gate,
                          zone_changed, zone_chg_from, zone_chg_to,
                          time_changed, time_delta_min) {
  const parts = [];
  if (gate_changed) {
    const fg = gate_chg_from_gate || "";
    const tg = gate_chg_to_gate   || "";
    if (fg || tg) parts.push(`Gate: ${fg} -> ${tg}`);
  }
  if (zone_changed) {
    const fz = zone_chg_from || "";
    const tz = zone_chg_to   || "";
    if (fz || tz) parts.push(`Zone: ${fz} -> ${tz}`);
  }
  if (time_changed && time_delta_min != null) {
    parts.push(`TimeDelta: ${time_delta_min} min`);
  }
  return parts.join(" | ");
}

/**
 * Full FIDS → D1 sync — exact replica of GAS syncDispatchDB.
 * Manual fields (wchr, wchc, comment, assignment, pax_assisted) are NEVER overwritten.
 * Per-board ACKs are reset only when a NEW change is detected.
 */
async function syncFIDSToD1(env, arrivals, departures) {
  const now    = new Date();
  const nowIso = now.toISOString();
  const tz     = DEFAULT_TZ;

  // ── Load zone overrides ──────────────────────────────────
  const { results: ovRows } = await env.DB.prepare(
    "SELECT gate, zone FROM zone_overrides"
  ).all();
  const zoneOverrides = new Map(ovRows.map(r => [r.gate, r.zone]));

  // ── Load US airport codes ────────────────────────────────
  const { results: usRows } = await env.DB.prepare(
    "SELECT code FROM us_airport_codes"
  ).all();
  const usMap = new Set(usRows.map(r => r.code));

  // ── Load ALL existing flights (key → row) ────────────────
  const { results: existing } = await env.DB.prepare("SELECT * FROM flights").all();
  const existingMap = new Map(existing.map(r => [r.key, r]));

  const toInsert = [];
  const toUpdate = [];

  function processFlights(flights, typeLabel) {
    for (const f of flights) {
      const schedIso = parseAeroTime(f.sched);
      const estIso   = parseAeroTime(f.est) || schedIso;
      if (!f.flight || !schedIso) continue;

      const key = buildFlightKey(typeLabel, f.flight, schedIso, tz);
      if (!key) continue;

      const originOrDest = typeLabel === "ARR" ? (f.origin || "") : (f.dest || "");
      const region  = getRegionForPort(originOrDest, usMap);
      const newZone = getZoneForFlight(typeLabel, f.gate, f.terminal, region, zoneOverrides) || ZONE_UNASSIGNED;
      const newGateNorm = normalizeGate(f.gate);

      const ex = existingMap.get(key);

      if (!ex) {
        // ── NEW FLIGHT ─────────────────────────────────────
        toInsert.push({
          key,
          type:         typeLabel,
          flight:       f.flight,
          time_est:     estIso,
          sched:        schedIso,
          origin_dest:  originOrDest,
          gate:         f.gate || "",
          zone_current: newZone,
          zone_previous: newZone,
          zone_prev:    "",
          alert_text:   "",
          created_at:   nowIso,
          updated_at:   nowIso,
        });

      } else {
        // ── EXISTING FLIGHT ────────────────────────────────
        const oldGateNorm = normalizeGate(ex.gate);
        const oldZone     = ex.zone_current || "";
        const oldEst      = ex.time_est ? new Date(ex.time_est) : null;

        // Carry forward existing change flags + ACKs
        let u = {
          key,
          flight:              f.flight,
          time_est:            estIso,
          sched:               schedIso,
          origin_dest:         originOrDest,
          gate:                f.gate || "",

          zone_current:        ex.zone_current || newZone,
          zone_previous:       ex.zone_previous || ex.zone_current || newZone,
          zone_prev:           ex.zone_prev || "",

          gate_changed:        ex.gate_changed,
          gate_chg_time:       ex.gate_chg_time,
          gate_chg_from_zone:  ex.gate_chg_from_zone,
          gate_chg_to_zone:    ex.gate_chg_to_zone,
          gate_chg_from_gate:  ex.gate_chg_from_gate,
          gate_chg_to_gate:    ex.gate_chg_to_gate,

          time_prev_est:       ex.time_prev_est,
          time_changed:        ex.time_changed,
          time_delta_min:      ex.time_delta_min,
          time_chg_time:       ex.time_chg_time,

          zone_changed:        ex.zone_changed,
          zone_chg_time:       ex.zone_chg_time,
          zone_chg_from:       ex.zone_chg_from,
          zone_chg_to:         ex.zone_chg_to,

          dispatch_ack:        ex.dispatch_ack,
          piera_ack:           ex.piera_ack,
          tb_ack:              ex.tb_ack,
          t1_ack:              ex.t1_ack,
          unassigned_ack:      ex.unassigned_ack,
          gates_ack:           ex.gates_ack,

          updated_at:          nowIso,
        };

        let anyNewChange = false;

        // ── Gate change detection ──────────────────────────
        if (newGateNorm && oldGateNorm && newGateNorm !== oldGateNorm) {
          u.gate_changed       = 1;
          u.gate_chg_time      = nowIso;
          u.gate_chg_from_gate = ex.gate || "";
          u.gate_chg_to_gate   = f.gate  || "";
          u.gate_chg_from_zone = oldZone;
          anyNewChange         = true;
        }

        // ── Zone change detection ──────────────────────────
        if (oldZone && newZone && oldZone !== newZone) {
          // ZonePrev carry-over logic (mirrors GAS applyZoneChangeRow_)
          const curPrev = (ex.zone_prev || "").trim();
          let canOverwrite = true;
          if (curPrev) {
            const prevBoard = zoneToBoard(curPrev);
            if (prevBoard && !getBoardAck(ex, prevBoard)) canOverwrite = false;
          }
          if (canOverwrite) u.zone_prev = oldZone;

          u.zone_current  = newZone;
          u.zone_changed  = 1;
          u.zone_chg_time = nowIso;
          u.zone_chg_from = oldZone;
          u.zone_chg_to   = newZone;
          anyNewChange    = true;
        }

        // Fill gate_chg_to_zone after zone is settled
        if (u.gate_changed) u.gate_chg_to_zone = u.zone_current || "";

        // ── Time change detection ──────────────────────────
        if (oldEst) {
          const diffMin = Math.round((new Date(estIso).getTime() - oldEst.getTime()) / 60_000);
          if (Math.abs(diffMin) >= TIME_DELTA_THRESHOLD) {
            u.time_prev_est  = ex.time_est;
            u.time_changed   = 1;
            u.time_delta_min = diffMin;
            u.time_chg_time  = nowIso;
            anyNewChange     = true;
          }
        }

        // ── Reset ALL ACKs on any new change ──────────────
        if (anyNewChange) {
          u.dispatch_ack   = 0;
          u.piera_ack      = 0;
          u.tb_ack         = 0;
          u.t1_ack         = 0;
          u.unassigned_ack = 0;
          u.gates_ack      = 0;
        }

        // Rebuild alert text
        u.alert_text = rebuildAlertText(
          isTrue(u.gate_changed), u.gate_chg_from_gate, u.gate_chg_to_gate,
          isTrue(u.zone_changed), u.zone_chg_from, u.zone_chg_to,
          isTrue(u.time_changed), u.time_delta_min
        );

        toUpdate.push(u);
      }
    }
  }

  processFlights(arrivals,   "ARR");
  processFlights(departures, "DEP");

  // ── Batch INSERT new flights ───────────────────────────────
  if (toInsert.length > 0) {
    const INS = env.DB.prepare(`
      INSERT INTO flights
        (key,type,flight,time_est,sched,origin_dest,gate,
         zone_current,zone_previous,zone_prev,alert_text,
         dispatch_ack,piera_ack,tb_ack,t1_ack,unassigned_ack,gates_ack,
         created_at,updated_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    `);
    const batch = toInsert.map(r => INS.bind(
      r.key, r.type, r.flight, r.time_est, r.sched, r.origin_dest, r.gate,
      r.zone_current, r.zone_previous, r.zone_prev, r.alert_text,
      0, 0, 0, 0, 0, 0,
      r.created_at, r.updated_at
    ));
    for (let i = 0; i < batch.length; i += 100)
      await env.DB.batch(batch.slice(i, i + 100));
  }

  // ── Batch UPDATE existing flights (FIDS fields only) ──────
  if (toUpdate.length > 0) {
    const UPD = env.DB.prepare(`
      UPDATE flights SET
        flight=?,time_est=?,sched=?,origin_dest=?,gate=?,
        zone_current=?,zone_previous=?,zone_prev=?,
        gate_changed=?,gate_chg_time=?,gate_chg_from_zone=?,gate_chg_to_zone=?,
        gate_chg_from_gate=?,gate_chg_to_gate=?,
        time_prev_est=?,time_changed=?,time_delta_min=?,time_chg_time=?,
        zone_changed=?,zone_chg_time=?,zone_chg_from=?,zone_chg_to=?,
        alert_text=?,
        dispatch_ack=?,piera_ack=?,tb_ack=?,t1_ack=?,unassigned_ack=?,gates_ack=?,
        updated_at=?
      WHERE key=?
    `);
    const batch = toUpdate.map(u => UPD.bind(
      u.flight, u.time_est, u.sched, u.origin_dest, u.gate,
      u.zone_current, u.zone_previous, u.zone_prev,
      u.gate_changed, u.gate_chg_time, u.gate_chg_from_zone, u.gate_chg_to_zone,
      u.gate_chg_from_gate, u.gate_chg_to_gate,
      u.time_prev_est, u.time_changed, u.time_delta_min, u.time_chg_time,
      u.zone_changed, u.zone_chg_time, u.zone_chg_from, u.zone_chg_to,
      u.alert_text,
      u.dispatch_ack, u.piera_ack, u.tb_ack, u.t1_ack, u.unassigned_ack, u.gates_ack,
      u.updated_at,
      u.key
    ));
    for (let i = 0; i < batch.length; i += 100)
      await env.DB.batch(batch.slice(i, i + 100));
  }

  console.log(`[sync] inserted=${toInsert.length} updated=${toUpdate.length}`);
  return { inserted: toInsert.length, updated: toUpdate.length };
}

// ─────────────────────────────────────────────────────────────
// § 8b  ARCHIVE RETRIEVAL (Mgmt)
// ─────────────────────────────────────────────────────────────

async function handleArchiveDates(req, env) {
  const { results } = await env.DB.prepare(
    "SELECT DISTINCT ops_date, COUNT(*) as flight_count FROM archive GROUP BY ops_date ORDER BY ops_date DESC"
  ).all();
  return json({ ok: true, dates: results.map(r => ({ date: r.ops_date, flights: r.flight_count })) });
}

async function handleArchiveRows(req, env) {
  const url = new URL(req.url);
  const opsDate = (url.searchParams.get("date") || "").trim();
  if (!opsDate || !/^\d{4}-\d{2}-\d{2}$/.test(opsDate))
    return json({ ok: false, error: "Provide ?date=YYYY-MM-DD" }, { status: 400 });

  const { results } = await env.DB.prepare(
    "SELECT flight_data FROM archive WHERE ops_date = ? ORDER BY id"
  ).bind(opsDate).all();

  const rows = results.map(r => {
    try { return JSON.parse(r.flight_data); } catch { return null; }
  }).filter(Boolean);

  return json({ ok: true, opsDate, flights: rows.length, rows });
}

// § 9  NIGHTLY ARCHIVE  (mirrors GAS nightlyArchive)
// ─────────────────────────────────────────────────────────────

async function nightlyArchive(env) {
  const now = new Date();
  const tz  = DEFAULT_TZ;
  const p   = getTzParts(now, tz);

  // Archive yesterday's ops-day (before 03:00 → use day-2)
  let archiveBase = { year: p.year, month: p.month, day: p.day };
  if (p.hour < 3) archiveBase = addDaysLocal(archiveBase, -2, tz);
  else             archiveBase = addDaysLocal(archiveBase, -1, tz);

  const archStart = zonedTimeToUtc({ ...archiveBase, hour: 3, minute: 0, second: 0 }, tz);
  const archEnd   = zonedTimeToUtc({ ...addDaysLocal(archiveBase, 1, tz), hour: 2, minute: 59, second: 59 }, tz);
  archEnd.setUTCMilliseconds(999);

  const opsDateStr = `${archiveBase.year}-${String(archiveBase.month).padStart(2,"0")}-${String(archiveBase.day).padStart(2,"0")}`;

  // Fetch flights in the archive window
  const { results } = await env.DB.prepare(
    "SELECT * FROM flights WHERE time_est >= ? AND time_est <= ?"
  ).bind(archStart.toISOString(), archEnd.toISOString()).all();

  if (!results.length) {
    console.log(`[archive] no flights for ops-date ${opsDateStr}`);
    return { archived: 0 };
  }

  // Idempotent: remove any previous (possibly partial) archive for this date
  await env.DB.prepare(
    "DELETE FROM archive WHERE ops_date = ?"
  ).bind(opsDateStr).run();

  // Write to archive table (one row per flight)
  const INS = env.DB.prepare(
    "INSERT INTO archive (ops_date, flight_data) VALUES (?, ?)"
  );
  const batch = results.map(r => INS.bind(opsDateStr, JSON.stringify(r)));
  for (let i = 0; i < batch.length; i += 100)
    await env.DB.batch(batch.slice(i, i + 100));

  // Delete archived flights from the main table (keep DB clean)
  await env.DB.prepare(
    "DELETE FROM flights WHERE time_est >= ? AND time_est <= ?"
  ).bind(archStart.toISOString(), archEnd.toISOString()).run();

  console.log(`[archive] ops-date=${opsDateStr} archived=${results.length}`);
  return { archived: results.length };
}

// ─────────────────────────────────────────────────────────────
// § 10  PATCH HELPERS  (write-through cache, same-isolate)
// ─────────────────────────────────────────────────────────────

function setPatch(key, patch) {
  _patches.set(String(key), { patch, expAt: Date.now() + PATCH_TTL_MS });
}

function applyPatch(rowObj) {
  const p = _patches.get(String(rowObj.key || ""));
  if (!p) return rowObj;
  if (Date.now() > p.expAt) { _patches.delete(String(rowObj.key)); return rowObj; }
  return { ...rowObj, ...p.patch };
}

// ─────────────────────────────────────────────────────────────
// § 11  API HANDLERS
// ─────────────────────────────────────────────────────────────

// ── Auth ──────────────────────────────────────────────────────

async function handleLogin(req, env) {
  const body     = await req.json().catch(() => ({}));
  const username = String(body.username || "").trim();
  const pin      = String(body.pin      || "").trim();

  if (!username || !pin)
    return json({ ok: false, error: "Missing username or pin." }, { status: 400 });

  const { results } = await env.DB.prepare(
    "SELECT username, role, pin FROM users WHERE username = ? LIMIT 1"
  ).bind(username).all();

  const user = results[0];
  if (!user || user.pin !== pin)
    return json({ ok: false, error: "Invalid username or pin." }, { status: 401 });

  const access = ROLE_ACCESS[user.role];
  if (!access)
    return json({ ok: false, error: `Invalid role: ${user.role}` }, { status: 401 });

  const expAt = Date.now() + (6 * 60 * 60 * 1000);
  const token = await signToken(env, { username: user.username, role: user.role, expAt });
  return json({ ok: true, token, user: { username: user.username, role: user.role, expAt }, access });
}

async function handleValidate(req, env) {
  const v   = await requireAuth(req, env, "");
  const url = new URL(req.url);
  const app = (url.searchParams.get("app") || "").trim().toLowerCase();
  if (app && !v.access[app])
    return json({ ok: false, error: `No access to ${app}` }, { status: 403 });
  return json({ ok: true, user: v.user, access: v.access });
}

// ── Dispatch rows ─────────────────────────────────────────────

async function handleDispatchRows(req, env) {
  const url      = new URL(req.url);
  const fromTime = url.searchParams.get("from") || "";
  const toTime   = url.searchParams.get("to")   || "";
  const opsDay   = url.searchParams.get("opsDay") || "current";

  const win  = computeOpsWindow(new Date(), {
    fromTime: fromTime || undefined,
    toTime:   toTime   || undefined,
    opsDay:   opsDay   || undefined,
  });
  const rows = await getFlightsInWindow(env, win.startISO, win.endISO);

  const out = rows
    .map(r => {
      const acked = isTrue(r.dispatch_ack);
      return applyPatch({
        key:         r.key,
        type:        r.type,
        flight:      r.flight,
        timeEst:     r.time_est,
        sched:       r.sched,
        origin:      r.origin_dest,
        gate:        r.gate,
        zone:        r.zone_current,

        alert:       acked ? "" : (r.alert_text || ""),
        gateChanged: acked ? false : isTrue(r.gate_changed),
        zoneChanged: acked ? false : isTrue(r.zone_changed),
        timeChanged: acked ? false : isTrue(r.time_changed),

        timeDelta:   acked ? ""  : String(r.time_delta_min ?? ""),
        timeChgAt:   acked ? ""  : (r.time_chg_time || ""),

        wchr:        String(r.wchr ?? ""),
        wchc:        String(r.wchc ?? ""),
        comment:     r.comment || "",
        assignment:  r.assignment || "",
        pax:         String(r.pax_assisted ?? ""),
      });
    })
    .sort((a, b) => new Date(a.timeEst).getTime() - new Date(b.timeEst).getTime());

  return json({ ok: true, rows: out, generatedAt: new Date().toISOString() });
}

// ── Dispatch update ───────────────────────────────────────────

async function handleDispatchUpdate(req, env) {
  const body    = await req.json().catch(() => ({}));
  const key     = String(body.key || "");
  if (!key) throw new Error("Missing key");

  const fields = [];
  const vals   = [];
  const patch  = {};

  if (body.wchr !== undefined) {
    // Track previous value
    const { results } = await env.DB.prepare(
      "SELECT wchr FROM flights WHERE key = ?"
    ).bind(key).all();
    const oldWchr = results[0]?.wchr ?? 0;
    if (String(oldWchr) !== String(body.wchr)) {
      fields.push("prev_wchr=?"); vals.push(oldWchr);
    }
    fields.push("wchr=?");  vals.push(body.wchr);
    patch.wchr = String(body.wchr ?? "");
  }

  if (body.wchc !== undefined) {
    const { results } = await env.DB.prepare(
      "SELECT wchc FROM flights WHERE key = ?"
    ).bind(key).all();
    const oldWchc = results[0]?.wchc ?? 0;
    if (String(oldWchc) !== String(body.wchc)) {
      fields.push("prev_wchc=?"); vals.push(oldWchc);
    }
    fields.push("wchc=?");  vals.push(body.wchc);
    patch.wchc = String(body.wchc ?? "");
  }

  if (body.comment !== undefined) {
    fields.push("comment=?"); vals.push(body.comment);
    patch.comment = String(body.comment ?? "");
  }

  if (!fields.length) return json({ ok: true });

  fields.push("updated_at=?"); vals.push(new Date().toISOString());
  vals.push(key);

  await env.DB.prepare(
    `UPDATE flights SET ${fields.join(",")} WHERE key=?`
  ).bind(...vals).run();

  if (Object.keys(patch).length) setPatch(key, patch);
  return json({ ok: true });
}

// ── Dispatch ACK ──────────────────────────────────────────────

async function handleDispatchAck(req, env) {
  const body = await req.json().catch(() => ({}));
  const key  = String(body.key || "");
  if (!key) throw new Error("Missing key");

  await env.DB.prepare(
    "UPDATE flights SET dispatch_ack=1, updated_at=? WHERE key=?"
  ).bind(new Date().toISOString(), key).run();

  setPatch(key, { alert: "", gateChanged: false, timeChanged: false, zoneChanged: false });
  return json({ ok: true });
}

// ── Lead init ─────────────────────────────────────────────────

function handleLeadInit() {
  return json({
    ok:         true,
    zones:      ["TB","Gates","Pier A","T1","Unassigned"],
    serverTime: new Date().toISOString(),
  });
}

// ── Lead rows ─────────────────────────────────────────────────

async function handleLeadRows(req, env) {
  const url        = new URL(req.url);
  const zoneWanted = normalizeZone(url.searchParams.get("zone") || "TB");
  const typeFilter = String(url.searchParams.get("type") || "ALL").toUpperCase();
  const q          = String(url.searchParams.get("q") || "").trim().toUpperCase().replace(/\s+/g, "");
  const fromTime   = url.searchParams.get("from") || "";
  const toTime     = url.searchParams.get("to")   || "";
  const opsDay     = url.searchParams.get("opsDay") || "current";

  const win  = computeOpsWindow(new Date(), {
    fromTime: fromTime || undefined,
    toTime:   toTime   || undefined,
    opsDay:   opsDay   || undefined,
  });
  const rows = await getFlightsInWindow(env, win.startISO, win.endISO);

  // Board ACK column for the wanted zone
  const board   = zoneWanted !== "ALL" ? ZONE_TO_BOARD[zoneWanted] : null;

  const out = [];
  for (const r of rows) {
    // Type filter
    if (typeFilter !== "ALL" && r.type !== typeFilter) continue;

    // Flight-number search
    if (q) {
      const flightQ = (r.flight || "").toUpperCase().replace(/\s+/g, "");
      if (!flightQ.includes(q)) continue;
    }

    const zoneCur = normalizeZone(r.zone_current);
    const zonePrv = normalizeZone(r.zone_prev);

    if (zoneWanted !== "ALL") {
      const inMyZone = zoneCur === zoneWanted;
      const carryOld = zonePrv === zoneWanted; // old-zone carry-over until old-zone ACKs
      if (!inMyZone && !carryOld) continue;
    }

    // Per-board ACK filter
    const ackedHere = board ? getBoardAck(r, board) : false;
    if (ackedHere) continue;

    out.push(applyPatch({
      key:         r.key,
      type:        r.type,
      flight:      r.flight,
      timeEst:     r.time_est,
      origin:      r.origin_dest || "",
      gate:        r.gate,
      zone:        r.zone_current,

      wchr:        String(r.wchr ?? ""),
      wchc:        String(r.wchc ?? ""),
      assignment:  r.assignment || "",
      pax:         String(r.pax_assisted ?? ""),
      watchlist:   r.watchlist || "",

      alert:       r.alert_text || "",
      gateChanged: isTrue(r.gate_changed),
      zoneChanged: isTrue(r.zone_changed),
      timeChanged: isTrue(r.time_changed),

      zoneFrom:    r.zone_chg_from || "",
      zoneTo:      r.zone_chg_to   || "",
      timeDelta:   String(r.time_delta_min ?? ""),
    }));
  }

  out.sort((a, b) => new Date(a.timeEst).getTime() - new Date(b.timeEst).getTime());
  return json({ ok: true, rows: out, generatedAt: new Date().toISOString() });
}

// ── Lead update ───────────────────────────────────────────────

async function handleLeadUpdate(req, env, user) {
  const body = await req.json().catch(() => ({}));
  const key  = String(body.key || "");
  if (!key) throw new Error("Missing key");

  const fields = [];
  const vals   = [];
  const patch  = {};

  if (body.assignment !== undefined) {
    fields.push("assignment=?", "assign_edited_by=?", "assign_edited_at=?");
    vals.push(body.assignment, user.username || "", new Date().toISOString());
    patch.assignment = String(body.assignment ?? "");
  }

  if (body.pax !== undefined) {
    fields.push("pax_assisted=?");
    vals.push(body.pax);
    patch.pax = String(body.pax ?? "");
  }

  if (body.watchlist !== undefined) {
    const wVal = (body.watchlist === true || body.watchlist === "true" || body.watchlist === 1) ? "1" : "";
    fields.push("watchlist=?");
    vals.push(wVal);
    patch.watchlist = wVal;
  }

  if (!fields.length) return json({ ok: true });

  fields.push("updated_at=?"); vals.push(new Date().toISOString());
  vals.push(key);

  await env.DB.prepare(
    `UPDATE flights SET ${fields.join(",")} WHERE key=?`
  ).bind(...vals).run();

  if (Object.keys(patch).length) setPatch(key, patch);
  return json({ ok: true });
}

// ── Lead ACK ──────────────────────────────────────────────────

async function handleLeadAck(req, env) {
  const body     = await req.json().catch(() => ({}));
  const key      = String(body.key || "");
  const zoneRaw  = String(body.zone || "");
  if (!key) throw new Error("Missing key");

  const zone  = normalizeZone(zoneRaw);
  const board = ZONE_TO_BOARD[zone];
  if (!board) throw new Error(`Unknown zone for ACK: ${zoneRaw}`);

  const ackCol = BOARD_ACK_COL[board];
  const nowIso = new Date().toISOString();

  // Fetch the row to check ZonePrev carry-over
  const { results } = await env.DB.prepare(
    "SELECT zone_prev, zone_current FROM flights WHERE key=? LIMIT 1"
  ).bind(key).all();
  const row = results[0];

  // If zone_prev matches the ACKing zone and flight has moved away,
  // clear zone_prev (mirrors GAS ackFlight / setBoardAckByKey_)
  const clearZonePrev =
    row &&
    normalizeZone(row.zone_prev) === zone &&
    normalizeZone(row.zone_current) !== zone;

  const sql = clearZonePrev
    ? `UPDATE flights SET ${ackCol}=1, zone_prev='', updated_at=? WHERE key=?`
    : `UPDATE flights SET ${ackCol}=1, updated_at=? WHERE key=?`;

  await env.DB.prepare(sql).bind(nowIso, key).run();

  setPatch(key, { alert: "", gateChanged: false, timeChanged: false, zoneChanged: false });
  return json({ ok: true });
}

// ── D1 query helper ───────────────────────────────────────────

async function getFlightsInWindow(env, startISO, endISO) {
  const { results } = await env.DB.prepare(
    "SELECT * FROM flights WHERE time_est >= ? AND time_est <= ? ORDER BY time_est ASC"
  ).bind(startISO, endISO).all();
  return results;
}

// ─────────────────────────────────────────────────────────────
// § 12  SCHEDULED HANDLER  (Cron Triggers)
// ─────────────────────────────────────────────────────────────

async function handleScheduled(event, env) {
  const cron = event.cron || "";

  // Nightly archive at 03:30 Toronto time (07:30 UTC in EDT, 08:30 UTC in EST)
  if (cron === "30 7 * * *" || cron === "30 8 * * *") {
    try { await nightlyArchive(env); }
    catch (err) { console.error("[archive] error:", err?.message || err); }
    return;
  }

  // Every-minute FIDS sync (all other cron expressions)
  try {
    const { arrivals, departures } = await fetchFIDSData(env);
    const result = await syncFIDSToD1(env, arrivals, departures);
    console.log(`[cron] sync complete:`, JSON.stringify(result));
  } catch (err) {
    console.error("[cron] sync error:", err?.message || err);
  }
}

// ─────────────────────────────────────────────────────────────
// § 13  FETCH HANDLER  (HTTP router)
// ─────────────────────────────────────────────────────────────

export default {
  async fetch(req, env, ctx) {
    const origin = req.headers.get("origin") || "*";

    if (req.method === "OPTIONS")
      return withCors(new Response(null, { status: 204 }), origin);

    try {
      const url  = new URL(req.url);
      const path = url.pathname.replace(/\/+$/, "") || "/";

      // ── Health ─────────────────────────────────────────────
      if (path === "/" || path === "/health") {
        return withCors(json({ ok: true, name: "prm-d1-worker", time: new Date().toISOString() }), origin);
      }

      // ── Auth ───────────────────────────────────────────────
      if (path === "/auth/login" && req.method === "POST")
        return withCors(await handleLogin(req, env), origin);

      if (path === "/auth/validate" && req.method === "GET")
        return withCors(await handleValidate(req, env), origin);

      // ── Dispatch ───────────────────────────────────────────
      if (path === "/dispatch/rows" && req.method === "GET") {
        await requireAuth(req, env, "dispatch");
        return withCors(await handleDispatchRows(req, env), origin);
      }

      if (path === "/dispatch/update" && req.method === "PATCH") {
        await requireAuth(req, env, "dispatch");
        return withCors(await handleDispatchUpdate(req, env), origin);
      }

      if (path === "/dispatch/ack" && req.method === "POST") {
        await requireAuth(req, env, "dispatch");
        return withCors(await handleDispatchAck(req, env), origin);
      }

      // ── Lead ───────────────────────────────────────────────
      if (path === "/lead/init" && req.method === "GET") {
        await requireAuth(req, env, "lead");
        return withCors(handleLeadInit(), origin);
      }

      if (path === "/lead/rows" && req.method === "GET") {
        await requireAuth(req, env, "lead");
        return withCors(await handleLeadRows(req, env), origin);
      }

      if (path === "/lead/update" && req.method === "PATCH") {
        const v = await requireAuth(req, env, "lead");
        return withCors(await handleLeadUpdate(req, env, v.user), origin);
      }

      if (path === "/lead/ack" && req.method === "POST") {
        await requireAuth(req, env, "lead");
        return withCors(await handleLeadAck(req, env), origin);
      }

      // ── Archive (Mgmt only) ────────────────────────────────
      if (path === "/archive/dates" && req.method === "GET") {
        await requireAuth(req, env, "mgmt");
        return withCors(await handleArchiveDates(req, env), origin);
      }

      if (path === "/archive/rows" && req.method === "GET") {
        await requireAuth(req, env, "mgmt");
        return withCors(await handleArchiveRows(req, env), origin);
      }

      // ── Admin: manual sync trigger (for testing) ───────────
      if (path === "/admin/sync" && req.method === "POST") {
        await requireAuth(req, env, "dispatch");
        const { arrivals, departures } = await fetchFIDSData(env);
        const result = await syncFIDSToD1(env, arrivals, departures);
        return withCors(json({ ok: true, ...result }), origin);
      }

      return withCors(json({ ok: false, error: "Not found" }, { status: 404 }), origin);

    } catch (err) {
      const msg = err?.message || String(err);
      const is401 = /missing authorization|unauthorized|expired|no access|invalid token/i.test(msg);
      return withCors(
        json({ ok: false, error: msg }, { status: is401 ? 401 : 500 }),
        origin
      );
    }
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(handleScheduled(event, env));
  },
};
