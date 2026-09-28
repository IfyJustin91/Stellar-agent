import Database from "better-sqlite3";
import path from "node:path";
import fs from "node:fs";

/**
 * Initialises (or opens) the SQLite database and ensures the schema and
 * indexes are present.  Safe to call on every startup — uses IF NOT EXISTS.
 */
export function openDatabase(dbPath: string): Database.Database {
  const dir = path.dirname(dbPath);
  fs.mkdirSync(dir, { recursive: true });

  const db = new Database(dbPath);

  // WAL mode for better concurrent read performance
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");

  db.exec(`
    CREATE TABLE IF NOT EXISTS events (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      ledger      INTEGER NOT NULL,
      timestamp   INTEGER NOT NULL,
      contract    TEXT    NOT NULL,
      event_type  TEXT    NOT NULL,
      payload     TEXT    NOT NULL   -- JSON blob
    );

    CREATE INDEX IF NOT EXISTS idx_events_ledger
      ON events(ledger);

    CREATE INDEX IF NOT EXISTS idx_events_type
      ON events(event_type);

    CREATE INDEX IF NOT EXISTS idx_events_contract
      ON events(contract);

    CREATE INDEX IF NOT EXISTS idx_events_timestamp
      ON events(timestamp);

    -- Tracks the last successfully indexed ledger so we can resume on restart
    CREATE TABLE IF NOT EXISTS indexer_state (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );
  `);

  return db;
}

// ── Prepared statement helpers ───────────────────────────────────────────────

export type EventRow = {
  id: number;
  ledger: number;
  timestamp: number;
  contract: string;
  event_type: string;
  payload: string;
};

export type ParsedEventRow = Omit<EventRow, "payload"> & { payload: Record<string, unknown> };

function parseRow(row: EventRow): ParsedEventRow {
  return { ...row, payload: JSON.parse(row.payload) as Record<string, unknown> };
}

export type InsertEventParams = {
  ledger: number;
  timestamp: number;
  contract: string;
  event_type: string;
  payload: Record<string, unknown>;
};

export type QueryOptions = {
  limit?: number;
  offset?: number;
  fromTimestamp?: number;
  toTimestamp?: number;
};

export class EventStore {
  private db: Database.Database;

  private stmtInsert: Database.Statement;
  private stmtGetState: Database.Statement;
  private stmtSetState: Database.Statement;

  constructor(db: Database.Database) {
    this.db = db;

    this.stmtInsert = db.prepare(`
      INSERT INTO events (ledger, timestamp, contract, event_type, payload)
      VALUES (@ledger, @timestamp, @contract, @event_type, @payload)
    `);

    this.stmtGetState = db.prepare(
      `SELECT value FROM indexer_state WHERE key = ?`,
    );

    this.stmtSetState = db.prepare(`
      INSERT INTO indexer_state (key, value) VALUES (?, ?)
      ON CONFLICT(key) DO UPDATE SET value = excluded.value
    `);
  }

  /** Bulk-insert events inside a single transaction. */
  insertMany(events: InsertEventParams[]): void {
    const insert = this.db.transaction((rows: InsertEventParams[]) => {
      for (const row of rows) {
        this.stmtInsert.run({
          ledger: row.ledger,
          timestamp: row.timestamp,
          contract: row.contract,
          event_type: row.event_type,
          payload: JSON.stringify(row.payload),
        });
      }
    });
    insert(events);
  }

  getLastIndexedLedger(): number | null {
    const row = this.stmtGetState.get("last_ledger") as
      | { value: string }
      | undefined;
    return row ? Number(row.value) : null;
  }

  setLastIndexedLedger(ledger: number): void {
    this.stmtSetState.run("last_ledger", String(ledger));
  }

  // ── Query API ──────────────────────────────────────────────────────────────

  getByProvider(
    provider: string,
    opts: QueryOptions = {},
  ): ParsedEventRow[] {
    const { limit = 20, offset = 0, fromTimestamp, toTimestamp } = opts;
    let sql = `
      SELECT * FROM events
      WHERE json_extract(payload, '$.provider') = ?
    `;
    const params: (string | number)[] = [provider];
    if (fromTimestamp !== undefined) {
      sql += " AND timestamp >= ?";
      params.push(fromTimestamp);
    }
    if (toTimestamp !== undefined) {
      sql += " AND timestamp <= ?";
      params.push(toTimestamp);
    }
    sql += " ORDER BY ledger DESC LIMIT ? OFFSET ?";
    params.push(limit, offset);

    const rows = this.db.prepare(sql).all(...params) as EventRow[];
    return rows.map(parseRow);
  }

  getByClient(
    client: string,
    opts: QueryOptions = {},
  ): ParsedEventRow[] {
    const { limit = 20, offset = 0, fromTimestamp, toTimestamp } = opts;
    let sql = `
      SELECT * FROM events
      WHERE json_extract(payload, '$.client') = ?
    `;
    const params: (string | number)[] = [client];
    if (fromTimestamp !== undefined) {
      sql += " AND timestamp >= ?";
      params.push(fromTimestamp);
    }
    if (toTimestamp !== undefined) {
      sql += " AND timestamp <= ?";
      params.push(toTimestamp);
    }
    sql += " ORDER BY ledger DESC LIMIT ? OFFSET ?";
    params.push(limit, offset);

    const rows = this.db.prepare(sql).all(...params) as EventRow[];
    return rows.map(parseRow);
  }

  getByEventType(
    eventType: string,
    opts: QueryOptions = {},
  ): ParsedEventRow[] {
    const { limit = 20, offset = 0, fromTimestamp, toTimestamp } = opts;
    let sql = `SELECT * FROM events WHERE event_type = ?`;
    const params: (string | number)[] = [eventType];
    if (fromTimestamp !== undefined) {
      sql += " AND timestamp >= ?";
      params.push(fromTimestamp);
    }
    if (toTimestamp !== undefined) {
      sql += " AND timestamp <= ?";
      params.push(toTimestamp);
    }
    sql += " ORDER BY ledger DESC LIMIT ? OFFSET ?";
    params.push(limit, offset);

    const rows = this.db.prepare(sql).all(...params) as EventRow[];
    return rows.map(parseRow);
  }

  getByDateRange(
    from: number,
    to: number,
    opts: QueryOptions = {},
  ): ParsedEventRow[] {
    const { limit = 50, offset = 0 } = opts;
    const rows = this.db
      .prepare(
        `SELECT * FROM events
         WHERE timestamp BETWEEN ? AND ?
         ORDER BY ledger ASC
         LIMIT ? OFFSET ?`,
      )
      .all(from, to, limit, offset) as EventRow[];
    return rows.map(parseRow);
  }

  /** Total event count — useful for health/status checks. */
  count(): number {
    const row = this.db
      .prepare("SELECT COUNT(*) as c FROM events")
      .get() as { c: number };
    return row.c;
  }
}
