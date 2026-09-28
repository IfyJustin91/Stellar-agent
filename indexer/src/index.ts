import { rpc as StellarRpc } from "@stellar/stellar-sdk";
import { openDatabase, EventStore, type QueryOptions, type ParsedEventRow } from "./db.js";
import { SorobanEventPoller, type PollerOptions } from "./poller.js";

export type { QueryOptions, ParsedEventRow };

export type BearEventIndexerConfig = {
  /** Soroban RPC URL */
  rpcUrl: string;
  /** Identity contract address */
  identityContract: string;
  /** Commerce contract address */
  commerceContract: string;
  /** Path to the SQLite database file. Default: "./data/events.db" */
  dbPath?: string;
  /** Ledger to start indexing from on first run. Default: 0 (chain origin) */
  startLedger?: number;
  /** Polling options */
  polling?: PollerOptions;
};

/**
 * High-level facade for the Bear Protocol event indexer.
 *
 * @example
 * ```typescript
 * import { BearEventIndexer } from "bear-event-indexer";
 *
 * const indexer = new BearEventIndexer({
 *   rpcUrl: "https://soroban-testnet.stellar.org",
 *   identityContract: "CAMPXYFZJTIPEVOPOAZPRG5OHXKNBDPGTPRCOIO4LVPGEM4TONPY65A5",
 *   commerceContract: "CD2KWU7IE74Z2QKVP3FQ67J46XHNMGIDTNKXVWE7ZNVRC7T6UH46GQXE",
 *   dbPath: "./data/events.db",
 *   startLedger: 1_000_000,
 * });
 *
 * await indexer.start();
 * const jobs = await indexer.getJobsByProvider(providerAddress, { limit: 20 });
 * ```
 */
export class BearEventIndexer {
  private readonly store: EventStore;
  private readonly poller: SorobanEventPoller;

  constructor(config: BearEventIndexerConfig) {
    const dbPath = config.dbPath ?? "./data/events.db";
    const startLedger = config.startLedger ?? 0;

    const db = openDatabase(dbPath);
    this.store = new EventStore(db);

    const rpcServer = new StellarRpc.Server(config.rpcUrl, {
      allowHttp: config.rpcUrl.startsWith("http://"),
    });

    const contracts = [config.identityContract, config.commerceContract].filter(
      (c) => c && c.trim() !== "",
    );

    this.poller = new SorobanEventPoller(rpcServer, this.store, contracts, startLedger, {
      ...config.polling,
    });
  }

  /** Start continuous indexing. Resolves once the first poll completes. */
  async start(): Promise<void> {
    await this.poller.start();
  }

  /** Stop the indexer gracefully. */
  stop(): void {
    this.poller.stop();
  }

  // ── Query API ──────────────────────────────────────────────────────────────

  /**
   * Retrieve events where the `provider` field in the payload matches the
   * given address. Matches JobCreated, JobCompleted, etc.
   */
  getJobsByProvider(provider: string, opts?: QueryOptions): ParsedEventRow[] {
    return this.store.getByProvider(provider, opts);
  }

  /**
   * Retrieve events where the `client` field in the payload matches the
   * given address.
   */
  getJobsByClient(client: string, opts?: QueryOptions): ParsedEventRow[] {
    return this.store.getByClient(client, opts);
  }

  /**
   * Retrieve events filtered by a specific event type, e.g. "JobCreated".
   */
  getEventsByType(eventType: string, opts?: QueryOptions): ParsedEventRow[] {
    return this.store.getByEventType(eventType, opts);
  }

  /**
   * Retrieve all events within a Unix timestamp range (seconds, inclusive).
   */
  getEventsByDateRange(from: number, to: number, opts?: QueryOptions): ParsedEventRow[] {
    return this.store.getByDateRange(from, to, opts);
  }

  /** Returns the last indexed ledger sequence number, or null if none yet. */
  getLastIndexedLedger(): number | null {
    return this.store.getLastIndexedLedger();
  }

  /** Returns the total number of indexed events. */
  totalEvents(): number {
    return this.store.count();
  }
}

// Re-export lower-level primitives for advanced use-cases
export { openDatabase, EventStore } from "./db.js";
export { SorobanEventPoller } from "./poller.js";
