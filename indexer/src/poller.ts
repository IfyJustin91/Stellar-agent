import { rpc as StellarRpc } from "@stellar/stellar-sdk";
import { EventStore, type InsertEventParams } from "./db.js";

export type SorobanRpcClient = {
  getEvents(
    params: StellarRpc.Server.GetEventsRequest,
  ): Promise<StellarRpc.Api.GetEventsResponse>;
  getLatestLedger(): Promise<StellarRpc.Api.GetLatestLedgerResponse>;
};

// Known Bear Protocol event topic names
export const BEAR_EVENT_TYPES = [
  "JobCreated",
  "JobSubmitted",
  "JobCompleted",
  "JobCancelled",
  "JobDisputed",
  "JobExpired",
  "JobRefunded",
  "Registered",
  "AgentDeregistered",
  "UriUpdated",
  "OwnerTransferred",
] as const;

export type BearEventType = (typeof BEAR_EVENT_TYPES)[number];

/**
 * Converts a raw Soroban event into the InsertEventParams format.
 * The full event is stored as a JSON blob in `payload` so no information
 * is lost and new fields can be queried without schema migrations.
 */
function toInsertParams(
  event: StellarRpc.Api.EventResponse,
  ledgerCloseTime: number,
): InsertEventParams {
  // The first topic element is typically the event name / discriminant
  const topics = event.topic ?? [];
  const firstTopic = topics[0];
  let eventType = "Unknown";
  try {
    // Topic values come back as xdr strings; attempt a best-effort decode
    if (firstTopic && typeof firstTopic === "object" && "value" in firstTopic) {
      eventType = String((firstTopic as Record<string, unknown>).value);
    } else if (typeof firstTopic === "string") {
      eventType = firstTopic;
    }
  } catch {
    // leave as "Unknown"
  }

  const payload: Record<string, unknown> = {
    topics: topics,
    value: event.value,
    txHash: event.txHash,
    id: event.id,
  };

  return {
    ledger: event.ledger,
    timestamp: ledgerCloseTime,
    contract: event.contractId ?? "",
    event_type: eventType,
    payload,
  };
}

export type PollerOptions = {
  /** Polling interval in milliseconds. Default: 5000 */
  intervalMs?: number;
  /** Max events to fetch per RPC call. Default: 100 */
  batchSize?: number;
  /** Emit debug logs. Default: false */
  debug?: boolean;
};

/**
 * Continuously polls Soroban RPC for new contract events and persists them
 * to SQLite.  Call `start()` to begin and `stop()` to halt gracefully.
 */
export class SorobanEventPoller {
  private readonly rpc: SorobanRpcClient;
  private readonly store: EventStore;
  private readonly contracts: string[];
  private readonly startLedger: number;
  private readonly intervalMs: number;
  private readonly batchSize: number;
  private readonly debug: boolean;

  private timer: ReturnType<typeof setTimeout> | null = null;
  private running = false;

  constructor(
    rpc: SorobanRpcClient,
    store: EventStore,
    contracts: string[],
    startLedger: number,
    opts: PollerOptions = {},
  ) {
    this.rpc = rpc;
    this.store = store;
    this.contracts = contracts;
    this.startLedger = startLedger;
    this.intervalMs = opts.intervalMs ?? 5_000;
    this.batchSize = opts.batchSize ?? 100;
    this.debug = opts.debug ?? false;
  }

  private log(msg: string): void {
    if (this.debug) console.log(`[indexer] ${msg}`);
  }

  /** Returns the ledger to start polling from (resume or initial). */
  private getFromLedger(): number {
    const last = this.store.getLastIndexedLedger();
    // Start one ledger ahead of the last processed to avoid re-indexing
    return last !== null ? last + 1 : this.startLedger;
  }

  private async poll(): Promise<void> {
    const fromLedger = this.getFromLedger();

    try {
      const latest = await this.rpc.getLatestLedger();
      if (fromLedger > latest.sequence) {
        this.log(`Up to date at ledger ${latest.sequence}`);
        return;
      }

      // Query each contract separately to stay within RPC limits
      for (const contractId of this.contracts) {
        try {
          const response = await this.rpc.getEvents({
            startLedger: fromLedger,
            filters: [
              {
                type: "contract",
                contractIds: [contractId],
              },
            ],
            limit: this.batchSize,
          });

          if (!response.events || response.events.length === 0) continue;

          // Soroban getEvents returns ledger close times via the `ledgerClosedAt` field;
          // fall back to current time when unavailable (e.g. in tests).
          const inserts: InsertEventParams[] = response.events.map((ev) => {
            const closeTime =
              typeof (ev as Record<string, unknown>).ledgerClosedAt === "string"
                ? Math.floor(
                    new Date(
                      (ev as Record<string, unknown>).ledgerClosedAt as string,
                    ).getTime() / 1000,
                  )
                : Math.floor(Date.now() / 1000);
            return toInsertParams(ev as StellarRpc.Api.EventResponse, closeTime);
          });

          this.store.insertMany(inserts);
          this.log(
            `Indexed ${inserts.length} events from contract ${contractId} (ledgers ${fromLedger}+)`,
          );
        } catch (err) {
          console.error(
            `[indexer] Failed to fetch events for contract ${contractId}:`,
            (err as Error).message,
          );
        }
      }

      // Advance the watermark to the latest ledger we queried up to
      this.store.setLastIndexedLedger(latest.sequence);
    } catch (err) {
      console.error("[indexer] Poll error:", (err as Error).message);
    }
  }

  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;
    this.log("Starting event poller…");

    const tick = async () => {
      if (!this.running) return;
      await this.poll();
      if (this.running) {
        this.timer = setTimeout(tick, this.intervalMs);
      }
    };

    await tick();
  }

  stop(): void {
    this.running = false;
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.log("Poller stopped.");
  }
}
