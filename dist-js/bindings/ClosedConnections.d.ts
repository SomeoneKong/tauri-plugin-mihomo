import type { ClosedConnection } from "./ClosedConnection";
/**
 * A page of the connections the core closed recently, from `GET /connections/closed`.
 *
 * Records are in the order of their sequence numbers within one `instance`. Traffic counted
 * after a close gives the record a new sequence number, so a reader following them reads it
 * again and replaces what it had by id. Every connection closed after `evicted_until`,
 * within the time the core keeps them, is kept; it is unset when none was dropped for room.
 * `truncated` is set when records after the one read last were dropped.
 */
export type ClosedConnections = {
    instance: string;
    startedAt: string;
    oldest: number;
    latest: number;
    evictedUntil: string | null;
    truncated: boolean;
    records: Array<ClosedConnection>;
};
