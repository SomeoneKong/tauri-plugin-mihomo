import type { ConnectionMetaData } from "./ConnectionMetaData";
import type { JsonValue } from "./serde_json/JsonValue";
/**
 * A closed connection as the connections list shows it, with its sequence number and the
 * time it was closed.
 */
export type ClosedConnection = {
    seq: number;
    closedAt: string;
    id: string;
    metadata: ConnectionMetaData;
    upload: number;
    download: number;
    start: string;
    chains: Array<string>;
    providerChains: Array<string> | null;
    rule: string;
    rulePayload: string;
} & ({
    [key in string]: number | string | boolean | Array<JsonValue> | {
        [key in string]: JsonValue;
    } | null;
});
