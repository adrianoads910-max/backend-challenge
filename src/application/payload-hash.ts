import { createHash } from "node:crypto";

/**
 * Canonical JSON: object keys sorted recursively (UTF-16 code unit order), no whitespace,
 * `undefined` members dropped, arrays kept in order. Matches RFC 8785 for the value types we
 * hash (strings and nested objects — amounts are already normalised decimal strings).
 */
export function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") {
    if (typeof value === "number" || typeof value === "bigint") {
      throw new TypeError("numbers are not allowed in hashed payloads");
    }
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, v]) => v !== undefined)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`).join(",")}}`;
}

/** SHA-256 hex of the canonical JSON. */
export function sha256Canonical(value: unknown): string {
  return createHash("sha256").update(canonicalJson(value), "utf8").digest("hex");
}
