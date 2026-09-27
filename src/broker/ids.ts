import { randomBytes } from "node:crypto";

const ID_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

function randomSuffix(length: number): string {
  const bytes = randomBytes(length);
  let out = "";
  for (let i = 0; i < bytes.length; i += 1) {
    const byte = bytes[i];
    if (byte === undefined) continue;
    out += ID_ALPHABET[byte % ID_ALPHABET.length];
  }
  return out;
}

/** Default consumer id when the caller does not name its consumer. */
export function generateConsumerId(): string {
  return `cons_${randomSuffix(10)}`;
}
