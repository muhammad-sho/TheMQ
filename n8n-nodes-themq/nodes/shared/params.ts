import type { IDataObject, NodeParameterValueType } from "n8n-workflow";

/** Narrow an API value to n8n's item-data type. */
export function toDataObject(value: unknown): IDataObject {
  if (typeof value === "object" && value !== null) return value as IDataObject;
  return {};
}

/** Narrow node parameters explicitly. */
export function strParam(value: NodeParameterValueType | object, fallback = ""): string {
  return typeof value === "string" ? value : fallback;
}

export function numParam(value: NodeParameterValueType | object, fallback: number): number {
  return typeof value === "number" ? value : fallback;
}

export function boolParam(value: NodeParameterValueType | object, fallback = false): boolean {
  return typeof value === "boolean" ? value : fallback;
}
