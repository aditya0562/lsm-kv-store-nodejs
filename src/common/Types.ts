/**
 * Common type definitions for the KV store.
 * These types are used across the storage engine and API layers.
 */

export interface KVPair {
  key: string;
  value: string;
}

export interface EntryMetadata {
  timestamp: number;
  deleted: boolean;
}

export interface Entry extends KVPair, EntryMetadata {}
