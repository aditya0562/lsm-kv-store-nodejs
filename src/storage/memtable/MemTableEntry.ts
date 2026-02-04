/**
 * MemTable entry type.
 * 
 * Design: Each entry tracks value, timestamp, and deletion status.
 * Timestamp enables future time-based queries and conflict resolution.
 */

export interface MemTableEntry {
  value: string;
  timestamp: number;
  deleted: boolean;
}
