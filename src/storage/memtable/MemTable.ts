/**
 * MemTable - In-memory sorted key-value store
 * 
 * Design Pattern: Implements IMemTable interface for dependency injection.
 * Uses Red-Black tree (SortedMap) for O(log n) operations and sorted iteration.
 * 
 * For Java developers: This is similar to a ConcurrentSkipListMap or TreeMap
 * backed in-memory buffer. The sorted structure enables efficient range queries
 * and ordered SSTable flushing.
 * 
 * Tombstone Pattern: Deletes create entries with deleted=true. This allows
 * deletes to propagate through SSTable layers during compaction.
 */

import { MemTableEntry } from './MemTableEntry';
import { SortedMap } from './SortedMap';
import { IMemTable } from '../../interfaces/Storage';

export class MemTable implements IMemTable {
  private data: SortedMap<MemTableEntry>;
  private currentSize: number = 0;
  private readonly sizeLimit: number;
  
  constructor(sizeLimit: number) {
    this.sizeLimit = sizeLimit;
    this.data = new SortedMap<MemTableEntry>();
  }
  
  /**
   * Put key-value pair into MemTable
   * Time complexity: O(log n) due to Red-Black tree
   */
  put(key: string, value: string, deleted: boolean = false): void {
    // Remove old entry size if exists
    const existingEntry = this.data.get(key);
    if (existingEntry) {
      this.currentSize -= this.calculateEntrySize(key, existingEntry);
    }
    
    const entry: MemTableEntry = {
      value,
      timestamp: Date.now(),
      deleted,
    };
    
    this.data.set(key, entry);
    this.currentSize += this.calculateEntrySize(key, entry);
  }
  
  /**
   * Get value for key
   * Time complexity: O(log n)
   * 
   * Returns null if key not found or deleted (tombstone).
   */
  get(key: string): string | null {
    const entry = this.data.get(key);
    if (!entry || entry.deleted) {
      return null;
    }
    return entry.value;
  }
  
  /**
   * Delete key (tombstone)
   * Time complexity: O(log n)
   */
  delete(key: string): void {
    const existingEntry = this.data.get(key);
    if (existingEntry) {
      this.currentSize -= this.calculateEntrySize(key, existingEntry);
    }
    this.put(key, '', true);
  }
  
  /**
   * Check if MemTable has reached size limit
   */
  isFull(): boolean {
    return this.currentSize >= this.sizeLimit;
  }
  
  /**
   * Get current size in bytes
   */
  size(): number {
    return this.currentSize;
  }
  
  /**
   * Get all entries in sorted order
   * Time complexity: O(n) - in-order traversal
   * 
   * Critical for LSM tree: Returns entries sorted by key for:
   * 1. SSTable flushing (must be sorted)
   * 2. Range queries (already sorted, can merge efficiently)
   */
  getAllSorted(): Array<{ key: string; entry: MemTableEntry }> {
    return this.data.entries().map(([key, entry]) => ({ key, entry }));
  }
  
  /**
   * Get entries in key range [startKey, endKey]
   * Time complexity: O(log n + k) where k is number of results
   */
  getRange(startKey: string, endKey: string): Array<{ key: string; entry: MemTableEntry }> {
    return this.data.range(startKey, endKey).map(([key, entry]) => ({ key, entry }));
  }
  
  /**
   * Clear all entries
   */
  clear(): void {
    this.data.clear();
    this.currentSize = 0;
  }
  
  /**
   * Calculate size of entry in bytes
   */
  private calculateEntrySize(key: string, entry: MemTableEntry): number {
    const keySize = Buffer.byteLength(key, 'utf8');
    const valueSize = Buffer.byteLength(entry.value, 'utf8');
    // Metadata: timestamp (8 bytes) + deleted flag (1 byte) + tree node overhead (~40 bytes)
    const metadataSize = 8 + 1 + 40;
    return keySize + valueSize + metadataSize;
  }
}
