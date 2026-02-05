import { LogEntry, LogOperationType } from '../storage/wal/LogEntry';
import { MemTableEntry } from '../storage/memtable/MemTableEntry';

export interface IWAL {
  open(): Promise<void>;
  append(entry: Omit<LogEntry, 'sequenceId' | 'timestamp'>): Promise<void>;
  replay(): Promise<LogEntry[]>;
  checkpoint(): Promise<void>;
  close(): Promise<void>;
}

export interface IMemTable {
  put(key: string, value: string, deleted?: boolean): void;
  get(key: string): string | null;
  getEntry(key: string): MemTableEntry | null;
  delete(key: string): void;
  isFull(): boolean;
  size(): number;
  getAllSorted(): Array<{ key: string; entry: MemTableEntry }>;
  getRange(startKey: string, endKey: string): Array<{ key: string; entry: MemTableEntry }>;
  clear(): void;
}

export interface KVPair {
  readonly key: string;
  readonly value: string;
}

export interface RangeQueryOptions {
  limit?: number;
}

export interface IStorageEngine {
  initialize(): Promise<void>;
  put(key: string, value: string): Promise<void>;
  get(key: string): Promise<string | null>;
  delete(key: string): Promise<void>;
  
  /**
   * Put multiple key-value pairs in a single atomic operation.
   * More efficient than multiple put() calls due to single WAL write.
   * 
   * @param entries - Array of key-value pairs
   * @returns Number of entries written
   */
  batchPut(entries: KVPair[]): Promise<number>;
  
  /**
   * Read a range of keys [startKey, endKey] in sorted order.
   * Both bounds are inclusive.
   * 
   * @param startKey - Start of range (inclusive)
   * @param endKey - End of range (inclusive)
   * @param options - Optional query parameters
   * @returns Async iterable of key-value pairs
   */
  readKeyRange(
    startKey: string, 
    endKey: string, 
    options?: RangeQueryOptions
  ): AsyncIterable<KVPair>;
  
  close(): Promise<void>;
}

export interface IStorageFactory {
  createWAL(): IWAL;
  createMemTable(): IMemTable;
}
