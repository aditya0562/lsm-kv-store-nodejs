import { SSTableEntry, SSTableMetadata } from './SSTableTypes';

export interface SSTableIteratorResult {
  readonly key: string;
  readonly value: string;
  readonly timestamp: number;
  readonly deleted: boolean;
}

/**
 * Interface for reading SSTable files.
 * 
 * Lifecycle:
 * 1. Create reader with file path
 * 2. Call open() to load index
 * 3. Use get() or iterate() for queries
 * 4. Call close() when done
 */
export interface ISSTableReader {
  
  open(): Promise<void>;

  close(): Promise<void>;

  isOpen(): boolean;

  getMetadata(): SSTableMetadata;

  get(key: string): Promise<SSTableEntry | null>;

  mayContain(key: string): boolean;

  isInRange(key: string): boolean;

  /**
   * Async iterator for range queries.
   * Returns entries in sorted order within the range.
   * 
   * @param startKey - Start of range (inclusive), empty string for beginning
   * @param endKey - End of range (inclusive), empty string for end
   * @yields SSTableEntry for each entry in range
   */
  iterate(startKey?: string, endKey?: string): AsyncIterable<SSTableEntry>;
}
