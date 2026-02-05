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
  
  put(key: string, value: string, deleted: boolean = false): void {
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
  
  get(key: string): string | null {
    const entry = this.data.get(key);
    if (!entry || entry.deleted) {
      return null;
    }
    return entry.value;
  }
  
  getEntry(key: string): MemTableEntry | null {
    return this.data.get(key) ?? null;
  }
 
  delete(key: string): void {
    const existingEntry = this.data.get(key);
    if (existingEntry) {
      this.currentSize -= this.calculateEntrySize(key, existingEntry);
    }
    this.put(key, '', true);
  }
  
  isFull(): boolean {
    return this.currentSize >= this.sizeLimit;
  }
  
  size(): number {
    return this.currentSize;
  }
  
  getAllSorted(): Array<{ key: string; entry: MemTableEntry }> {
    return this.data.entries().map(([key, entry]) => ({ key, entry }));
  }
  
  getRange(startKey: string, endKey: string): Array<{ key: string; entry: MemTableEntry }> {
    return this.data.range(startKey, endKey).map(([key, entry]) => ({ key, entry }));
  }
  
 
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
    const metadataSize = 8 + 1 + 40;
    return keySize + valueSize + metadataSize;
  }
}
