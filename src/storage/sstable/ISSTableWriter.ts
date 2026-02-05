import { SSTableEntry, SSTableMetadata } from './SSTableTypes';

/**
 * Interface for writing SSTable files.
 * 
 * Usage pattern (Builder):
 * 1. Create writer
 * 2. Add entries (must be in sorted order)
 * 3. Call build() to write to disk
 * 4. Get metadata for manifest
 */
export interface ISSTableWriter {
  
  add(entry: SSTableEntry): void;

  build(): Promise<SSTableMetadata>;

  getEntryCount(): number;

  abort(): Promise<void>;
}
