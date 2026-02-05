/**
 * SSTable Writer Implementation
 * Write Process:
 * 1. Accumulate entries in memory (sorted order required)
 * 2. Write to temp file: sstable-XXXXX.sst.tmp
 * 3. Build sparse index while writing
 * 4. Write index section
 * 5. Write footer with metadata
 * 6. fsync for durability
 * 7. Atomic rename: .tmp → .sst
 */

import * as fs from 'fs/promises';
import * as path from 'path';
import { 
  SSTableEntry, 
  SSTableMetadata, 
  IndexEntry, 
  SSTableConfig,
  DEFAULT_SSTABLE_CONFIG 
} from './SSTableTypes';
import { ISSTableWriter } from './ISSTableWriter';
import { SSTableSerializer } from './SSTableSerializer';

export class SSTableWriter implements ISSTableWriter {
  private readonly fileNumber: number;
  private readonly config: SSTableConfig;
  private readonly entries: SSTableEntry[] = [];
  private readonly sparseIndex: IndexEntry[] = [];
  
  private lastKey: string | null = null;
  private tempFilePath: string | null = null;
  private built: boolean = false;

  constructor(fileNumber: number, config: Partial<SSTableConfig> = {}) {
    this.fileNumber = fileNumber;
    this.config = { ...DEFAULT_SSTABLE_CONFIG, ...config };
  }

  public add(entry: SSTableEntry): void {
    if (this.built) {
      throw new Error('SSTableWriter: Cannot add entries after build()');
    }

    if (this.lastKey !== null && entry.key <= this.lastKey) {
      throw new Error(
        `SSTableWriter: Keys must be in sorted order. ` +
        `Got "${entry.key}" after "${this.lastKey}"`
      );
    }

    this.entries.push(entry);
    this.lastKey = entry.key;
  }

  public getEntryCount(): number {
    return this.entries.length;
  }

  public async build(): Promise<SSTableMetadata> {
    if (this.built) {
      throw new Error('SSTableWriter: build() already called');
    }

    if (this.entries.length === 0) {
      throw new Error('SSTableWriter: Cannot build empty SSTable');
    }

    this.built = true;

    await fs.mkdir(this.config.dataDir, { recursive: true });

    const fileName = this.formatFileName(this.fileNumber);
    const finalPath = path.join(this.config.dataDir, fileName);
    this.tempFilePath = finalPath + '.tmp';

    const handle = await fs.open(this.tempFilePath, 'w');

    try {
      let currentOffset = 0;
      const dataOffset = 0;

      for (let i = 0; i < this.entries.length; i++) {
        const entry = this.entries[i]!;

        if (i % this.config.sparseIndexInterval === 0) {
          this.sparseIndex.push({ key: entry.key, offset: currentOffset });
        }

        const entryBuffer = SSTableSerializer.serializeEntry(entry);
        await handle.write(entryBuffer);
        currentOffset += entryBuffer.length;
      }

      const indexOffset = currentOffset;

      const indexCountBuffer = Buffer.allocUnsafe(4);
      indexCountBuffer.writeUInt32BE(this.sparseIndex.length, 0);
      await handle.write(indexCountBuffer);
      currentOffset += 4;

      for (const indexEntry of this.sparseIndex) {
        const indexBuffer = SSTableSerializer.serializeIndexEntry(indexEntry);
        await handle.write(indexBuffer);
        currentOffset += indexBuffer.length;
      }

      const firstKey = this.entries[0]!.key;
      const lastKey = this.entries[this.entries.length - 1]!.key;
      const createdAt = Date.now();

      const footerBuffer = SSTableSerializer.serializeFooter(
        this.fileNumber,
        this.entries.length,
        dataOffset,
        indexOffset,
        firstKey,
        lastKey,
        createdAt
      );
      await handle.write(footerBuffer);
      currentOffset += footerBuffer.length;

      await handle.sync();
      await handle.close();

      await fs.rename(this.tempFilePath, finalPath);
      this.tempFilePath = null;

      const metadata: SSTableMetadata = {
        fileNumber: this.fileNumber,
        filePath: finalPath,
        entryCount: this.entries.length,
        firstKey,
        lastKey,
        fileSize: currentOffset,
        createdAt,
        indexOffset,
        dataOffset,
      };

      return metadata;

    } catch (error) {
      await handle.close().catch(() => {});
      await this.cleanupTempFile();
      throw error;
    }
  }

  public async abort(): Promise<void> {
    await this.cleanupTempFile();
    this.built = true; // Prevent further use
  }

  private formatFileName(fileNumber: number): string {
    const paddedNumber = fileNumber.toString().padStart(5, '0');
    return `sstable-${paddedNumber}.sst`;
  }

  private async cleanupTempFile(): Promise<void> {
    if (this.tempFilePath) {
      try {
        await fs.unlink(this.tempFilePath);
      } catch {
      }
      this.tempFilePath = null;
    }
  }
}
