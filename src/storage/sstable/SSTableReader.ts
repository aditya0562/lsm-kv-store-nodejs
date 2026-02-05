import * as fs from 'fs/promises';
import { FileHandle } from 'fs/promises';
import { 
  SSTableEntry, 
  SSTableMetadata, 
  IndexEntry,
  SSTABLE_MAGIC,
  SSTABLE_VERSION,
} from './SSTableTypes';
import { ISSTableReader } from './ISSTableReader';
import { SSTableSerializer } from './SSTableSerializer';

interface ReaderState {
  readonly handle: FileHandle;
  readonly fileSize: number;
  readonly metadata: SSTableMetadata;
  readonly sparseIndex: IndexEntry[];
}

export class SSTableReader implements ISSTableReader {
  private readonly filePath: string;
  private state: ReaderState | null = null;

  constructor(filePath: string) {
    this.filePath = filePath;
  }

  public static async openFile(filePath: string): Promise<SSTableReader> {
    const reader = new SSTableReader(filePath);
    await reader.open();
    return reader;
  }

  public async open(): Promise<void> {
    if (this.state !== null) {
      throw new Error('SSTableReader: Already open');
    }

    const handle = await fs.open(this.filePath, 'r');
    
    try {
      const stats = await handle.stat();
      const fileSize = stats.size;

      const metadata = await this.readFooter(handle, fileSize);
      
      const sparseIndex = await this.loadSparseIndex(handle, metadata.indexOffset, fileSize);

      this.state = {
        handle,
        fileSize,
        metadata,
        sparseIndex,
      };

    } catch (error) {
      await handle.close();
      throw error;
    }
  }

  public async close(): Promise<void> {
    if (this.state === null) {
      return;
    }

    await this.state.handle.close();
    this.state = null;
  }

  public isOpen(): boolean {
    return this.state !== null;
  }

  public getMetadata(): SSTableMetadata {
    this.ensureOpen();
    return this.state!.metadata;
  }

  /**
   * Point lookup using sparse index + sequential scan.
   * 
   * Algorithm:
   * 1. Binary search sparse index for largest key ≤ target
   * 2. Seek to that offset
   * 3. Sequential scan until found or past target
   */
  public async get(key: string): Promise<SSTableEntry | null> {
    this.ensureOpen();
    const { metadata, sparseIndex, handle } = this.state!;

    if (!this.isInRange(key)) {
      return null;
    }

    const startOffset = this.findStartOffset(key, sparseIndex, metadata.dataOffset);

    let offset = startOffset;
    const endOffset = metadata.indexOffset;

    while (offset < endOffset) {
      const [entry, bytesRead] = await this.readEntryAt(handle, offset);
      
      if (entry.key === key) {
        return entry;
      }
      
      if (entry.key > key) {
        return null;
      }
      
      offset += bytesRead;
    }

    return null;
  }


  public mayContain(key: string): boolean {
    return this.isInRange(key);
  }

  public isInRange(key: string): boolean {
    this.ensureOpen();
    const { metadata } = this.state!;
    return key >= metadata.firstKey && key <= metadata.lastKey;
  }

  public async *iterate(startKey?: string, endKey?: string): AsyncIterable<SSTableEntry> {
    this.ensureOpen();
    const { metadata, sparseIndex, handle } = this.state!;

    const effectiveStart = startKey ?? metadata.firstKey;
    const effectiveEnd = endKey ?? metadata.lastKey;

    if (effectiveStart > metadata.lastKey || effectiveEnd < metadata.firstKey) {
      return;
    }

    const startOffset = this.findStartOffset(effectiveStart, sparseIndex, metadata.dataOffset);
    const endOffset = metadata.indexOffset;

    let offset = startOffset;

    while (offset < endOffset) {
      const [entry, bytesRead] = await this.readEntryAt(handle, offset);
      
      if (entry.key < effectiveStart) {
        offset += bytesRead;
        continue;
      }
      
      if (entry.key > effectiveEnd) {
        return;
      }
      
      yield entry;
      
      offset += bytesRead;
    }
  }

  /**
   * Read and parse the SSTable footer.
   * 
   * Footer format (with footerSize for easy parsing):
   * [fileNumber:4][entryCount:4][dataOffset:8][indexOffset:8]
   * [firstKeyLen:2][firstKey:N][lastKeyLen:2][lastKey:M]
   * [createdAt:8][version:2][footerSize:4][magic:4]
   */
  protected async readFooter(handle: FileHandle, fileSize: number): Promise<SSTableMetadata> {
    // Read last 8 bytes: [footerSize:4][magic:4]
    const tailBuf = Buffer.allocUnsafe(8);
    await handle.read(tailBuf, 0, 8, fileSize - 8);
    
    const footerSize = tailBuf.readUInt32BE(0);
    const magic = tailBuf.readUInt32BE(4);
    
    if (magic !== SSTABLE_MAGIC) {
      throw new Error(`SSTableReader: Invalid magic number (got 0x${magic.toString(16)}, expected 0x${SSTABLE_MAGIC.toString(16)}) in ${this.filePath}`);
    }
    
    if (footerSize > fileSize || footerSize < 46) {
      throw new Error(`SSTableReader: Invalid footer size ${footerSize} in ${this.filePath}`);
    }
    
    const footerStart = fileSize - footerSize;
    const footerBuf = Buffer.allocUnsafe(footerSize);
    await handle.read(footerBuf, 0, footerSize, footerStart);

    let pos = 0;
    
    const fileNumber = footerBuf.readUInt32BE(pos);
    pos += 4;
    
    const entryCount = footerBuf.readUInt32BE(pos);
    pos += 4;
    
    const dataOffset = Number(footerBuf.readBigUInt64BE(pos));
    pos += 8;
    
    const indexOffset = Number(footerBuf.readBigUInt64BE(pos));
    pos += 8;
    
    const firstKeyLen = footerBuf.readUInt16BE(pos);
    pos += 2;
    
    const firstKey = footerBuf.toString('utf8', pos, pos + firstKeyLen);
    pos += firstKeyLen;
    
    const lastKeyLen = footerBuf.readUInt16BE(pos);
    pos += 2;
    
    const lastKey = footerBuf.toString('utf8', pos, pos + lastKeyLen);
    pos += lastKeyLen;
    
    const createdAt = Number(footerBuf.readBigUInt64BE(pos));
    pos += 8;
    
    const version = footerBuf.readUInt16BE(pos);
    if (version !== SSTABLE_VERSION) {
      throw new Error(`SSTableReader: Unsupported version ${version} in ${this.filePath}`);
    }

    return {
      fileNumber,
      filePath: this.filePath,
      entryCount,
      firstKey,
      lastKey,
      fileSize,
      createdAt,
      indexOffset,
      dataOffset,
    };
  }

  private async parseFooterFromEnd(handle: FileHandle, fileSize: number): Promise<SSTableMetadata> {
    const tailBuf = Buffer.allocUnsafe(8);
    await handle.read(tailBuf, 0, 8, fileSize - 8);
    
    const footerSize = tailBuf.readUInt32BE(0);
    const magic = tailBuf.readUInt32BE(4);
    
    if (magic !== SSTABLE_MAGIC) {
      throw new Error(`SSTableReader: Invalid magic number (got ${magic.toString(16)}, expected ${SSTABLE_MAGIC.toString(16)})`);
    }
    
    if (footerSize > fileSize || footerSize < 46) {
      throw new Error(`SSTableReader: Invalid footer size ${footerSize}`);
    }
    
    const footerStart = fileSize - footerSize;
    const footerBuf = Buffer.allocUnsafe(footerSize);
    await handle.read(footerBuf, 0, footerSize, footerStart);
    
    let pos = 0;
    
    const fileNumber = footerBuf.readUInt32BE(pos);
    pos += 4;
    
    const entryCount = footerBuf.readUInt32BE(pos);
    pos += 4;
    
    const dataOffset = Number(footerBuf.readBigUInt64BE(pos));
    pos += 8;
    
    const indexOffset = Number(footerBuf.readBigUInt64BE(pos));
    pos += 8;
    
    const firstKeyLen = footerBuf.readUInt16BE(pos);
    pos += 2;
    
    const firstKey = footerBuf.toString('utf8', pos, pos + firstKeyLen);
    pos += firstKeyLen;
    
    const lastKeyLen = footerBuf.readUInt16BE(pos);
    pos += 2;
    
    const lastKey = footerBuf.toString('utf8', pos, pos + lastKeyLen);
    pos += lastKeyLen;
    
    const createdAt = Number(footerBuf.readBigUInt64BE(pos));
    pos += 8;
    
    const version = footerBuf.readUInt16BE(pos);
    if (version !== SSTABLE_VERSION) {
      throw new Error(`SSTableReader: Unsupported version ${version}`);
    }

    return {
      fileNumber,
      filePath: this.filePath,
      entryCount,
      firstKey,
      lastKey,
      fileSize,
      createdAt,
      indexOffset,
      dataOffset,
    };
  }

  protected async loadSparseIndex(
    handle: FileHandle, 
    indexOffset: number,
    fileSize: number
  ): Promise<IndexEntry[]> {
    const index: IndexEntry[] = [];

    const countBuf = Buffer.allocUnsafe(4);
    await handle.read(countBuf, 0, 4, indexOffset);
    const indexCount = countBuf.readUInt32BE(0);

    let offset = indexOffset + 4;
    
    for (let i = 0; i < indexCount; i++) {
      const keyLenBuf = Buffer.allocUnsafe(2);
      await handle.read(keyLenBuf, 0, 2, offset);
      const keyLen = keyLenBuf.readUInt16BE(0);

      const entrySize = 2 + keyLen + 8;
      const entryBuf = Buffer.allocUnsafe(entrySize);
      await handle.read(entryBuf, 0, entrySize, offset);

      const [indexEntry] = SSTableSerializer.deserializeIndexEntry(entryBuf, 0);
      index.push(indexEntry);

      offset += entrySize;
    }

    return index;
  }

  protected async readEntryAt(handle: FileHandle, offset: number): Promise<[SSTableEntry, number]> {
    const headerBuf = Buffer.allocUnsafe(6);
    await handle.read(headerBuf, 0, 6, offset);
    
    const keyLen = headerBuf.readUInt16BE(0);
    const valueLen = headerBuf.readUInt32BE(2);
    
    const entrySize = 2 + keyLen + 4 + valueLen + 8 + 1;
    
    const entryBuf = Buffer.allocUnsafe(entrySize);
    await handle.read(entryBuf, 0, entrySize, offset);
    
    return SSTableSerializer.deserializeEntry(entryBuf, 0);
  }

  private findStartOffset(
    key: string, 
    sparseIndex: IndexEntry[],
    dataOffset: number
  ): number {
    if (sparseIndex.length === 0) {
      return dataOffset;
    }

    let left = 0;
    let right = sparseIndex.length - 1;
    let result = dataOffset;

    while (left <= right) {
      const mid = Math.floor((left + right) / 2);
      const indexEntry = sparseIndex[mid]!;

      if (indexEntry.key <= key) {
        result = indexEntry.offset;
        left = mid + 1;
      } else {
        right = mid - 1;
      }
    }

    return result;
  }

  private ensureOpen(): void {
    if (this.state === null) {
      throw new Error('SSTableReader: Not open. Call open() first.');
    }
  }
}
