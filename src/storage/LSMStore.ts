/**
 * LSMStore - Main storage engine orchestrator
 * Write Path: WAL → Active MemTable → (async) SSTable
 * Read Path: Active MemTable → Immutable MemTable → SSTables (newest first)
 */

import * as fs from 'fs/promises';
import * as path from 'path';
import { WAL, WALEntryListener } from './wal/WAL';
import { LogOperationType, LogEntry } from './wal/LogEntry';
import { MemTable } from './memtable/MemTable';
import { StorageConfig, SSTableTuning, resolveSSTableTuning } from '../common/Config';
import { IStorageEngine, IWAL, IMemTable, KVPair, RangeQueryOptions } from '../interfaces/Storage';
import { 
  SSTableWriter, 
  SSTableReader, 
  SSTableMetadata,
  SSTableEntry,
} from './sstable';
import type { ISSTableReader } from './sstable';
import { MergeIterator, MergeEntry } from './iterator';
import { Manifest, IManifest } from './manifest';
import { 
  CompactionManager, 
  ICompactionManager, 
  CompactionResult,
  CompactionStats,
} from '../engine/compaction';

export interface LSMStoreDependencies {
  wal?: IWAL;
  activeMemTable?: IMemTable;
  manifest?: IManifest;
  compactionManager?: ICompactionManager;
  onWALEntryAppended?: WALEntryListener | undefined;
}

interface SSTableState {
  metadata: SSTableMetadata;
  reader: ISSTableReader;
}

export class LSMStore implements IStorageEngine {
  private readonly config: StorageConfig;
  private readonly wal: IWAL;
  private readonly manifest: IManifest;
  private readonly sstableDir: string;
  private readonly sstableTuning: SSTableTuning;
  
  private activeMemTable: IMemTable;
  private immutableMemTable: IMemTable | null = null;
  
  private sstables: SSTableState[] = [];
  
  private flushing: boolean = false;
  private flushPromise: Promise<void> | null = null;
  
  private compactionManager: ICompactionManager | null = null;
  private useCompaction: boolean;
  
  private initialized: boolean = false;

  /**
   * @param config - Storage configuration
   * @param dependencies - Optional injected dependencies (for testing)
   */
  constructor(config: StorageConfig, dependencies?: LSMStoreDependencies) {
    this.config = config;
    this.sstableDir = path.join(config.dataDir, 'sstables');
    this.sstableTuning = resolveSSTableTuning(config.sstableTuning);
    this.useCompaction = config.enableCompaction !== false;
    
    const walDir = path.join(config.dataDir, 'wal');
    this.wal = dependencies?.wal ?? new WAL({
      logDir: walDir,
      syncPolicy: config.syncPolicy,
      onEntryAppended: dependencies?.onWALEntryAppended,
    });
    this.activeMemTable = dependencies?.activeMemTable ?? new MemTable(config.memTableSizeLimit);
    this.manifest = dependencies?.manifest ?? new Manifest(config.dataDir);
    
    if (dependencies?.compactionManager) {
      this.compactionManager = dependencies.compactionManager;
    }
  }

  /**
   * Initialize storage engine.
   * 
   * 1. Create directories
   * 2. Load manifest
   * 3. Open SSTable readers for files in manifest
   * 4. Replay WAL for crash recovery
   * 5. Start compaction manager
   */
  async initialize(): Promise<void> {
    if (this.initialized) {
      throw new Error('LSMStore already initialized');
    }

    await fs.mkdir(this.sstableDir, { recursive: true });
    
    await this.manifest.load();
    
    await this.loadSSTablesFromManifest();
    
    await this.wal.open();
    const entries = await this.wal.replay();
    
    for (const entry of entries) {
      this.applyLogEntry(entry);
    }

    if (this.useCompaction && !this.compactionManager) {
      this.compactionManager = new CompactionManager(
        {
          manifest: this.manifest,
          onComplete: (result) => this.handleCompactionComplete(result),
        },
        {
          sstableDir: this.sstableDir,
          threshold: 4,
          checkIntervalMs: 60_000,
          sstableTuning: this.sstableTuning,
        }
      );
    }
    
    this.compactionManager?.start();

    this.initialized = true;
    console.log(`Initialized: Replayed ${entries.length} WAL entries, loaded ${this.sstables.length} SSTables`);
  }

  /**
   * Put key-value pair.
   * 
   * Write Path:
   * 1. WAL (durability)
   * 2. Active MemTable
   * 3. Trigger flush if full (non-blocking)
   */
  async put(key: string, value: string): Promise<void> {
    this.ensureInitialized();
    await this.wal.append({
      operation: LogOperationType.PUT,
      key,
      value,
    });

    this.activeMemTable.put(key, value, false);

    this.maybeFlush();
  }

  /**
   * Get value for key.
   * 
   * Read Path (newest first):
   * 1. Active MemTable
   * 2. Immutable MemTable (if flushing)
   * 3. SSTables (newest first)
   * 
   * Tombstone Handling: If a tombstone is found at any level, we return null
   * immediately. The tombstone shadows any older values in lower levels.
   */
  async get(key: string): Promise<string | null> {
    this.ensureInitialized();
    const activeEntry = this.activeMemTable.getEntry(key);
    if (activeEntry !== null) {
      return activeEntry.deleted ? null : activeEntry.value;
    }

    if (this.immutableMemTable !== null) {
      const immutableEntry = this.immutableMemTable.getEntry(key);
      if (immutableEntry !== null) {
        return immutableEntry.deleted ? null : immutableEntry.value;
      }
    }

    const sstableSnapshot = [...this.sstables];
    
    for (const sstable of sstableSnapshot) {
      if (!sstable.reader.isInRange(key)) {
        continue;
      }

      if (!sstable.reader.mayContain(key)) {
        continue;
      }

      const entry = await sstable.reader.get(key);
      if (entry !== null) {
        return entry.deleted ? null : entry.value;
      }
    }

    return null;
  }

  async delete(key: string): Promise<void> {
    this.ensureInitialized();

    await this.wal.append({
      operation: LogOperationType.DELETE,
      key,
    });

    this.activeMemTable.delete(key);

    this.maybeFlush();
  }

  async batchPut(entries: KVPair[]): Promise<number> {
    this.ensureInitialized();

    if (entries.length === 0) {
      return 0;
    }

    const keys = entries.map(e => e.key);
    const values = entries.map(e => e.value);

    await this.wal.append({
      operation: LogOperationType.BATCH_PUT,
      keys,
      values,
    });

    for (const entry of entries) {
      this.activeMemTable.put(entry.key, entry.value, false);
    }

    this.maybeFlush();

    return entries.length;
  }

  /**
   * Read a range of keys [startKey, endKey] in sorted order.
   * 
   * Algorithm: K-way merge of all sources (MemTables + SSTables)
   *   - Each source provides sorted entries in the range
   *   - MergeIterator produces unified sorted stream
   *   - Handles duplicates (newest wins) and tombstones (filtered)
   * 
   * @param startKey - Start of range (inclusive)
   * @param endKey - End of range (inclusive)
   * @param options - Optional query parameters (e.g., limit)
   * @yields Key-value pairs in sorted order
   */
  async *readKeyRange(
    startKey: string, 
    endKey: string, 
    options?: RangeQueryOptions
  ): AsyncIterable<KVPair> {
    this.ensureInitialized();

    if (startKey > endKey) {
      return;
    }

    const syncSources: MergeEntry[][] = [];

    const activeEntries = this.memTableRangeToMergeEntries(
      this.activeMemTable.getRange(startKey, endKey)
    );
    if (activeEntries.length > 0) {
      syncSources.push(activeEntries);
    }

    if (this.immutableMemTable !== null) {
      const immutableEntries = this.memTableRangeToMergeEntries(
        this.immutableMemTable.getRange(startKey, endKey)
      );
      if (immutableEntries.length > 0) {
        syncSources.push(immutableEntries);
      }
    }

    const sstableSnapshot = [...this.sstables];
    const asyncSources: AsyncIterable<MergeEntry>[] = [];

    for (const sstable of sstableSnapshot) {
      const meta = sstable.metadata;
      if (meta.lastKey < startKey || meta.firstKey > endKey) {
        continue;
      }

      asyncSources.push(this.sstableRangeToMergeEntries(sstable.reader, startKey, endKey));
    }

    const merger = MergeIterator.fromMixedSources(syncSources, asyncSources);

    let count = 0;
    const limit = options?.limit;

    for await (const pair of merger.iterate()) {
      yield pair;
      count++;

      if (limit !== undefined && count >= limit) {
        break;
      }
    }
  }

  /**
   * Close storage engine gracefully.
   * Waits for any ongoing flush and compaction to complete.
   */
  async close(): Promise<void> {
    if (this.compactionManager) {
      await this.compactionManager.stop();
    }
    
    if (this.flushPromise) {
      try {
        await this.flushPromise;
      } catch (error) {
        console.error('Error during final flush:', error);
      }
    }

    for (const sstable of this.sstables) {
      await sstable.reader.close();
    }

    await this.wal.close();
    this.initialized = false;
  }

  private maybeFlush(): void {
    if (!this.activeMemTable.isFull()) {
      return;
    }

    if (this.flushing) {
      console.warn('LSMStore: MemTable full but flush already in progress. Skipping.');
      return;
    }

    this.triggerFlush();
  }

  
  private triggerFlush(): void {
    this.immutableMemTable = this.activeMemTable;
    this.activeMemTable = new MemTable(this.config.memTableSizeLimit);

    console.log('LSMStore: MemTable swap complete. Starting background flush...');

    this.flushing = true;
    this.flushPromise = this.doFlush()
      .then(() => {
        console.log('LSMStore: Flush completed successfully');
      })
      .catch((error) => {
        console.error('LSMStore: Flush failed:', error);
      })
      .finally(() => {
        this.flushing = false;
        this.flushPromise = null;
      });
  }

  /**
   * Perform the actual flush operation.
   * 
   * Algorithm:
   * 1. Get next file number from manifest
   * 2. Get sorted entries from immutable MemTable
   * 3. Write to SSTable using SSTableWriter
   * 4. Update manifest with new SSTable
   * 5. Open reader and add to readers list
   * 6. Checkpoint WAL
   * 7. Clear immutable MemTable reference
   */
  private async doFlush(): Promise<void> {
    if (this.immutableMemTable === null) {
      return;
    }

    const fileNumber = this.manifest.getNextFileNumber();
    console.log(`LSMStore: Flushing to SSTable ${fileNumber}...`);

    const entries = this.immutableMemTable.getAllSorted();

    if (entries.length === 0) {
      this.immutableMemTable = null;
      return;
    }

    const writer = new SSTableWriter(fileNumber, {
      dataDir: this.sstableDir,
      sparseIndexInterval: this.sstableTuning.sparseIndexInterval,
      bloomFilterFalsePositiveRate: this.sstableTuning.bloomFilterFalsePositiveRate,
    });

    for (const { key, entry } of entries) {
      const sstableEntry: SSTableEntry = {
        key,
        value: entry.value,
        timestamp: entry.timestamp,
        deleted: entry.deleted,
      };
      writer.add(sstableEntry);
    }

    const metadata = await writer.build();
    console.log(`LSMStore: SSTable ${fileNumber} created with ${metadata.entryCount} entries`);

    await this.manifest.addSSTable(metadata);

    const reader = await SSTableReader.openFile(metadata.filePath);

    this.sstables.unshift({ metadata, reader });

    await this.wal.checkpoint();

    this.immutableMemTable = null;
  }

  private async handleCompactionComplete(result: CompactionResult): Promise<void> {
    console.log(`LSMStore: Handling compaction result. Removing ${result.compactedFileNumbers.length} files, adding 1 new.`);

    const compactedSet = new Set(result.compactedFileNumbers);
    const newSSTables: SSTableState[] = [];

    for (const sstable of this.sstables) {
      if (compactedSet.has(sstable.metadata.fileNumber)) {
        await sstable.reader.close();
      } else {
        newSSTables.push(sstable);
      }
    }

    const newReader = await SSTableReader.openFile(result.newSSTable.filePath);
    newSSTables.push({ 
      metadata: result.newSSTable, 
      reader: newReader,
    });

    newSSTables.sort((a, b) => b.metadata.fileNumber - a.metadata.fileNumber);

    this.sstables = newSSTables;

    console.log(`LSMStore: Now have ${this.sstables.length} SSTables after compaction`);
  }

  private async loadSSTablesFromManifest(): Promise<void> {
    const sstableMetadata = this.manifest.getSSTables();

    for (const metadata of sstableMetadata) {
      try {
        const reader = await SSTableReader.openFile(metadata.filePath);
        this.sstables.push({ metadata, reader });
      } catch (error) {
        console.warn(`LSMStore: Failed to load SSTable ${metadata.filePath}:`, error);
        await this.manifest.removeSSTables([metadata.fileNumber]);
      }
    }

    console.log(`LSMStore: Loaded ${this.sstables.length} SSTables from manifest`);
  }

  private applyLogEntry(entry: LogEntry): void {
    switch (entry.operation) {
      case LogOperationType.PUT:
        if (entry.key && entry.value !== undefined) {
          this.activeMemTable.put(entry.key, entry.value, false);
        }
        break;

      case LogOperationType.DELETE:
        if (entry.key) {
          this.activeMemTable.delete(entry.key);
        }
        break;

      case LogOperationType.BATCH_PUT:
        if (entry.keys && entry.values) {
          for (let i = 0; i < entry.keys.length; i++) {
            const key = entry.keys[i];
            const value = entry.values[i];
            if (key && value !== undefined) {
              this.activeMemTable.put(key, value, false);
            }
          }
        }
        break;
    }
  }

  private ensureInitialized(): void {
    if (!this.initialized) {
      throw new Error('LSMStore not initialized. Call initialize() first.');
    }
  }

  public getStats(): {
    activeMemTableSize: number;
    immutableMemTableSize: number | null;
    sstableCount: number;
    isFlushing: boolean;
    manifestVersion: number;
    compaction: CompactionStats | null;
  } {
    return {
      activeMemTableSize: this.activeMemTable.size(),
      immutableMemTableSize: this.immutableMemTable?.size() ?? null,
      sstableCount: this.sstables.length,
      isFlushing: this.flushing,
      manifestVersion: this.manifest.getState().version,
      compaction: this.compactionManager?.getStats() ?? null,
    };
  }

  /**
   * Manually trigger compaction (for testing).
   */
  public async triggerCompaction(): Promise<CompactionResult | null> {
    if (!this.compactionManager) {
      throw new Error('Compaction manager not available');
    }
    return this.compactionManager.triggerCompaction();
  }

  public async applyReplicatedEntry(entry: LogEntry): Promise<void> {
    this.ensureInitialized();
    
    const walEntry: Omit<LogEntry, 'sequenceId' | 'timestamp'> = {
      operation: entry.operation,
    };
    
    if (entry.key !== undefined) {
      (walEntry as { key: string }).key = entry.key;
    }
    if (entry.value !== undefined) {
      (walEntry as { value: string }).value = entry.value;
    }
    if (entry.keys !== undefined) {
      (walEntry as { keys: string[] }).keys = entry.keys;
    }
    if (entry.values !== undefined) {
      (walEntry as { values: string[] }).values = entry.values;
    }
    
    await this.wal.append(walEntry);

    this.applyLogEntry(entry);
    this.maybeFlush();
  }

  private memTableRangeToMergeEntries(
    entries: Array<{ key: string; entry: { value: string; timestamp: number; deleted: boolean } }>
  ): MergeEntry[] {
    return entries.map(({ key, entry }) => ({
      key,
      value: entry.value,
      timestamp: entry.timestamp,
      deleted: entry.deleted,
    }));
  }

  private async *sstableRangeToMergeEntries(
    reader: ISSTableReader,
    startKey: string,
    endKey: string
  ): AsyncIterable<MergeEntry> {
    for await (const entry of reader.iterate(startKey, endKey)) {
      yield {
        key: entry.key,
        value: entry.value,
        timestamp: entry.timestamp,
        deleted: entry.deleted,
      };
    }
  }
}
