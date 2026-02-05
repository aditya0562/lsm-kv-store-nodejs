import * as fs from 'fs/promises';
import { IManifest } from '../../storage/manifest';
import { 
  SSTableWriter, 
  SSTableReader, 
  SSTableMetadata,
  SSTableEntry,
} from '../../storage/sstable';
import { MergeIterator, MergeEntry } from '../../storage/iterator';
import { ICompactionManager, CompactionCompleteCallback } from './ICompactionManager';
import { 
  CompactionConfig, 
  CompactionResult, 
  CompactionStats,
  DEFAULT_COMPACTION_CONFIG,
} from './CompactionTypes';


export interface CompactionManagerDependencies {
  readonly manifest: IManifest;
  
  readonly onComplete: CompactionCompleteCallback;
}

export class CompactionManager implements ICompactionManager {
  private readonly manifest: IManifest;
  private readonly config: CompactionConfig;
  private readonly onComplete: CompactionCompleteCallback;
  
  private running: boolean = false;
  private compacting: boolean = false;
  private timer: ReturnType<typeof setInterval> | null = null;
  private currentCompaction: Promise<CompactionResult | null> | null = null;
  
  private totalCompactions: number = 0;
  private totalEntriesCompacted: number = 0;
  private totalEntriesRemoved: number = 0;
  private lastCompactionTime: number | null = null;

  /**
   * Create a CompactionManager.
   * 
   * @param dependencies - Required dependencies (manifest, callback)
   * @param config - Optional configuration overrides
   */
  constructor(
    dependencies: CompactionManagerDependencies,
    config?: Partial<CompactionConfig>
  ) {
    this.manifest = dependencies.manifest;
    this.onComplete = dependencies.onComplete;
    this.config = { ...DEFAULT_COMPACTION_CONFIG, ...config };
  }

  public start(): void {
    if (this.running) {
      return;
    }

    this.running = true;
    console.log(`CompactionManager: Started (threshold=${this.config.threshold}, interval=${this.config.checkIntervalMs}ms)`);

    this.checkAndTrigger();

    this.timer = setInterval(() => {
      this.checkAndTrigger();
    }, this.config.checkIntervalMs);
  }

  public async stop(): Promise<void> {
    if (!this.running) {
      return;
    }

    this.running = false;

    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }

    if (this.currentCompaction) {
      console.log('CompactionManager: Waiting for in-progress compaction...');
      try {
        await this.currentCompaction;
      } catch (error) {
        console.error('CompactionManager: Error during shutdown compaction:', error);
      }
    }

    console.log('CompactionManager: Stopped');
  }

  public async triggerCompaction(): Promise<CompactionResult | null> {
    if (this.compacting) {
      console.log('CompactionManager: Compaction already in progress, skipping');
      return null;
    }

    return this.doCompaction();
  }

  public isCompacting(): boolean {
    return this.compacting;
  }

  public getStats(): CompactionStats {
    return {
      totalCompactions: this.totalCompactions,
      totalEntriesCompacted: this.totalEntriesCompacted,
      totalEntriesRemoved: this.totalEntriesRemoved,
      isCompacting: this.compacting,
      lastCompactionTime: this.lastCompactionTime,
    };
  }

  private checkAndTrigger(): void {
    if (!this.shouldCompact()) {
      return;
    }

    this.currentCompaction = this.doCompaction()
      .catch(error => {
        console.error('CompactionManager: Compaction failed:', error);
        return null;
      })
      .finally(() => {
        this.currentCompaction = null;
      });
  }

  private shouldCompact(): boolean {
    if (this.compacting) {
      return false;
    }

    const sstables = this.manifest.getSSTables();
    return sstables.length >= this.config.threshold;
  }

  /**
   * Perform the actual compaction.
   * 
   * Algorithm:
   * 1. Snapshot current SSTables
   * 2. Open readers for all SSTables
   * 3. Merge using MergeIterator (dedup + remove tombstones)
   * 4. Write to new SSTable
   * 5. Update manifest atomically
   * 6. Notify LSMStore via callback
   * 7. Delete old SSTable files
   */
  private async doCompaction(): Promise<CompactionResult | null> {
    const startTime = Date.now();
    
    const sstableMetas = this.manifest.getSSTables();
    
    if (sstableMetas.length < 2) {
      return null;
    }

    this.compacting = true;
    console.log(`CompactionManager: Starting compaction of ${sstableMetas.length} SSTables`);

    try {
      const readers: SSTableReader[] = [];
      for (const meta of sstableMetas) {
        const reader = await SSTableReader.openFile(meta.filePath);
        readers.push(reader);
      }

      const asyncSources = readers.map(reader => this.readerToAsyncIterable(reader));
      const merger = MergeIterator.fromAsyncSources(asyncSources, { 
        includeTombstones: false,
      });

      const newFileNumber = this.manifest.getNextFileNumber();
      const writer = new SSTableWriter(newFileNumber, {
        dataDir: this.config.sstableDir,
      });

      let entriesWritten = 0;
      let totalEntriesRead = sstableMetas.reduce((sum, m) => sum + m.entryCount, 0);

      for await (const pair of merger.iterate()) {
        const entry: SSTableEntry = {
          key: pair.key,
          value: pair.value,
          timestamp: Date.now(),
          deleted: false,
        };
        writer.add(entry);
        entriesWritten++;
      }

      for (const reader of readers) {
        await reader.close();
      }

      if (entriesWritten === 0) {
        console.log('CompactionManager: All entries were tombstones, nothing to write');
        
        const compactedFileNumbers = sstableMetas.map(m => m.fileNumber);
        await this.manifest.removeSSTables(compactedFileNumbers);

        await this.deleteOldFiles(sstableMetas);

        this.updateStats(0, totalEntriesRead, Date.now() - startTime);
        
        return null;
      }

      const newMetadata = await writer.build();
      console.log(`CompactionManager: New SSTable ${newFileNumber} created with ${entriesWritten} entries`);

      const compactedFileNumbers = sstableMetas.map(m => m.fileNumber);
      await this.manifest.applyEdit({
        addedSSTables: [newMetadata],
        removedFileNumbers: compactedFileNumbers,
        nextFileNumber: newFileNumber + 1,
      });

      const entriesRemoved = totalEntriesRead - entriesWritten;
      const durationMs = Date.now() - startTime;

      const result: CompactionResult = {
        newSSTable: newMetadata,
        compactedFileNumbers,
        entriesWritten,
        entriesRemoved,
        durationMs,
      };

      await this.onComplete(result);

      await this.deleteOldFiles(sstableMetas);

      this.updateStats(entriesWritten, entriesRemoved, durationMs);

      console.log(
        `CompactionManager: Compaction complete in ${durationMs}ms. ` +
        `${sstableMetas.length} files → 1 file. ` +
        `${entriesWritten} entries kept, ${entriesRemoved} removed.`
      );

      return result;
    } finally {
      this.compacting = false;
    }
  }

  private async *readerToAsyncIterable(reader: SSTableReader): AsyncIterable<MergeEntry> {
    for await (const entry of reader.iterate()) {
      yield {
        key: entry.key,
        value: entry.value,
        timestamp: entry.timestamp,
        deleted: entry.deleted,
      };
    }
  }

  private async deleteOldFiles(sstables: SSTableMetadata[]): Promise<void> {
    for (const meta of sstables) {
      try {
        await fs.unlink(meta.filePath);
        console.log(`CompactionManager: Deleted ${meta.filePath}`);
      } catch (error) {
        console.warn(`CompactionManager: Failed to delete ${meta.filePath}:`, error);
      }
    }
  }

  private updateStats(entriesWritten: number, entriesRemoved: number, durationMs: number): void {
    this.totalCompactions++;
    this.totalEntriesCompacted += entriesWritten;
    this.totalEntriesRemoved += entriesRemoved;
    this.lastCompactionTime = Date.now();
  }
}
