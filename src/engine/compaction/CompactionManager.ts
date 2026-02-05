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
import { SSTableTuning, DEFAULT_SSTABLE_TUNING } from '../../common/Config';

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

  constructor(
    dependencies: CompactionManagerDependencies,
    config?: Partial<Omit<CompactionConfig, 'sstableTuning'>> & { sstableTuning?: Partial<SSTableTuning> }
  ) {
    this.manifest = dependencies.manifest;
    this.onComplete = dependencies.onComplete;
    this.config = {
      ...DEFAULT_COMPACTION_CONFIG,
      ...config,
      sstableTuning: { ...DEFAULT_SSTABLE_TUNING, ...config?.sstableTuning },
    };
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
      await this.currentCompaction;
    }

    console.log('CompactionManager: Stopped');
  }

  public async triggerCompaction(): Promise<CompactionResult | null> {
    if (this.compacting) {
      return this.currentCompaction;
    }

    const state = this.manifest.getState();
    if (state.sstables.length < 2) {
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
    if (this.compacting || !this.running) {
      return;
    }

    const state = this.manifest.getState();
    if (state.sstables.length >= this.config.threshold) {
      this.doCompaction().catch(err => {
        console.error('CompactionManager: Background compaction failed:', err);
      });
    }
  }

  private async doCompaction(): Promise<CompactionResult | null> {
    if (this.compacting) {
      return this.currentCompaction;
    }

    this.compacting = true;
    this.currentCompaction = this.executeCompaction();

    try {
      return await this.currentCompaction;
    } finally {
      this.compacting = false;
      this.currentCompaction = null;
    }
  }

  private async executeCompaction(): Promise<CompactionResult | null> {
    const startTime = Date.now();
    const state = this.manifest.getState();
    const sstableMetas = [...state.sstables];

    if (sstableMetas.length < 2) {
      return null;
    }

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
        sparseIndexInterval: this.config.sstableTuning.sparseIndexInterval,
        bloomFilterFalsePositiveRate: this.config.sstableTuning.bloomFilterFalsePositiveRate,
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
      
      await this.manifest.addSSTable(newMetadata);
      await this.manifest.removeSSTables(compactedFileNumbers);

      const result: CompactionResult = {
        newSSTable: newMetadata,
        compactedFileNumbers,
        entriesWritten,
        entriesRemoved: totalEntriesRead - entriesWritten,
        durationMs: Date.now() - startTime,
      };

      await this.deleteOldFiles(sstableMetas);

      this.updateStats(entriesWritten, totalEntriesRead - entriesWritten, result.durationMs);

      await this.onComplete(result);

      return result;

    } catch (error) {
      console.error('CompactionManager: Compaction failed:', error);
      throw error;
    }
  }

  private updateStats(entriesCompacted: number, entriesRemoved: number, durationMs: number): void {
    this.totalCompactions++;
    this.totalEntriesCompacted += entriesCompacted;
    this.totalEntriesRemoved += entriesRemoved;
    this.lastCompactionTime = Date.now();
  }

  private async deleteOldFiles(metas: SSTableMetadata[]): Promise<void> {
    for (const meta of metas) {
      try {
        await fs.unlink(meta.filePath);
      } catch {
      }
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
}
