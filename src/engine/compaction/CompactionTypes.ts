import { SSTableMetadata } from '../../storage/sstable';

export interface CompactionConfig {
  readonly threshold: number;
  
  readonly checkIntervalMs: number;
  
  readonly sstableDir: string;
}

export const DEFAULT_COMPACTION_CONFIG: CompactionConfig = {
  threshold: 4,
  checkIntervalMs: 60_000, // 1 minute
  sstableDir: './data/sstables',
};

export interface CompactionResult {
  readonly newSSTable: SSTableMetadata;
  
  readonly compactedFileNumbers: number[];
  
  readonly entriesWritten: number;
  
  readonly entriesRemoved: number;
  
  readonly durationMs: number;
}

export interface CompactionStats {
  readonly totalCompactions: number;
  
  readonly totalEntriesCompacted: number;
  
  readonly totalEntriesRemoved: number;
  
  readonly isCompacting: boolean;
  
  readonly lastCompactionTime: number | null;
}
