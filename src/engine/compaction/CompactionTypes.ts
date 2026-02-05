import { SSTableMetadata } from '../../storage/sstable';
import { SSTableTuning } from '../../common/Config';

export interface CompactionConfig {
  readonly threshold: number;
  readonly checkIntervalMs: number;
  readonly sstableDir: string;
  readonly sstableTuning: SSTableTuning;
}

export const DEFAULT_COMPACTION_CONFIG: Omit<CompactionConfig, 'sstableTuning'> = {
  threshold: 4,
  checkIntervalMs: 60_000,
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
