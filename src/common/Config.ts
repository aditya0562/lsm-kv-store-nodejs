export enum SyncPolicy {
  SYNC_EVERY_WRITE = 'sync_every_write',
  GROUP_COMMIT_100MS = 'group_commit_100ms',
  PERIODIC_10MS = 'periodic_10ms',
}

export interface SSTableTuning {
  sparseIndexInterval: number;
  bloomFilterFalsePositiveRate: number;
}

export interface StorageConfig {
  dataDir: string;
  memTableSizeLimit: number;
  syncPolicy: SyncPolicy;
  httpPort: number;
  enableCompaction?: boolean;
  compactionThreshold?: number;
  sstableTuning?: Partial<SSTableTuning>;
}

export const DEFAULT_SSTABLE_TUNING: SSTableTuning = {
  sparseIndexInterval: 10,
  bloomFilterFalsePositiveRate: 0.01,
};

export const DEFAULT_CONFIG: StorageConfig = {
  dataDir: './data',
  memTableSizeLimit: 4 * 1024 * 1024,
  syncPolicy: SyncPolicy.GROUP_COMMIT_100MS,
  httpPort: 3000,
  enableCompaction: true,
  compactionThreshold: 4,
};

export function resolveSSTableTuning(config?: Partial<SSTableTuning>): SSTableTuning {
  const resolved = { ...DEFAULT_SSTABLE_TUNING, ...config };
  
  if (resolved.sparseIndexInterval < 1) {
    throw new Error('sparseIndexInterval must be >= 1');
  }
  if (resolved.sparseIndexInterval > 1000) {
    throw new Error('sparseIndexInterval must be <= 1000');
  }
  if (resolved.bloomFilterFalsePositiveRate <= 0 || resolved.bloomFilterFalsePositiveRate >= 1) {
    throw new Error('bloomFilterFalsePositiveRate must be between 0 and 1');
  }
  
  return resolved;
}
