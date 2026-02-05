export enum SyncPolicy {
  /**
   * Sync after every write. Highest durability, lowest throughput (~1K writes/sec).
   * Use when data loss is unacceptable.
   */
  SYNC_EVERY_WRITE = 'sync_every_write',
  
  /**
   * Batch writes and sync every 100ms. Balanced durability/throughput (~10K writes/sec).
   * Default policy - good for most use cases.
   */
  GROUP_COMMIT_100MS = 'group_commit_100ms',
  
  /**
   * Batch writes and sync every 10ms. Higher throughput, lower durability (~50K writes/sec).
   * Use when throughput is critical and small data loss window is acceptable.
   */
  PERIODIC_10MS = 'periodic_10ms',
}

export interface StorageConfig {
  dataDir: string;
  memTableSizeLimit: number;
  syncPolicy: SyncPolicy;
  httpPort: number;
  
  /** Enable background compaction (default: true) */
  enableCompaction?: boolean;
  
  /** Compaction threshold - number of SSTables to trigger compaction */
  compactionThreshold?: number;
}

export const DEFAULT_CONFIG: StorageConfig = {
  dataDir: './data',
  memTableSizeLimit: 4 * 1024 * 1024, // 4MB
  syncPolicy: SyncPolicy.GROUP_COMMIT_100MS,
  httpPort: 3000,
  enableCompaction: true,
  compactionThreshold: 4,
};
