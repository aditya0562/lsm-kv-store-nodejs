export interface SSTableEntry {
  readonly key: string;
  readonly value: string;
  readonly timestamp: number;
  readonly deleted: boolean;
}

export interface SSTableMetadata {
  readonly fileNumber: number;
  readonly filePath: string;
  readonly entryCount: number;
  readonly firstKey: string;
  readonly lastKey: string;
  readonly fileSize: number;
  readonly createdAt: number;
  readonly indexOffset: number;
  readonly dataOffset: number;
  readonly bloomFilterOffset?: number;
}

export interface IndexEntry {
  readonly key: string;
  readonly offset: number;
}

export interface SSTableConfig {
  readonly dataDir: string;
  readonly sparseIndexInterval: number;
  readonly targetFileSize: number;
  readonly bloomFilterFalsePositiveRate: number;
}

export const DEFAULT_SSTABLE_CONFIG: SSTableConfig = {
  dataDir: './data/sstables',
  sparseIndexInterval: 10,
  targetFileSize: 4 * 1024 * 1024,
  bloomFilterFalsePositiveRate: 0.01,
};

export const SSTABLE_MAGIC = 0x5353544C;
export const SSTABLE_VERSION = 2;
