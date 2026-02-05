import { SSTableMetadata } from '../sstable';
import { ManifestState, VersionEdit } from './ManifestTypes';

/**
 * Interface for manifest management.
 * 
 * Lifecycle:
 * 1. load() - Read existing manifest or create new
 * 2. getState() - Get current state
 * 3. applyEdit() - Apply changes (add/remove SSTables)
 * 4. save() - Persist changes to disk
 */
export interface IManifest {
  load(): Promise<void>;

  getState(): ManifestState;

  getSSTables(): SSTableMetadata[];

  getNextFileNumber(): number;

  applyEdit(edit: Partial<VersionEdit>): Promise<void>;

  addSSTable(metadata: SSTableMetadata): Promise<void>;

  removeSSTables(fileNumbers: number[]): Promise<void>;

  save(): Promise<void>;
}
