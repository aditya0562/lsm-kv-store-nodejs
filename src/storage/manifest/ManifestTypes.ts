/**
 * Manifest Type Definitions
 * 
 * Single Responsibility: Only type definitions for manifest tracking.
 * 
 * The manifest tracks the current state of the database:
 *   - Which SSTables are active
 *   - Next file number for new SSTables
 *   - Last WAL sequence that was flushed
 */

import { SSTableMetadata } from '../sstable';

export interface VersionEdit {
  readonly addedSSTables: SSTableMetadata[];

  readonly removedFileNumbers: number[];
  
  readonly nextFileNumber: number;
  
  readonly lastFlushedSequence: number;
  
  readonly timestamp: number;
}

export interface ManifestState {

  readonly sstables: SSTableMetadata[];
  
  readonly nextFileNumber: number;
  
  readonly lastFlushedSequence: number;
  
  readonly version: number;
  
  readonly createdAt: number;
}

export const EMPTY_MANIFEST_STATE: ManifestState = {
  sstables: [],
  nextFileNumber: 1,
  lastFlushedSequence: 0,
  version: 0,
  createdAt: Date.now(),
};

export const MANIFEST_MAGIC = 0x4D414E49;
export const MANIFEST_VERSION = 1;
