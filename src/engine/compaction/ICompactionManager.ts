import { CompactionResult, CompactionStats } from './CompactionTypes';


export type CompactionCompleteCallback = (result: CompactionResult) => Promise<void>;

/**
 * Interface for managing background compaction.
 * 
 * Lifecycle:
 * 1. Create with configuration
 * 2. Call start() to begin background checks
 * 3. Compaction runs automatically when threshold met
 * 4. Call stop() for graceful shutdown
 */
export interface ICompactionManager {
  
  start(): void;

  stop(): Promise<void>;

  triggerCompaction(): Promise<CompactionResult | null>;

  isCompacting(): boolean;

  getStats(): CompactionStats;
}
