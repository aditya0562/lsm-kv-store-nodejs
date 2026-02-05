export type { 
  CompactionConfig, 
  CompactionResult, 
  CompactionStats,
} from './CompactionTypes';
export { DEFAULT_COMPACTION_CONFIG } from './CompactionTypes';

export type { 
  ICompactionManager, 
  CompactionCompleteCallback,
} from './ICompactionManager';

export { CompactionManager } from './CompactionManager';
export type { CompactionManagerDependencies } from './CompactionManager';
