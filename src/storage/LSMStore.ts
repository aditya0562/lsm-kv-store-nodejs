/**
 * LSMStore - Main storage engine orchestrator
 * 
 * Design Patterns:
 * 1. Facade Pattern: Simple interface hiding complex subsystem interactions
 * 2. Dependency Injection: Dependencies injected via constructor (SOLID - DIP)
 * 3. Strategy Pattern: Sync policy determines durability behavior
 * 
 * For Java developers: Similar to a Spring-managed service with constructor injection.
 * Dependencies (WAL, MemTable) are injected, not created internally.
 * 
 * Write Path: WAL → MemTable (ensures durability before in-memory update)
 * Read Path: MemTable only (Phase 1). Phase 2 adds SSTable reads.
 */

import * as path from 'path';
import { WAL } from './wal/WAL';
import { LogOperationType, LogEntry } from './wal/LogEntry';
import { MemTable } from './memtable/MemTable';
import { StorageConfig } from '../common/Config';
import { IStorageEngine, IWAL, IMemTable } from '../interfaces/Storage';

/**
 * Factory function for creating default dependencies
 * 
 * Design: Factory Method Pattern - encapsulates object creation.
 * Production code uses defaults, tests can inject mocks.
 */
export interface LSMStoreDependencies {
  wal: IWAL;
  memTable: IMemTable;
}

export class LSMStore implements IStorageEngine {
  private readonly config: StorageConfig;
  private readonly wal: IWAL;
  private readonly memTable: IMemTable;
  private initialized: boolean = false;
  
  /**
   * Constructor with dependency injection
   * 
   * @param config - Storage configuration
   * @param dependencies - Optional injected dependencies (for testing)
   * 
   * Design: If dependencies not provided, creates defaults.
   * This allows both production use (no deps) and testing (mock deps).
   */
  constructor(config: StorageConfig, dependencies?: Partial<LSMStoreDependencies>) {
    this.config = config;
    
    // Dependency Injection: Use provided dependencies or create defaults
    const walDir = path.join(config.dataDir, 'wal');
    this.wal = dependencies?.wal ?? new WAL(walDir, config.syncPolicy);
    this.memTable = dependencies?.memTable ?? new MemTable(config.memTableSizeLimit);
  }
  
  /**
   * Initialize storage engine
   * 
   * Opens WAL and replays entries for crash recovery.
   * Template Method: Subclasses can override for additional initialization.
   */
  async initialize(): Promise<void> {
    if (this.initialized) {
      throw new Error('LSMStore already initialized');
    }
    
    await this.wal.open();
    
    // Replay WAL entries into MemTable (crash recovery)
    const entries = await this.wal.replay();
    for (const entry of entries) {
      this.applyLogEntry(entry);
    }
    
    this.initialized = true;
    console.log(`Initialized: Replayed ${entries.length} entries from WAL`);
  }
  
  /**
   * Put key-value pair
   * 
   * Write Path: WAL first (durability), then MemTable (fast reads).
   * Invariant: Data is durable in WAL before considered "written".
   */
  async put(key: string, value: string): Promise<void> {
    this.ensureInitialized();
    
    // Write to WAL first (durability guarantee)
    await this.wal.append({
      operation: LogOperationType.PUT,
      key,
      value,
    });
    
    // Then update MemTable
    this.memTable.put(key, value, false);
    
    // Warn if MemTable is full (Phase 1 limitation)
    if (this.memTable.isFull()) {
      console.warn('MemTable is full. Phase 2 will add automatic flushing to SSTable.');
    }
  }
  
  /**
   * Get value for key
   * 
   * Read Path: MemTable only (Phase 1).
   * Phase 2 extends to: MemTable → Immutable MemTables → SSTables
   */
  async get(key: string): Promise<string | null> {
    this.ensureInitialized();
    return this.memTable.get(key);
  }
  
  /**
   * Delete key (tombstone)
   * 
   * Creates tombstone entry that hides the value.
   * Tombstones are cleaned up during compaction (Phase 2).
   */
  async delete(key: string): Promise<void> {
    this.ensureInitialized();
    
    await this.wal.append({
      operation: LogOperationType.DELETE,
      key,
    });
    
    this.memTable.delete(key);
  }
  
  /**
   * Close storage engine gracefully
   */
  async close(): Promise<void> {
    await this.wal.close();
    this.initialized = false;
  }
  
  /**
   * Apply log entry to MemTable (used during replay)
   * 
   * Command Pattern: Each log entry is a command that modifies state.
   */
  private applyLogEntry(entry: LogEntry): void {
    switch (entry.operation) {
      case LogOperationType.PUT:
        if (entry.key && entry.value !== undefined) {
          this.memTable.put(entry.key, entry.value, false);
        }
        break;
        
      case LogOperationType.DELETE:
        if (entry.key) {
          this.memTable.delete(entry.key);
        }
        break;
        
      case LogOperationType.BATCH_PUT:
        if (entry.keys && entry.values) {
          for (let i = 0; i < entry.keys.length; i++) {
            const key = entry.keys[i];
            const value = entry.values[i];
            if (key && value !== undefined) {
              this.memTable.put(key, value, false);
            }
          }
        }
        break;
    }
  }
  
  /**
   * Guard clause: Ensure store is initialized
   */
  private ensureInitialized(): void {
    if (!this.initialized) {
      throw new Error('LSMStore not initialized. Call initialize() first.');
    }
  }
}
