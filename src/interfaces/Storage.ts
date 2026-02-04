/**
 * Storage Engine Interfaces
 * 
 * Design: Interface Segregation Principle (ISP) - clients depend only on
 * the interfaces they need. Dependency Inversion Principle (DIP) - high-level
 * modules depend on abstractions, not concrete implementations.
 * 
 * Java developers: These interfaces are similar to Java interfaces.
 * They enable dependency injection and testability.
 */

import { LogEntry, LogOperationType } from '../storage/wal/LogEntry';
import { MemTableEntry } from '../storage/memtable/MemTableEntry';

/**
 * Write-Ahead Log interface
 * 
 * Abstraction for durability layer. Implementations can vary in
 * sync policy, storage medium, or replication strategy.
 */
export interface IWAL {
  open(): Promise<void>;
  append(entry: Omit<LogEntry, 'sequenceId' | 'timestamp'>): Promise<void>;
  replay(): Promise<LogEntry[]>;
  checkpoint(): Promise<void>;
  close(): Promise<void>;
}

/**
 * MemTable interface
 * 
 * Abstraction for in-memory sorted key-value store.
 * Implementations must maintain sorted order for efficient range queries.
 */
export interface IMemTable {
  put(key: string, value: string, deleted?: boolean): void;
  get(key: string): string | null;
  delete(key: string): void;
  isFull(): boolean;
  size(): number;
  getAllSorted(): Array<{ key: string; entry: MemTableEntry }>;
  clear(): void;
}

/**
 * Storage Engine interface
 * 
 * Main abstraction for the KV store. HTTP/TCP servers depend on this
 * interface, not concrete implementation. Enables testing with mocks.
 */
export interface IStorageEngine {
  initialize(): Promise<void>;
  put(key: string, value: string): Promise<void>;
  get(key: string): Promise<string | null>;
  delete(key: string): Promise<void>;
  close(): Promise<void>;
}

/**
 * Factory interface for creating storage components
 * 
 * Abstract Factory Pattern: Encapsulates object creation.
 * Allows different configurations (test vs production) to
 * create different implementations.
 */
export interface IStorageFactory {
  createWAL(): IWAL;
  createMemTable(): IMemTable;
}
