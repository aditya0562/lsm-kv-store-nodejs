/**
 * Storage Factory - Creates storage components
 * 
 * Design Pattern: Abstract Factory
 * 
 * Encapsulates object creation logic. Allows different factory implementations
 * for different environments (production vs test vs benchmark).
 * 
 * For Java developers: Similar to Spring's @Configuration class with @Bean methods,
 * or a factory pattern with dependency injection container.
 */

import * as path from 'path';
import { StorageConfig } from '../common/Config';
import { WAL } from '../storage/wal/WAL';
import { MemTable } from '../storage/memtable/MemTable';
import { LSMStore, LSMStoreDependencies } from '../storage/LSMStore';
import { HTTPServer } from '../server/HTTPServer';
import { IWAL, IMemTable, IStorageEngine, IStorageFactory } from '../interfaces/Storage';

/**
 * Default storage factory for production use
 */
export class DefaultStorageFactory implements IStorageFactory {
  private readonly config: StorageConfig;
  
  constructor(config: StorageConfig) {
    this.config = config;
  }
  
  createWAL(): IWAL {
    const walDir = path.join(this.config.dataDir, 'wal');
    return new WAL(walDir, this.config.syncPolicy);
  }
  
  createMemTable(): IMemTable {
    return new MemTable(this.config.memTableSizeLimit);
  }
}

/**
 * Application Builder - Assembles the complete application
 * 
 * Design Pattern: Builder
 * 
 * Separates construction from representation. Allows step-by-step
 * construction of complex objects.
 */
export class ApplicationBuilder {
  private config: StorageConfig;
  private factory: IStorageFactory | null = null;
  private dependencies: Partial<LSMStoreDependencies> | null = null;
  
  constructor(config: StorageConfig) {
    this.config = config;
  }
  
  withFactory(factory: IStorageFactory): ApplicationBuilder {
    this.factory = factory;
    return this;
  }
  
  withDependencies(deps: Partial<LSMStoreDependencies>): ApplicationBuilder {
    this.dependencies = deps;
    return this;
  }
  
  buildStorage(): IStorageEngine {
    if (this.dependencies) {
      return new LSMStore(this.config, this.dependencies);
    }
    
    if (this.factory) {
      return new LSMStore(this.config, {
        wal: this.factory.createWAL(),
        activeMemTable: this.factory.createMemTable(),
      });
    }
    
    return new LSMStore(this.config);
  }
  
  buildHTTPServer(store: IStorageEngine, port: number): HTTPServer {
    return new HTTPServer(store, port);
  }
}
