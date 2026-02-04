/**
 * Write-Ahead Log (WAL) Implementation
 * 
 * Design Pattern: Write-Ahead Logging ensures durability by writing all
 * mutations to disk before applying to in-memory structures. This enables
 * crash recovery by replaying the log on startup.
 * 
 * File Format:
 * [length: 4 bytes][checksum: 4 bytes][sequenceId: 8 bytes]
 * [timestamp: 8 bytes][operation: 1 byte][payload: variable]
 * 
 * Group Commit Pattern: Batches multiple writes and syncs once, trading
 * off small durability window for significant throughput improvement.
 */

import * as fs from 'fs/promises';
import * as path from 'path';
import { LogEntry, LogOperationType } from './LogEntry';
import { SyncPolicy } from '../../common/Config';
import { IWAL } from '../../interfaces/Storage';

interface PendingWrite {
  entry: LogEntry;
  resolve: () => void;
  reject: (err: Error) => void;
}

/**
 * WAL implements IWAL interface
 * 
 * Design: Dependency Inversion Principle - LSMStore depends on IWAL abstraction,
 * not this concrete implementation. Enables testing with mock WAL.
 */
export class WAL implements IWAL {
  private logDir: string;
  private currentLogFile: string = '';
  private fileHandle: fs.FileHandle | null = null;
  private sequenceId: number = 0;
  private syncPolicy: SyncPolicy;
  
  // Group commit state
  private pendingWrites: PendingWrite[] = [];
  private flushTimer: NodeJS.Timeout | null = null;
  
  constructor(logDir: string, syncPolicy: SyncPolicy) {
    this.logDir = logDir;
    this.syncPolicy = syncPolicy;
  }
  
  /**
   * Open WAL file - creates directory if needed, finds or creates log file
   */
  async open(): Promise<void> {
    await fs.mkdir(this.logDir, { recursive: true });
    
    // Find latest log file or create new
    const files = await fs.readdir(this.logDir);
    const logFiles = files.filter(f => f.endsWith('.log')).sort();
    
    if (logFiles.length > 0) {
      this.currentLogFile = path.join(this.logDir, logFiles[logFiles.length - 1]!);
      this.sequenceId = await this.readLastSequenceId();
    } else {
      await this.rotateLog();
    }
    
    if (!this.currentLogFile) {
      throw new Error('Failed to initialize log file');
    }
    
    this.fileHandle = await fs.open(this.currentLogFile, 'a');
    
    // Start periodic flush for group commit policies
    if (this.syncPolicy !== SyncPolicy.SYNC_EVERY_WRITE) {
      const interval = this.syncPolicy === SyncPolicy.GROUP_COMMIT_100MS ? 100 : 10;
      this.flushTimer = setInterval(() => this.flushBatch(), interval);
    }
  }
  
  /**
   * Append entry to WAL
   * 
   * Strategy Pattern: Different sync policies use different write strategies:
   * - SYNC_EVERY_WRITE: Immediate write + sync (highest durability)
   * - GROUP_COMMIT: Batch writes, sync periodically (balanced)
   */
  async append(entry: Omit<LogEntry, 'sequenceId' | 'timestamp'>): Promise<void> {
    const logEntry: LogEntry = {
      ...entry,
      sequenceId: this.sequenceId++,
      timestamp: Date.now(),
    };
    
    if (this.syncPolicy === SyncPolicy.SYNC_EVERY_WRITE) {
      // Immediate durability: write and sync
      await this.writeEntry(logEntry);
      if (!this.fileHandle) {
        throw new Error('WAL file handle not open');
      }
      await this.fileHandle.sync();
    } else {
      // Group commit: batch writes for throughput
      return new Promise((resolve, reject) => {
        this.pendingWrites.push({ entry: logEntry, resolve, reject });
        
        // Flush immediately if batch is large enough
        if (this.pendingWrites.length >= 100) {
          this.flushBatch();
        }
      });
    }
  }
  
  /**
   * Flush batch of pending writes
   * 
   * Design: Single fsync for entire batch provides 10-50x throughput
   * improvement vs syncing each write individually.
   */
  private async flushBatch(): Promise<void> {
    if (this.pendingWrites.length === 0) return;
    
    const batch = this.pendingWrites;
    this.pendingWrites = [];
    
    try {
      // Write all entries to file
      for (const { entry } of batch) {
        await this.writeEntry(entry);
      }
      
      // Single fsync for entire batch (key optimization)
      if (!this.fileHandle) {
        throw new Error('WAL file handle not open');
      }
      await this.fileHandle.sync();
      
      // Resolve all promises
      for (const { resolve } of batch) {
        resolve();
      }
    } catch (err) {
      // Reject all promises on error
      for (const { reject } of batch) {
        reject(err as Error);
      }
      throw err;
    }
  }
  
  /**
   * Write single entry to file
   */
  private async writeEntry(entry: LogEntry): Promise<void> {
    if (!this.fileHandle) {
      throw new Error('WAL file handle not open');
    }
    const buffer = this.serializeEntry(entry);
    await this.fileHandle.write(buffer);
  }
  
  /**
   * Replay all entries from WAL (crash recovery)
   * 
   * Design: Reads all log files in order and returns entries. Stops at
   * first checksum failure (corrupted tail) to prevent reading garbage.
   */
  async replay(): Promise<LogEntry[]> {
    const entries: LogEntry[] = [];
    const files = await fs.readdir(this.logDir);
    const logFiles = files.filter(f => f.endsWith('.log')).sort();
    
    for (const file of logFiles) {
      const filePath = path.join(this.logDir, file);
      const fileEntries = await this.readLogFile(filePath);
      entries.push(...fileEntries);
    }
    
    return entries;
  }
  
  /**
   * Read all entries from a log file
   * 
   * Error Handling: Stops at first corruption (checksum failure) to prevent
   * reading garbage data. This handles torn writes gracefully.
   */
  private async readLogFile(filePath: string): Promise<LogEntry[]> {
    const entries: LogEntry[] = [];
    const fd = await fs.open(filePath, 'r');
    
    try {
      const stats = await fd.stat();
      let offset = 0;
      
      while (offset < stats.size) {
        // Read length field
        const lengthBuffer = Buffer.allocUnsafe(4);
        const { bytesRead } = await fd.read(lengthBuffer, 0, 4, offset);
        if (bytesRead < 4) break; // Incomplete entry
        
        const length = lengthBuffer.readUInt32BE(0);
        offset += 4;
        
        // Validate length (prevent reading beyond file)
        if (offset + length > stats.size) {
          console.warn(`Corrupted entry at offset ${offset - 4}: length ${length} exceeds file size`);
          break;
        }
        
        // Read entry data
        const entryBuffer = Buffer.allocUnsafe(length);
        await fd.read(entryBuffer, 0, length, offset);
        offset += length;
        
        // Deserialize and validate checksum
        try {
          const entry = this.deserializeEntry(entryBuffer);
          if (this.validateChecksum(entry, entryBuffer)) {
            entries.push(entry);
          } else {
            // Checksum failure - stop reading (corrupted tail)
            console.warn(`Checksum failure at offset ${offset - length - 4}, stopping replay`);
            break;
          }
        } catch (err) {
          console.warn(`Failed to deserialize entry at offset ${offset - length - 4}: ${err}`);
          break;
        }
      }
    } finally {
      await fd.close();
    }
    
    return entries;
  }
  
  /**
   * Checkpoint - delete old WAL files after successful flush
   * 
   * Design: Called after MemTable flush to SSTable. Old WAL files can be
   * safely deleted since data is now persisted in SSTables.
   */
  async checkpoint(): Promise<void> {
    // Flush any pending writes first
    if (this.pendingWrites.length > 0) {
      await this.flushBatch();
    }
    
    if (this.fileHandle) {
      await this.fileHandle.close();
      this.fileHandle = null;
    }
    
    // Delete old log files (data now in SSTables)
    const files = await fs.readdir(this.logDir);
    const currentBasename = this.currentLogFile ? path.basename(this.currentLogFile) : '';
    for (const file of files) {
      if (file.endsWith('.log') && file !== currentBasename) {
        await fs.unlink(path.join(this.logDir, file));
      }
    }
    
    // Start new log file
    await this.rotateLog();
    if (!this.currentLogFile) {
      throw new Error('Failed to rotate log file');
    }
    this.fileHandle = await fs.open(this.currentLogFile, 'a');
  }
  
  /**
   * Close WAL gracefully
   */
  async close(): Promise<void> {
    // Flush any pending writes
    if (this.pendingWrites.length > 0) {
      await this.flushBatch();
    }
    
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
    
    if (this.fileHandle) {
      await this.fileHandle.close();
      this.fileHandle = null;
    }
  }
  
  /**
   * Rotate to new log file
   */
  private async rotateLog(): Promise<void> {
    const timestamp = Date.now();
    this.currentLogFile = path.join(this.logDir, `wal-${timestamp}.log`);
  }
  
  /**
   * Serialize log entry to binary format
   * 
   * Format: [length: 4][checksum: 4][sequenceId: 8][timestamp: 8][op: 1][payload: N]
   */
  private serializeEntry(entry: LogEntry): Buffer {
    const payload = this.serializePayload(entry);
    const entrySize = 4 + 8 + 8 + 1 + payload.length; // checksum + seq + ts + op + payload
    const buffer = Buffer.allocUnsafe(4 + entrySize); // length + entry
    
    let offset = 0;
    
    // Write length (entry size without length field itself)
    buffer.writeUInt32BE(entrySize, offset);
    offset += 4;
    
    // Placeholder for checksum (will calculate after)
    const checksumOffset = offset;
    offset += 4;
    
    // Sequence ID
    buffer.writeBigUInt64BE(BigInt(entry.sequenceId), offset);
    offset += 8;
    
    // Timestamp
    buffer.writeBigUInt64BE(BigInt(entry.timestamp), offset);
    offset += 8;
    
    // Operation code
    buffer.writeUInt8(this.operationToCode(entry.operation), offset);
    offset += 1;
    
    // Payload
    payload.copy(buffer, offset);
    
    // Calculate and write checksum (over entry data, not length)
    const entryData = buffer.slice(4);
    const checksum = this.calculateChecksum(entryData);
    buffer.writeUInt32BE(checksum, checksumOffset);
    
    return buffer;
  }
  
  /**
   * Serialize payload based on operation type
   */
  private serializePayload(entry: LogEntry): Buffer {
    switch (entry.operation) {
      case LogOperationType.PUT: {
        if (!entry.key || !entry.value) {
          throw new Error('PUT operation requires key and value');
        }
        const keyBuf = Buffer.from(entry.key, 'utf8');
        const valueBuf = Buffer.from(entry.value, 'utf8');
        const buffer = Buffer.allocUnsafe(2 + keyBuf.length + 4 + valueBuf.length);
        
        let offset = 0;
        buffer.writeUInt16BE(keyBuf.length, offset); offset += 2;
        keyBuf.copy(buffer, offset); offset += keyBuf.length;
        buffer.writeUInt32BE(valueBuf.length, offset); offset += 4;
        valueBuf.copy(buffer, offset);
        
        return buffer;
      }
      
      case LogOperationType.DELETE: {
        if (!entry.key) {
          throw new Error('DELETE operation requires key');
        }
        const keyBuf = Buffer.from(entry.key, 'utf8');
        const buffer = Buffer.allocUnsafe(2 + keyBuf.length);
        buffer.writeUInt16BE(keyBuf.length, 0);
        keyBuf.copy(buffer, 2);
        return buffer;
      }
      
      case LogOperationType.BATCH_PUT: {
        if (!entry.keys || !entry.values || entry.keys.length !== entry.values.length) {
          throw new Error('BATCH_PUT requires equal length keys and values arrays');
        }
        
        // Calculate total size
        let size = 4; // count
        const keyBuffers = entry.keys.map(k => Buffer.from(k, 'utf8'));
        const valueBuffers = entry.values.map(v => Buffer.from(v, 'utf8'));
        
        for (let i = 0; i < keyBuffers.length; i++) {
          const keyBuf = keyBuffers[i]!;
          const valBuf = valueBuffers[i]!;
          size += 2 + keyBuf.length + 4 + valBuf.length;
        }
        
        const buffer = Buffer.allocUnsafe(size);
        let offset = 0;
        
        buffer.writeUInt32BE(entry.keys.length, offset); offset += 4;
        
        for (let i = 0; i < keyBuffers.length; i++) {
          const keyBuf = keyBuffers[i]!;
          const valBuf = valueBuffers[i]!;
          buffer.writeUInt16BE(keyBuf.length, offset); offset += 2;
          keyBuf.copy(buffer, offset); offset += keyBuf.length;
          buffer.writeUInt32BE(valBuf.length, offset); offset += 4;
          valBuf.copy(buffer, offset); offset += valBuf.length;
        }
        
        return buffer;
      }
      
      default:
        throw new Error(`Unknown operation: ${entry.operation}`);
    }
  }
  
  /**
   * Deserialize binary to log entry
   */
  private deserializeEntry(buffer: Buffer): LogEntry {
    let offset = 0;
    
    // Skip checksum (validated separately)
    offset += 4;
    
    // Sequence ID
    const sequenceId = Number(buffer.readBigUInt64BE(offset));
    offset += 8;
    
    // Timestamp
    const timestamp = Number(buffer.readBigUInt64BE(offset));
    offset += 8;
    
    // Operation code
    const opCode = buffer.readUInt8(offset);
    offset += 1;
    const operation = this.codeToOperation(opCode);
    
    // Payload
    const payload = buffer.slice(offset);
    
    return {
      sequenceId,
      timestamp,
      operation,
      ...this.deserializePayload(operation, payload),
    };
  }
  
  /**
   * Deserialize payload based on operation type
   */
  private deserializePayload(operation: LogOperationType, payload: Buffer): Partial<LogEntry> {
    let offset = 0;
    
    switch (operation) {
      case LogOperationType.PUT: {
        const keyLen = payload.readUInt16BE(offset); offset += 2;
        const key = payload.toString('utf8', offset, offset + keyLen); offset += keyLen;
        const valueLen = payload.readUInt32BE(offset); offset += 4;
        const value = payload.toString('utf8', offset, offset + valueLen);
        return { key, value };
      }
      
      case LogOperationType.DELETE: {
        const keyLen = payload.readUInt16BE(offset); offset += 2;
        const key = payload.toString('utf8', offset, offset + keyLen);
        return { key };
      }
      
      case LogOperationType.BATCH_PUT: {
        const count = payload.readUInt32BE(offset); offset += 4;
        const keys: string[] = [];
        const values: string[] = [];
        
        for (let i = 0; i < count; i++) {
          const keyLen = payload.readUInt16BE(offset); offset += 2;
          const key = payload.toString('utf8', offset, offset + keyLen); offset += keyLen;
          keys.push(key);
          
          const valueLen = payload.readUInt32BE(offset); offset += 4;
          const value = payload.toString('utf8', offset, offset + valueLen); offset += valueLen;
          values.push(value);
        }
        
        return { keys, values };
      }
    }
  }
  
  /**
   * Calculate CRC32 checksum for corruption detection
   * 
   * Design: CRC32 provides fast checksum calculation with good error detection.
   * Used to detect torn writes and corruption.
   */
  private calculateChecksum(buffer: Buffer): number {
    let crc = 0xFFFFFFFF;
    for (let i = 0; i < buffer.length; i++) {
      const byte = buffer[i];
      if (byte === undefined) break;
      crc ^= byte;
      for (let j = 0; j < 8; j++) {
        crc = (crc >>> 1) ^ (0xEDB88320 & -(crc & 1));
      }
    }
    return ~crc >>> 0;
  }
  
  /**
   * Validate checksum of entry
   */
  private validateChecksum(entry: LogEntry, buffer: Buffer): boolean {
    const stored = buffer.readUInt32BE(0);
    const calculated = this.calculateChecksum(buffer.slice(4));
    return stored === calculated;
  }
  
  /**
   * Convert operation enum to code (for binary format)
   */
  private operationToCode(op: LogOperationType): number {
    switch (op) {
      case LogOperationType.PUT: return 1;
      case LogOperationType.DELETE: return 2;
      case LogOperationType.BATCH_PUT: return 3;
      default: throw new Error(`Unknown operation: ${op}`);
    }
  }
  
  /**
   * Convert code to operation enum
   */
  private codeToOperation(code: number): LogOperationType {
    switch (code) {
      case 1: return LogOperationType.PUT;
      case 2: return LogOperationType.DELETE;
      case 3: return LogOperationType.BATCH_PUT;
      default: throw new Error(`Unknown code: ${code}`);
    }
  }
  
  /**
   * Read last sequence ID from current log file
   * Used to continue sequence after restart
   */
  private async readLastSequenceId(): Promise<number> {
    try {
      if (!this.currentLogFile) {
        return 0;
      }
      const entries = await this.readLogFile(this.currentLogFile);
      return entries.length > 0 ? entries[entries.length - 1]!.sequenceId + 1 : 0;
    } catch {
      return 0;
    }
  }
}
