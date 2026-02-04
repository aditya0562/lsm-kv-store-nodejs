/**
 * WAL (Write-Ahead Log) entry types.
 * 
 * Design: WAL entries represent all mutations to the store. This enables
 * crash recovery by replaying entries on startup. The sequence ID ensures
 * ordering and helps with future replication support.
 */

export enum LogOperationType {
  PUT = 'PUT',
  DELETE = 'DELETE',
  BATCH_PUT = 'BATCH_PUT',
}

export interface LogEntry {
  sequenceId: number;
  timestamp: number;
  operation: LogOperationType;
  key?: string;
  value?: string;
  keys?: string[];
  values?: string[];
}
