import { LogEntry } from '../storage/wal/LogEntry';
import { ReplicationState, ReplicationMetrics } from './ReplicationTypes';

export interface IReplicationManager {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  replicate(entry: LogEntry): void;
  isConnected(): boolean;
  getState(): ReplicationState;
  getMetrics(): ReplicationMetrics;
}
