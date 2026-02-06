import { ReplicationState, ReplicationMetrics } from './ReplicationTypes';

export interface IReplicationServer {
  start(): Promise<void>;
  stop(): Promise<void>;
  getState(): ReplicationState;
  getMetrics(): ReplicationMetrics;
}
