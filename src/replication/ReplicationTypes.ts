export enum ReplicationRole {
  STANDALONE = 'standalone',
  PRIMARY = 'primary',
  BACKUP = 'backup',
}

export interface ReplicationConfig {
  readonly role: ReplicationRole;
  readonly backupHost?: string;
  readonly backupPort?: number;
  readonly replicationPort?: number;
  readonly connectionTimeoutMs?: number;
  readonly reconnectIntervalMs?: number;
}

export interface ReplicationState {
  readonly role: ReplicationRole;
  readonly connected: boolean;
  readonly lastReplicatedSequence: number;
  readonly pendingEntries: number;
  readonly lagMs: number;
}

export interface ReplicationMetrics {
  readonly entriesReplicated: number;
  readonly bytesReplicated: number;
  readonly failedAttempts: number;
  readonly lastSuccessTime: number | null;
  readonly lastFailureTime: number | null;
}

export const DEFAULT_CONNECTION_TIMEOUT_MS = 5000;
export const DEFAULT_RECONNECT_INTERVAL_MS = 3000;

export function resolveReplicationConfig(
  config?: Partial<ReplicationConfig>
): ReplicationConfig | undefined {
  if (!config || !config.role || config.role === ReplicationRole.STANDALONE) {
    return undefined;
  }

  const resolved: ReplicationConfig = {
    role: config.role,
    connectionTimeoutMs: config.connectionTimeoutMs ?? DEFAULT_CONNECTION_TIMEOUT_MS,
    reconnectIntervalMs: config.reconnectIntervalMs ?? DEFAULT_RECONNECT_INTERVAL_MS,
  };

  if (config.backupHost !== undefined) {
    (resolved as { backupHost: string }).backupHost = config.backupHost;
  }
  if (config.backupPort !== undefined) {
    (resolved as { backupPort: number }).backupPort = config.backupPort;
  }
  if (config.replicationPort !== undefined) {
    (resolved as { replicationPort: number }).replicationPort = config.replicationPort;
  }

  return resolved;
}
