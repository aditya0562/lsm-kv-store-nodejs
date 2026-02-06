import * as net from 'net';
import { LogEntry } from '../storage/wal/LogEntry';
import { IReplicationServer } from './IReplicationServer';
import { ReplicationState, ReplicationMetrics, ReplicationRole } from './ReplicationTypes';
import {
  ReplicationOpcode,
  ReplicationAckStatus,
  ReplicationProtocolSerializer,
  ReplicateMessage,
} from './ReplicationProtocol';

export interface ReplicationServerConfig {
  readonly port: number;
  readonly host?: string;
}

export type ReplicationEntryHandler = (entry: LogEntry) => Promise<void>;

export class ReplicationServer implements IReplicationServer {
  private readonly config: ReplicationServerConfig;
  private readonly entryHandler: ReplicationEntryHandler;

  private server: net.Server | null = null;
  private primarySocket: net.Socket | null = null;
  private lastAppliedSequence: number = -1;

  private metrics: ReplicationMetrics = {
    entriesReplicated: 0,
    bytesReplicated: 0,
    failedAttempts: 0,
    lastSuccessTime: null,
    lastFailureTime: null,
  };

  constructor(config: ReplicationServerConfig, entryHandler: ReplicationEntryHandler) {
    this.config = config;
    this.entryHandler = entryHandler;
  }

  public async start(): Promise<void> {
    if (this.server !== null) {
      throw new Error('ReplicationServer: Already started');
    }

    return new Promise((resolve, reject) => {
      this.server = net.createServer((socket) => this.handleConnection(socket));

      this.server.on('error', reject);

      this.server.listen(this.config.port, this.config.host ?? '0.0.0.0', () => {
        console.log(`ReplicationServer: Listening on port ${this.config.port}`);
        resolve();
      });
    });
  }

  public async stop(): Promise<void> {
    if (this.server === null) {
      return;
    }

    if (this.primarySocket) {
      this.primarySocket.destroy();
      this.primarySocket = null;
    }

    return new Promise((resolve) => {
      this.server!.close(() => {
        this.server = null;
        console.log('ReplicationServer: Stopped');
        resolve();
      });
    });
  }

  public getState(): ReplicationState {
    return {
      role: ReplicationRole.BACKUP,
      connected: this.primarySocket !== null,
      lastReplicatedSequence: this.lastAppliedSequence,
      pendingEntries: 0,
      lagMs: this.calculateLag(),
    };
  }

  public getMetrics(): ReplicationMetrics {
    return { ...this.metrics };
  }

  private handleConnection(socket: net.Socket): void {
    const clientId = `${socket.remoteAddress}:${socket.remotePort}`;

    if (this.primarySocket !== null) {
      console.warn(`ReplicationServer: Rejecting connection from ${clientId}, primary already connected`);
      socket.destroy();
      return;
    }

    this.primarySocket = socket;
    console.log(`ReplicationServer: Primary connected - ${clientId}`);

    let buffer = Buffer.alloc(0);

    socket.on('data', async (chunk: Buffer) => {
      socket.pause();

      buffer = Buffer.concat([buffer, chunk]);

      try {
        const remaining = await this.processBuffer(socket, buffer);
        buffer = Buffer.from(remaining);
      } catch (err) {
        console.error(`ReplicationServer: Error processing data: ${(err as Error).message}`);
      }

      socket.resume();
    });

    socket.on('close', () => {
      console.log(`ReplicationServer: Primary disconnected - ${clientId}`);
      this.primarySocket = null;
    });

    socket.on('error', (err) => {
      console.error(`ReplicationServer: Socket error - ${clientId}: ${err.message}`);
      this.primarySocket = null;
    });
  }

  private async processBuffer(socket: net.Socket, buffer: Buffer): Promise<Buffer> {
    let offset = 0;

    while (offset < buffer.length) {
      const remaining = buffer.subarray(offset);
      const result = ReplicationProtocolSerializer.parseMessage(remaining);

      if (result === null) {
        break;
      }

      if (result.message.opcode === ReplicationOpcode.REPLICATE) {
        await this.handleReplicate(socket, result.message);
      }

      offset += result.bytesConsumed;
    }

    return buffer.subarray(offset);
  }

  private async handleReplicate(socket: net.Socket, message: ReplicateMessage): Promise<void> {
    const { entry } = message;

    try {
      await this.entryHandler(entry);

      this.lastAppliedSequence = entry.sequenceId;
      this.updateMetricsSuccess();

      const ack = ReplicationProtocolSerializer.serializeAck(
        ReplicationAckStatus.OK,
        entry.sequenceId
      );
      socket.write(ack);
    } catch (err) {
      this.updateMetricsFailure();
      console.error(`ReplicationServer: Failed to apply entry ${entry.sequenceId}: ${(err as Error).message}`);

      const ack = ReplicationProtocolSerializer.serializeAck(
        ReplicationAckStatus.ERROR,
        entry.sequenceId
      );
      socket.write(ack);
    }
  }

  private updateMetricsSuccess(): void {
    this.metrics = {
      ...this.metrics,
      entriesReplicated: this.metrics.entriesReplicated + 1,
      lastSuccessTime: Date.now(),
    };
  }

  private updateMetricsFailure(): void {
    this.metrics = {
      ...this.metrics,
      failedAttempts: this.metrics.failedAttempts + 1,
      lastFailureTime: Date.now(),
    };
  }

  private calculateLag(): number {
    if (this.metrics.lastSuccessTime === null) {
      return 0;
    }
    return Date.now() - this.metrics.lastSuccessTime;
  }
}
