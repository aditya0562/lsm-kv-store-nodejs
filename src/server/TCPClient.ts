import * as net from 'net';
import { 
  Opcode,
  AckStatus,
  TCPProtocolSerializer,
  AckMessage,
} from './TCPProtocol';

export interface TCPClientConfig {
  readonly host: string;
  readonly port: number;
  readonly timeout?: number;
}

export class TCPClient {
  private readonly config: TCPClientConfig;
  private socket: net.Socket | null = null;
  private connected: boolean = false;
  private buffer: Buffer = Buffer.alloc(0);
  private pendingAcks: Array<{
    resolve: (ack: AckMessage) => void;
    reject: (err: Error) => void;
  }> = [];

  constructor(config: TCPClientConfig) {
    this.config = {
      timeout: 5000,
      ...config,
    };
  }

  public async connect(): Promise<void> {
    if (this.connected) {
      throw new Error('TCPClient: Already connected');
    }

    return new Promise((resolve, reject) => {
      this.socket = new net.Socket();
      
      this.socket.setTimeout(this.config.timeout!);
      
      this.socket.on('connect', () => {
        this.connected = true;
        resolve();
      });

      this.socket.on('data', (chunk: Buffer) => {
        this.handleData(chunk);
      });

      this.socket.on('error', (err) => {
        this.rejectAllPending(err);
        reject(err);
      });

      this.socket.on('close', () => {
        this.connected = false;
        this.rejectAllPending(new Error('Connection closed'));
      });

      this.socket.on('timeout', () => {
        this.socket?.destroy();
        this.rejectAllPending(new Error('Connection timeout'));
      });

      this.socket.connect(this.config.port, this.config.host);
    });
  }

  public async disconnect(): Promise<void> {
    if (!this.connected || !this.socket) {
      return;
    }

    return new Promise((resolve) => {
      this.socket!.end(() => {
        this.connected = false;
        this.socket = null;
        resolve();
      });
    });
  }

  public isConnected(): boolean {
    return this.connected;
  }

  public async put(key: string, value: string): Promise<void> {
    this.ensureConnected();
    
    const message = TCPProtocolSerializer.serializeStreamPut(key, value);
    this.socket!.write(message);
    
    const ack = await this.waitForAck();
    
    if (ack.status !== AckStatus.OK) {
      throw new Error(ack.message ?? 'Write failed');
    }
  }

  public async streamPut(entries: Array<{ key: string; value: string }>): Promise<number> {
    this.ensureConnected();
    
    let successCount = 0;
    
    for (const { key, value } of entries) {
      await this.put(key, value);
      successCount++;
    }
    
    return successCount;
  }

  public async endStream(): Promise<void> {
    this.ensureConnected();
    
    const message = TCPProtocolSerializer.serializeEndStream();
    this.socket!.write(message);
    
    await this.waitForAck();
    await this.disconnect();
  }

  private handleData(chunk: Buffer): void {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    
    while (this.buffer.length >= 5) {
      const result = TCPProtocolSerializer.parseMessage(this.buffer);
      
      if (result === null) {
        break;
      }

      if (result.message.opcode === Opcode.ACK) {
        const pending = this.pendingAcks.shift();
        if (pending) {
          pending.resolve(result.message);
        }
      } else if (result.message.opcode === Opcode.ERROR) {
        const pending = this.pendingAcks.shift();
        if (pending) {
          pending.reject(new Error(result.message.message));
        }
      }

      this.buffer = this.buffer.subarray(result.bytesConsumed);
    }
  }

  private waitForAck(): Promise<AckMessage> {
    return new Promise((resolve, reject) => {
      this.pendingAcks.push({ resolve, reject });
    });
  }

  private rejectAllPending(err: Error): void {
    for (const pending of this.pendingAcks) {
      pending.reject(err);
    }
    this.pendingAcks = [];
  }

  private ensureConnected(): void {
    if (!this.connected || !this.socket) {
      throw new Error('TCPClient: Not connected');
    }
  }
}
