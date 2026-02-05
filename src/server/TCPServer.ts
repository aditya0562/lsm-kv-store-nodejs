import * as net from 'net';
import { ITCPServer } from './ITCPServer';
import { IStorageEngine } from '../interfaces/Storage';
import { 
  Opcode, 
  AckStatus, 
  TCPProtocolSerializer,
  TCPMessage,
} from './TCPProtocol';

export interface TCPServerConfig {
  readonly port: number;
  readonly host?: string;
}

export class TCPServer implements ITCPServer {
  private readonly store: IStorageEngine;
  private readonly config: TCPServerConfig;
  private server: net.Server | null = null;
  private connections: Set<net.Socket> = new Set();

  constructor(store: IStorageEngine, config: TCPServerConfig) {
    this.store = store;
    this.config = config;
  }

  public async start(): Promise<void> {
    if (this.server !== null) {
      throw new Error('TCPServer: Already started');
    }

    return new Promise((resolve, reject) => {
      this.server = net.createServer((socket) => this.handleConnection(socket));
      
      this.server.on('error', reject);
      
      this.server.listen(this.config.port, this.config.host ?? '0.0.0.0', () => {
        console.log(`TCPServer: Listening on port ${this.config.port}`);
        resolve();
      });
    });
  }

  public async stop(): Promise<void> {
    if (this.server === null) {
      return;
    }

    for (const socket of this.connections) {
      socket.destroy();
    }
    this.connections.clear();

    return new Promise((resolve) => {
      this.server!.close(() => {
        this.server = null;
        console.log('TCPServer: Stopped');
        resolve();
      });
    });
  }

  public getPort(): number {
    return this.config.port;
  }

  public getConnectionCount(): number {
    return this.connections.size;
  }

  private handleConnection(socket: net.Socket): void {
    this.connections.add(socket);
    
    const clientId = `${socket.remoteAddress}:${socket.remotePort}`;
    console.log(`TCPServer: Client connected - ${clientId}`);

    let buffer = Buffer.alloc(0);

    socket.on('data', async (chunk: Buffer) => {
      socket.pause();
      
      buffer = Buffer.concat([buffer, chunk]);
      
      try {
        await this.processBuffer(socket, buffer, (remaining) => {
          buffer = Buffer.from(remaining);
        });
      } catch (err) {
        const errorMsg = err instanceof Error ? err.message : 'Unknown error';
        socket.write(TCPProtocolSerializer.serializeError(errorMsg));
      }
      
      socket.resume();
    });

    socket.on('close', () => {
      this.connections.delete(socket);
      console.log(`TCPServer: Client disconnected - ${clientId}`);
    });

    socket.on('error', (err) => {
      console.error(`TCPServer: Socket error - ${clientId}:`, err.message);
      this.connections.delete(socket);
    });
  }

  private async processBuffer(
    socket: net.Socket, 
    buffer: Buffer,
    updateBuffer: (remaining: Buffer) => void
  ): Promise<void> {
    let offset = 0;

    while (offset < buffer.length) {
      const remaining = buffer.subarray(offset);
      const result = TCPProtocolSerializer.parseMessage(remaining);
      
      if (result === null) {
        break;
      }

      await this.handleMessage(socket, result.message);
      offset += result.bytesConsumed;
    }

    updateBuffer(buffer.subarray(offset));
  }

  private async handleMessage(socket: net.Socket, message: TCPMessage): Promise<void> {
    switch (message.opcode) {
      case Opcode.STREAM_PUT:
        await this.handleStreamPut(socket, message.key, message.value);
        break;
        
      case Opcode.END_STREAM:
        this.handleEndStream(socket);
        break;
        
      default:
        socket.write(TCPProtocolSerializer.serializeError(`Unexpected opcode: ${message.opcode}`));
    }
  }

  private async handleStreamPut(socket: net.Socket, key: string, value: string): Promise<void> {
    try {
      await this.store.put(key, value);
      socket.write(TCPProtocolSerializer.serializeAck(AckStatus.OK));
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Write failed';
      socket.write(TCPProtocolSerializer.serializeAck(AckStatus.ERROR, errorMsg));
    }
  }

  private handleEndStream(socket: net.Socket): void {
    socket.write(TCPProtocolSerializer.serializeAck(AckStatus.OK));
    socket.end();
  }
}
