export enum Opcode {
  STREAM_PUT = 0x01,
  ACK = 0x02,
  END_STREAM = 0x03,
  ERROR = 0x04,
}

export enum AckStatus {
  OK = 0x00,
  ERROR = 0x01,
}

export interface StreamPutMessage {
  readonly opcode: Opcode.STREAM_PUT;
  readonly key: string;
  readonly value: string;
}

export interface AckMessage {
  readonly opcode: Opcode.ACK;
  readonly status: AckStatus;
  readonly message?: string | undefined;
}

export interface EndStreamMessage {
  readonly opcode: Opcode.END_STREAM;
}

export interface ErrorMessage {
  readonly opcode: Opcode.ERROR;
  readonly message: string;
}

export type TCPMessage = StreamPutMessage | AckMessage | EndStreamMessage | ErrorMessage;

export class TCPProtocolSerializer {
  
  public static serializeStreamPut(key: string, value: string): Buffer {
    const keyBuf = Buffer.from(key, 'utf8');
    const valueBuf = Buffer.from(value, 'utf8');
    
    const payloadSize = 1 + 2 + keyBuf.length + 4 + valueBuf.length;
    const buffer = Buffer.allocUnsafe(4 + payloadSize);
    
    let offset = 0;
    buffer.writeUInt32BE(payloadSize, offset);
    offset += 4;
    
    buffer.writeUInt8(Opcode.STREAM_PUT, offset);
    offset += 1;
    
    buffer.writeUInt16BE(keyBuf.length, offset);
    offset += 2;
    
    keyBuf.copy(buffer, offset);
    offset += keyBuf.length;
    
    buffer.writeUInt32BE(valueBuf.length, offset);
    offset += 4;
    
    valueBuf.copy(buffer, offset);
    
    return buffer;
  }

  public static serializeAck(status: AckStatus, message?: string): Buffer {
    const msgBuf = message ? Buffer.from(message, 'utf8') : Buffer.alloc(0);
    const payloadSize = 1 + 1 + (message ? 2 + msgBuf.length : 0);
    const buffer = Buffer.allocUnsafe(4 + payloadSize);
    
    let offset = 0;
    buffer.writeUInt32BE(payloadSize, offset);
    offset += 4;
    
    buffer.writeUInt8(Opcode.ACK, offset);
    offset += 1;
    
    buffer.writeUInt8(status, offset);
    offset += 1;
    
    if (message) {
      buffer.writeUInt16BE(msgBuf.length, offset);
      offset += 2;
      msgBuf.copy(buffer, offset);
    }
    
    return buffer;
  }

  public static serializeEndStream(): Buffer {
    const buffer = Buffer.allocUnsafe(5);
    buffer.writeUInt32BE(1, 0);
    buffer.writeUInt8(Opcode.END_STREAM, 4);
    return buffer;
  }

  public static serializeError(message: string): Buffer {
    const msgBuf = Buffer.from(message, 'utf8');
    const payloadSize = 1 + 2 + msgBuf.length;
    const buffer = Buffer.allocUnsafe(4 + payloadSize);
    
    let offset = 0;
    buffer.writeUInt32BE(payloadSize, offset);
    offset += 4;
    
    buffer.writeUInt8(Opcode.ERROR, offset);
    offset += 1;
    
    buffer.writeUInt16BE(msgBuf.length, offset);
    offset += 2;
    
    msgBuf.copy(buffer, offset);
    
    return buffer;
  }

  public static parseMessage(buffer: Buffer): { message: TCPMessage; bytesConsumed: number } | null {
    if (buffer.length < 5) {
      return null;
    }
    
    const length = buffer.readUInt32BE(0);
    const totalSize = 4 + length;
    
    if (buffer.length < totalSize) {
      return null;
    }
    
    const opcode = buffer.readUInt8(4);
    
    switch (opcode) {
      case Opcode.STREAM_PUT:
        return this.parseStreamPut(buffer, length);
      case Opcode.ACK:
        return this.parseAck(buffer, length);
      case Opcode.END_STREAM:
        return { message: { opcode: Opcode.END_STREAM }, bytesConsumed: totalSize };
      case Opcode.ERROR:
        return this.parseError(buffer, length);
      default:
        throw new Error(`Unknown opcode: ${opcode}`);
    }
  }

  private static parseStreamPut(buffer: Buffer, length: number): { message: StreamPutMessage; bytesConsumed: number } {
    let offset = 5;
    
    const keyLen = buffer.readUInt16BE(offset);
    offset += 2;
    
    const key = buffer.toString('utf8', offset, offset + keyLen);
    offset += keyLen;
    
    const valueLen = buffer.readUInt32BE(offset);
    offset += 4;
    
    const value = buffer.toString('utf8', offset, offset + valueLen);
    
    return {
      message: { opcode: Opcode.STREAM_PUT, key, value },
      bytesConsumed: 4 + length,
    };
  }

  private static parseAck(buffer: Buffer, length: number): { message: AckMessage; bytesConsumed: number } {
    const status = buffer.readUInt8(5) as AckStatus;
    
    let message: string | undefined;
    if (length > 2) {
      const msgLen = buffer.readUInt16BE(6);
      message = buffer.toString('utf8', 8, 8 + msgLen);
    }
    
    return {
      message: { opcode: Opcode.ACK, status, message },
      bytesConsumed: 4 + length,
    };
  }

  private static parseError(buffer: Buffer, length: number): { message: ErrorMessage; bytesConsumed: number } {
    const msgLen = buffer.readUInt16BE(5);
    const message = buffer.toString('utf8', 7, 7 + msgLen);
    
    return {
      message: { opcode: Opcode.ERROR, message },
      bytesConsumed: 4 + length,
    };
  }
}
