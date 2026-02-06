import { LogEntry, LogOperationType } from '../storage/wal/LogEntry';

export enum ReplicationOpcode {
  REPLICATE = 0x10,
  REPLICATE_ACK = 0x11,
}

export enum ReplicationAckStatus {
  OK = 0x00,
  ERROR = 0x01,
}

export interface ReplicateMessage {
  readonly opcode: ReplicationOpcode.REPLICATE;
  readonly entry: LogEntry;
}

export interface ReplicateAckMessage {
  readonly opcode: ReplicationOpcode.REPLICATE_ACK;
  readonly status: ReplicationAckStatus;
  readonly sequenceId: number;
}

export type ReplicationMessage = ReplicateMessage | ReplicateAckMessage;

export class ReplicationProtocolSerializer {

  public static serializeReplicate(entry: LogEntry): Buffer {
    const entryBuffer = this.serializeLogEntry(entry);
    const payloadSize = 1 + entryBuffer.length;
    const buffer = Buffer.allocUnsafe(4 + payloadSize);

    let offset = 0;
    buffer.writeUInt32BE(payloadSize, offset);
    offset += 4;

    buffer.writeUInt8(ReplicationOpcode.REPLICATE, offset);
    offset += 1;

    entryBuffer.copy(buffer, offset);

    return buffer;
  }

  public static serializeAck(status: ReplicationAckStatus, sequenceId: number): Buffer {
    const payloadSize = 1 + 1 + 8;
    const buffer = Buffer.allocUnsafe(4 + payloadSize);

    let offset = 0;
    buffer.writeUInt32BE(payloadSize, offset);
    offset += 4;

    buffer.writeUInt8(ReplicationOpcode.REPLICATE_ACK, offset);
    offset += 1;

    buffer.writeUInt8(status, offset);
    offset += 1;

    buffer.writeBigUInt64BE(BigInt(sequenceId), offset);

    return buffer;
  }

  public static parseMessage(buffer: Buffer): { message: ReplicationMessage; bytesConsumed: number } | null {
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
      case ReplicationOpcode.REPLICATE:
        return this.parseReplicate(buffer, length);
      case ReplicationOpcode.REPLICATE_ACK:
        return this.parseAck(buffer, length);
      default:
        throw new Error(`Unknown replication opcode: ${opcode}`);
    }
  }

  private static parseReplicate(
    buffer: Buffer,
    length: number
  ): { message: ReplicateMessage; bytesConsumed: number } {
    const entryBuffer = buffer.subarray(5, 4 + length);
    const entry = this.deserializeLogEntry(entryBuffer);

    return {
      message: { opcode: ReplicationOpcode.REPLICATE, entry },
      bytesConsumed: 4 + length,
    };
  }

  private static parseAck(
    buffer: Buffer,
    length: number
  ): { message: ReplicateAckMessage; bytesConsumed: number } {
    const status = buffer.readUInt8(5) as ReplicationAckStatus;
    const sequenceId = Number(buffer.readBigUInt64BE(6));

    return {
      message: { opcode: ReplicationOpcode.REPLICATE_ACK, status, sequenceId },
      bytesConsumed: 4 + length,
    };
  }

  private static serializeLogEntry(entry: LogEntry): Buffer {
    const payload = this.serializePayload(entry);
    const size = 8 + 8 + 1 + payload.length;
    const buffer = Buffer.allocUnsafe(size);

    let offset = 0;

    buffer.writeBigUInt64BE(BigInt(entry.sequenceId), offset);
    offset += 8;

    buffer.writeBigUInt64BE(BigInt(entry.timestamp), offset);
    offset += 8;

    buffer.writeUInt8(this.operationToCode(entry.operation), offset);
    offset += 1;

    payload.copy(buffer, offset);

    return buffer;
  }

  private static deserializeLogEntry(buffer: Buffer): LogEntry {
    let offset = 0;

    const sequenceId = Number(buffer.readBigUInt64BE(offset));
    offset += 8;

    const timestamp = Number(buffer.readBigUInt64BE(offset));
    offset += 8;

    const opCode = buffer.readUInt8(offset);
    offset += 1;

    const operation = this.codeToOperation(opCode);
    const payload = buffer.subarray(offset);

    return {
      sequenceId,
      timestamp,
      operation,
      ...this.deserializePayload(operation, payload),
    };
  }

  private static serializePayload(entry: LogEntry): Buffer {
    switch (entry.operation) {
      case LogOperationType.PUT: {
        const keyBuf = Buffer.from(entry.key ?? '', 'utf8');
        const valueBuf = Buffer.from(entry.value ?? '', 'utf8');
        const buffer = Buffer.allocUnsafe(2 + keyBuf.length + 4 + valueBuf.length);

        let offset = 0;
        buffer.writeUInt16BE(keyBuf.length, offset);
        offset += 2;
        keyBuf.copy(buffer, offset);
        offset += keyBuf.length;
        buffer.writeUInt32BE(valueBuf.length, offset);
        offset += 4;
        valueBuf.copy(buffer, offset);

        return buffer;
      }

      case LogOperationType.DELETE: {
        const keyBuf = Buffer.from(entry.key ?? '', 'utf8');
        const buffer = Buffer.allocUnsafe(2 + keyBuf.length);
        buffer.writeUInt16BE(keyBuf.length, 0);
        keyBuf.copy(buffer, 2);
        return buffer;
      }

      case LogOperationType.BATCH_PUT: {
        const keys = entry.keys ?? [];
        const values = entry.values ?? [];
        const keyBuffers = keys.map(k => Buffer.from(k, 'utf8'));
        const valueBuffers = values.map(v => Buffer.from(v, 'utf8'));

        let size = 4;
        for (let i = 0; i < keyBuffers.length; i++) {
          size += 2 + keyBuffers[i]!.length + 4 + valueBuffers[i]!.length;
        }

        const buffer = Buffer.allocUnsafe(size);
        let offset = 0;

        buffer.writeUInt32BE(keys.length, offset);
        offset += 4;

        for (let i = 0; i < keyBuffers.length; i++) {
          const keyBuf = keyBuffers[i]!;
          const valBuf = valueBuffers[i]!;
          buffer.writeUInt16BE(keyBuf.length, offset);
          offset += 2;
          keyBuf.copy(buffer, offset);
          offset += keyBuf.length;
          buffer.writeUInt32BE(valBuf.length, offset);
          offset += 4;
          valBuf.copy(buffer, offset);
          offset += valBuf.length;
        }

        return buffer;
      }

      default:
        throw new Error(`Unknown operation: ${entry.operation}`);
    }
  }

  private static deserializePayload(
    operation: LogOperationType,
    payload: Buffer
  ): Partial<LogEntry> {
    let offset = 0;

    switch (operation) {
      case LogOperationType.PUT: {
        const keyLen = payload.readUInt16BE(offset);
        offset += 2;
        const key = payload.toString('utf8', offset, offset + keyLen);
        offset += keyLen;
        const valueLen = payload.readUInt32BE(offset);
        offset += 4;
        const value = payload.toString('utf8', offset, offset + valueLen);
        return { key, value };
      }

      case LogOperationType.DELETE: {
        const keyLen = payload.readUInt16BE(offset);
        offset += 2;
        const key = payload.toString('utf8', offset, offset + keyLen);
        return { key };
      }

      case LogOperationType.BATCH_PUT: {
        const count = payload.readUInt32BE(offset);
        offset += 4;
        const keys: string[] = [];
        const values: string[] = [];

        for (let i = 0; i < count; i++) {
          const keyLen = payload.readUInt16BE(offset);
          offset += 2;
          const key = payload.toString('utf8', offset, offset + keyLen);
          offset += keyLen;
          keys.push(key);

          const valueLen = payload.readUInt32BE(offset);
          offset += 4;
          const value = payload.toString('utf8', offset, offset + valueLen);
          offset += valueLen;
          values.push(value);
        }

        return { keys, values };
      }

      default:
        return {};
    }
  }

  private static operationToCode(op: LogOperationType): number {
    switch (op) {
      case LogOperationType.PUT:
        return 1;
      case LogOperationType.DELETE:
        return 2;
      case LogOperationType.BATCH_PUT:
        return 3;
      default:
        throw new Error(`Unknown operation: ${op}`);
    }
  }

  private static codeToOperation(code: number): LogOperationType {
    switch (code) {
      case 1:
        return LogOperationType.PUT;
      case 2:
        return LogOperationType.DELETE;
      case 3:
        return LogOperationType.BATCH_PUT;
      default:
        throw new Error(`Unknown code: ${code}`);
    }
  }
}
