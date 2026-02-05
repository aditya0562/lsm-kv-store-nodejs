
import { SSTableEntry, IndexEntry, SSTABLE_MAGIC, SSTABLE_VERSION } from './SSTableTypes';

export class SSTableSerializer {
  
  public static calculateEntrySize(entry: SSTableEntry): number {
    const keyBytes = Buffer.byteLength(entry.key, 'utf8');
    const valueBytes = Buffer.byteLength(entry.value, 'utf8');
    return 2 + keyBytes + 4 + valueBytes + 8 + 1;
  }

  public static serializeEntry(entry: SSTableEntry): Buffer {
    const keyBuf = Buffer.from(entry.key, 'utf8');
    const valueBuf = Buffer.from(entry.value, 'utf8');
    
    const totalSize = 2 + keyBuf.length + 4 + valueBuf.length + 8 + 1;
    const buffer = Buffer.allocUnsafe(totalSize);
    
    let offset = 0;
    
    buffer.writeUInt16BE(keyBuf.length, offset);
    offset += 2;
    
    keyBuf.copy(buffer, offset);
    offset += keyBuf.length;
    
    buffer.writeUInt32BE(valueBuf.length, offset);
    offset += 4;
    
    valueBuf.copy(buffer, offset);
    offset += valueBuf.length;
    
    buffer.writeBigUInt64BE(BigInt(entry.timestamp), offset);
    offset += 8;
    
    buffer.writeUInt8(entry.deleted ? 1 : 0, offset);
    
    return buffer;
  }

  public static deserializeEntry(buffer: Buffer, startOffset: number = 0): [SSTableEntry, number] {
    let offset = startOffset;
    
    const keyLen = buffer.readUInt16BE(offset);
    offset += 2;
    
    const key = buffer.toString('utf8', offset, offset + keyLen);
    offset += keyLen;
    
    const valueLen = buffer.readUInt32BE(offset);
    offset += 4;
  
    const value = buffer.toString('utf8', offset, offset + valueLen);
    offset += valueLen;
    
    const timestamp = Number(buffer.readBigUInt64BE(offset));
    offset += 8;
    
    const deleted = buffer.readUInt8(offset) === 1;
    offset += 1;
    
    const entry: SSTableEntry = { key, value, timestamp, deleted };
    const bytesRead = offset - startOffset;
    
    return [entry, bytesRead];
  }

  public static serializeIndexEntry(indexEntry: IndexEntry): Buffer {
    const keyBuf = Buffer.from(indexEntry.key, 'utf8');
    const buffer = Buffer.allocUnsafe(2 + keyBuf.length + 8);
    
    let offset = 0;
    
    buffer.writeUInt16BE(keyBuf.length, offset);
    offset += 2;
    
    keyBuf.copy(buffer, offset);
    offset += keyBuf.length;
    
    buffer.writeBigUInt64BE(BigInt(indexEntry.offset), offset);
    
    return buffer;
  }

  public static deserializeIndexEntry(buffer: Buffer, startOffset: number = 0): [IndexEntry, number] {
    let offset = startOffset;
    
    const keyLen = buffer.readUInt16BE(offset);
    offset += 2;
    
    const key = buffer.toString('utf8', offset, offset + keyLen);
    offset += keyLen;
    
    const entryOffset = Number(buffer.readBigUInt64BE(offset));
    offset += 8;
    
    const indexEntry: IndexEntry = { key, offset: entryOffset };
    const bytesRead = offset - startOffset;
    
    return [indexEntry, bytesRead];
  }

  /**
   * Serialize the SSTable footer.
   * 
   * Footer format (can be read by knowing footerSize at end):
   * [fileNumber:4][entryCount:4][dataOffset:8][indexOffset:8]
   * [firstKeyLen:2][firstKey:N][lastKeyLen:2][lastKey:M]
   * [createdAt:8][version:2][footerSize:4][magic:4]
   */
  public static serializeFooter(
    fileNumber: number,
    entryCount: number,
    dataOffset: number,
    indexOffset: number,
    firstKey: string,
    lastKey: string,
    createdAt: number
  ): Buffer {
    const firstKeyBuf = Buffer.from(firstKey, 'utf8');
    const lastKeyBuf = Buffer.from(lastKey, 'utf8');
    
    const totalSize = 46 + firstKeyBuf.length + lastKeyBuf.length;
    const buffer = Buffer.allocUnsafe(totalSize);
    
    let offset = 0;
    
    buffer.writeUInt32BE(fileNumber, offset);
    offset += 4;
    
    buffer.writeUInt32BE(entryCount, offset);
    offset += 4;
    
    buffer.writeBigUInt64BE(BigInt(dataOffset), offset);
    offset += 8;
    
    buffer.writeBigUInt64BE(BigInt(indexOffset), offset);
    offset += 8;
    
    buffer.writeUInt16BE(firstKeyBuf.length, offset);
    offset += 2;
    
    firstKeyBuf.copy(buffer, offset);
    offset += firstKeyBuf.length;
    
    buffer.writeUInt16BE(lastKeyBuf.length, offset);
    offset += 2;
    
    lastKeyBuf.copy(buffer, offset);
    offset += lastKeyBuf.length;
    
    buffer.writeBigUInt64BE(BigInt(createdAt), offset);
    offset += 8;
    
    buffer.writeUInt16BE(SSTABLE_VERSION, offset);
    offset += 2;
    
    buffer.writeUInt32BE(totalSize, offset);
    offset += 4;
    
    buffer.writeUInt32BE(SSTABLE_MAGIC, offset);
    
    return buffer;
  }

  public static calculateFooterSize(firstKey: string, lastKey: string): number {
    const firstKeyLen = Buffer.byteLength(firstKey, 'utf8');
    const lastKeyLen = Buffer.byteLength(lastKey, 'utf8');
    return 46 + firstKeyLen + lastKeyLen; // 46 = fixed fields including footerSize
  }

  public static validateMagic(buffer: Buffer): boolean {
    if (buffer.length < 4) return false;
    const magic = buffer.readUInt32BE(buffer.length - 4);
    return magic === SSTABLE_MAGIC;
  }
}
