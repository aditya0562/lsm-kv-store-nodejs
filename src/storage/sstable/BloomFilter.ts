import { IBloomFilter, BloomFilterConfig } from './IBloomFilter';

export class BloomFilter implements IBloomFilter {
  private readonly bits: Uint8Array;
  private readonly numBits: number;
  private readonly numHashFunctions: number;

  private constructor(bits: Uint8Array, numBits: number, numHashFunctions: number) {
    this.bits = bits;
    this.numBits = numBits;
    this.numHashFunctions = numHashFunctions;
  }

  public static create(config: BloomFilterConfig): BloomFilter {
    const { numBits, numHashFunctions } = BloomFilter.calculateOptimalParams(
      config.expectedItems,
      config.falsePositiveRate
    );
    const byteSize = Math.ceil(numBits / 8);
    const bits = new Uint8Array(byteSize);
    return new BloomFilter(bits, numBits, numHashFunctions);
  }

  public static fromBuffer(buffer: Buffer): BloomFilter {
    let offset = 0;
    const numBits = buffer.readUInt32BE(offset);
    offset += 4;
    const numHashFunctions = buffer.readUInt32BE(offset);
    offset += 4;
    const byteSize = Math.ceil(numBits / 8);
    const bits = new Uint8Array(buffer.subarray(offset, offset + byteSize));
    return new BloomFilter(bits, numBits, numHashFunctions);
  }

  private static calculateOptimalParams(
    expectedItems: number,
    falsePositiveRate: number
  ): { numBits: number; numHashFunctions: number } {
    const n = Math.max(expectedItems, 1);
    const p = Math.max(falsePositiveRate, 0.0001);
    
    const numBits = Math.ceil(-(n * Math.log(p)) / (Math.LN2 * Math.LN2));
    
    const numHashFunctions = Math.max(1, Math.round((numBits / n) * Math.LN2));

    return { numBits, numHashFunctions };
  }

  public add(key: string): void {
    const [hash1, hash2] = this.getBaseHashes(key);
    for (let i = 0; i < this.numHashFunctions; i++) {
      const bitIndex = this.getCombinedHash(hash1, hash2, i);
      this.setBit(bitIndex);
    }
  }

  public mightContain(key: string): boolean {
    const [hash1, hash2] = this.getBaseHashes(key);
    for (let i = 0; i < this.numHashFunctions; i++) {
      const bitIndex = this.getCombinedHash(hash1, hash2, i);
      if (!this.getBit(bitIndex)) {
        return false;
      }
    }
    return true;
  }

  public serialize(): Buffer {
    const byteSize = Math.ceil(this.numBits / 8);
    const buffer = Buffer.alloc(4 + 4 + byteSize);
    let offset = 0;
    
    buffer.writeUInt32BE(this.numBits, offset);
    offset += 4;
    buffer.writeUInt32BE(this.numHashFunctions, offset);
    offset += 4;
    Buffer.from(this.bits).copy(buffer, offset);
    
    return buffer;
  }

  public getStats(): { numBits: number; numHashFunctions: number; byteSize: number } {
    return {
      numBits: this.numBits,
      numHashFunctions: this.numHashFunctions,
      byteSize: Math.ceil(this.numBits / 8),
    };
  }

  private getBaseHashes(key: string): [number, number] {
    return [this.fnv1a(key), this.djb2(key)];
  }

  private getCombinedHash(hash1: number, hash2: number, index: number): number {
    // Double hashing: h(i) = h1 + i * h2
    const combined = (hash1 + index * hash2) >>> 0;
    return combined % this.numBits;
  }

  private setBit(index: number): void {
    const byteIndex = Math.floor(index / 8);
    const bitOffset = index % 8;
    this.bits[byteIndex]! |= (1 << bitOffset);
  }

  private getBit(index: number): boolean {
    const byteIndex = Math.floor(index / 8);
    const bitOffset = index % 8;
    return (this.bits[byteIndex]! & (1 << bitOffset)) !== 0;
  }

  private fnv1a(str: string): number {
    let hash = 2166136261;
    for (let i = 0; i < str.length; i++) {
      hash ^= str.charCodeAt(i);
      hash = Math.imul(hash, 16777619);
    }
    return hash >>> 0;
  }

  private djb2(str: string): number {
    let hash = 5381;
    for (let i = 0; i < str.length; i++) {
      hash = ((hash << 5) + hash) ^ str.charCodeAt(i);
    }
    return hash >>> 0;
  }
}
