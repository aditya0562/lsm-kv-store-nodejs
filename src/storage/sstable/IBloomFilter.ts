export interface IBloomFilter {
  add(key: string): void;
  mightContain(key: string): boolean;
  serialize(): Buffer;
}

export interface BloomFilterConfig {
  expectedItems: number;
  falsePositiveRate: number;
}
