export interface MergeEntry {
  readonly key: string;
  readonly value: string;
  readonly timestamp: number;
  readonly deleted: boolean;
}


export interface HeapEntry extends MergeEntry {
  readonly sourceIndex: number;
}

export interface IMergeSource {
  next(): Promise<MergeEntry | null> | MergeEntry | null;
}

export interface KVPair {
  readonly key: string;
  readonly value: string;
}
