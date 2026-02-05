import { KVPair } from './MergeIteratorTypes';

export interface IMergeIterator {
  /**
   * Iterate over merged entries in sorted order.
   * Automatically handles:
   *   - Duplicate keys (newest source wins)
   *   - Tombstones (filtered out from output)
   * 
   * @yields Key-value pairs in ascending key order
   */
  iterate(): AsyncIterable<KVPair>;
}
