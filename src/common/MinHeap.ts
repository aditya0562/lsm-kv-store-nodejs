
export type Comparator<T> = (a: T, b: T) => number;

export class MinHeap<T> {
  private readonly heap: T[];
  private readonly compare: Comparator<T>;

  constructor(comparator: Comparator<T>) {
    this.heap = [];
    this.compare = comparator;
  }

  public insert(item: T): void {
    this.heap.push(item);
    this.bubbleUp(this.heap.length - 1);
  }

  public extractMin(): T | undefined {
    if (this.heap.length === 0) {
      return undefined;
    }

    if (this.heap.length === 1) {
      return this.heap.pop();
    }

    const min = this.heap[0];
    this.heap[0] = this.heap.pop()!;
    this.bubbleDown(0);
    return min;
  }

  public peek(): T | undefined {
    return this.heap[0];
  }

  public isEmpty(): boolean {
    return this.heap.length === 0;
  }

  public size(): number {
    return this.heap.length;
  }

  public clear(): void {
    this.heap.length = 0;
  }

  private bubbleUp(index: number): void {
    while (index > 0) {
      const parentIndex = Math.floor((index - 1) / 2);
      const parent = this.heap[parentIndex]!;
      const current = this.heap[index]!;

      if (this.compare(current, parent) >= 0) {
        break;
      }

      this.heap[parentIndex] = current;
      this.heap[index] = parent;
      index = parentIndex;
    }
  }

  /**
   * Restore heap property by moving element down.
   */
  private bubbleDown(index: number): void {
    const length = this.heap.length;

    while (true) {
      const leftChild = 2 * index + 1;
      const rightChild = 2 * index + 2;
      let smallest = index;

      if (leftChild < length && this.compare(this.heap[leftChild]!, this.heap[smallest]!) < 0) {
        smallest = leftChild;
      }

      if (rightChild < length && this.compare(this.heap[rightChild]!, this.heap[smallest]!) < 0) {
        smallest = rightChild;
      }

      if (smallest === index) {
        break;
      }

      const temp = this.heap[index]!;
      this.heap[index] = this.heap[smallest]!;
      this.heap[smallest] = temp;
      index = smallest;
    }
  }
}
