/**
 * Custom error types for the storage engine.
 * 
 * Design: Typed errors allow callers to handle specific error conditions
 * appropriately (e.g., KeyNotFound vs StorageError).
 */

export class StorageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'StorageError';
    Error.captureStackTrace(this, this.constructor);
  }
}

export class KeyNotFoundError extends StorageError {
  constructor(key: string) {
    super(`Key not found: ${key}`);
    this.name = 'KeyNotFoundError';
  }
}
