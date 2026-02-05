import express, { Request, Response, NextFunction } from 'express';
import { IStorageEngine } from '../interfaces/Storage';

export class HTTPServer {
  private readonly app: express.Application;
  private readonly store: IStorageEngine;
  private readonly port: number;
  private server: ReturnType<express.Application['listen']> | null = null;
  
  constructor(store: IStorageEngine, port: number) {
    this.store = store;
    this.port = port;
    this.app = express();
    this.setupMiddleware();
    this.setupRoutes();
    this.setupErrorHandling();
  }
  
  private setupMiddleware(): void {
    this.app.use(express.json({ limit: '10mb' }));
  }
  
  private setupRoutes(): void {
    this.app.get('/health', (_req: Request, res: Response) => {
      res.json({ status: 'ok', timestamp: Date.now() });
    });
    
    this.app.post('/put', this.handlePut.bind(this));
    
    this.app.post('/batch-put', this.handleBatchPut.bind(this));
    
    this.app.get('/get/:key', this.handleGet.bind(this));
    
    this.app.delete('/delete/:key', this.handleDelete.bind(this));
    
    this.app.get('/range', this.handleRange.bind(this));
  }
  
  private setupErrorHandling(): void {
    this.app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
      console.error('Unhandled error:', err);
      res.status(500).json({ error: 'Internal server error' });
    });
  }
  
  private async handlePut(req: Request, res: Response): Promise<void> {
    try {
      const { key, value } = req.body;
      
      if (typeof key !== 'string' || key.length === 0) {
        res.status(400).json({ error: 'Invalid key: must be non-empty string' });
        return;
      }
      
      if (value === undefined || value === null) {
        res.status(400).json({ error: 'Invalid value: must not be null or undefined' });
        return;
      }
      
      await this.store.put(key, String(value));
      res.json({ success: true });
    } catch (err) {
      console.error('PUT error:', err);
      res.status(500).json({ error: 'Internal server error' });
    }
  }

  /**
   * Handle BATCH PUT request (multiple entries)
   * 
   * Request body formats:
   * 1. Array format: { "entries": [{ "key": "k1", "value": "v1" }, ...] }
   * 2. Parallel arrays: { "keys": ["k1", "k2"], "values": ["v1", "v2"] }
   */
  private async handleBatchPut(req: Request, res: Response): Promise<void> {
    try {
      const { entries, keys, values } = req.body;

      // Parse entries from either format
      let parsedEntries: Array<{ key: string; value: string }>;

      if (Array.isArray(entries)) {
        parsedEntries = [];
        
        for (let i = 0; i < entries.length; i++) {
          const entry = entries[i];
          
          if (!entry || typeof entry.key !== 'string' || entry.key.length === 0) {
            res.status(400).json({ 
              error: `Invalid key at index ${i}: must be non-empty string` 
            });
            return;
          }
          
          if (entry.value === undefined || entry.value === null) {
            res.status(400).json({ 
              error: `Invalid value at index ${i}: must not be null or undefined` 
            });
            return;
          }
          
          parsedEntries.push({ key: entry.key, value: String(entry.value) });
        }
      } else if (Array.isArray(keys) && Array.isArray(values)) {
        if (keys.length !== values.length) {
          res.status(400).json({ 
            error: 'Keys and values arrays must have the same length' 
          });
          return;
        }
        
        parsedEntries = [];
        
        for (let i = 0; i < keys.length; i++) {
          const key = keys[i];
          const value = values[i];
          
          if (typeof key !== 'string' || key.length === 0) {
            res.status(400).json({ 
              error: `Invalid key at index ${i}: must be non-empty string` 
            });
            return;
          }
          
          if (value === undefined || value === null) {
            res.status(400).json({ 
              error: `Invalid value at index ${i}: must not be null or undefined` 
            });
            return;
          }
          
          parsedEntries.push({ key, value: String(value) });
        }
      } else {
        res.status(400).json({ 
          error: 'Invalid request body: provide either "entries" array or "keys" and "values" arrays' 
        });
        return;
      }

      if (parsedEntries.length === 0) {
        res.status(400).json({ error: 'No entries provided' });
        return;
      }

      const count = await this.store.batchPut(parsedEntries);
      
      res.json({ 
        success: true, 
        count,
      });
    } catch (err) {
      console.error('BATCH PUT error:', err);
      res.status(500).json({ error: 'Internal server error' });
    }
  }
  
  private async handleGet(req: Request, res: Response): Promise<void> {
    try {
      const key = req.params.key;
      
      if (!key) {
        res.status(400).json({ error: 'Key parameter required' });
        return;
      }
      
      const value = await this.store.get(key);
      
      if (value === null) {
        res.status(404).json({ error: 'Key not found', key });
        return;
      }
      
      res.json({ key, value });
    } catch (err) {
      console.error('GET error:', err);
      res.status(500).json({ error: 'Internal server error' });
    }
  }
  
  private async handleDelete(req: Request, res: Response): Promise<void> {
    try {
      const key = req.params.key;
      
      if (!key) {
        res.status(400).json({ error: 'Key parameter required' });
        return;
      }
      
      await this.store.delete(key);
      res.json({ success: true });
    } catch (err) {
      console.error('DELETE error:', err);
      res.status(500).json({ error: 'Internal server error' });
    }
  }

  private async handleRange(req: Request, res: Response): Promise<void> {
    try {
      const { start, end, limit } = req.query;
      
      if (typeof start !== 'string' || start.length === 0) {
        res.status(400).json({ error: 'Invalid start: must be non-empty string' });
        return;
      }
      
      if (typeof end !== 'string' || end.length === 0) {
        res.status(400).json({ error: 'Invalid end: must be non-empty string' });
        return;
      }
      
      let parsedLimit: number | undefined;
      if (limit !== undefined) {
        parsedLimit = parseInt(String(limit), 10);
        if (isNaN(parsedLimit) || parsedLimit < 1) {
          res.status(400).json({ error: 'Invalid limit: must be positive integer' });
          return;
        }
      } else {
        parsedLimit = 100;
      }
      
      const results: Array<{ key: string; value: string }> = [];
      
      for await (const pair of this.store.readKeyRange(start, end, { limit: parsedLimit })) {
        results.push(pair);
      }
      
      res.json({ 
        count: results.length,
        results,
      });
    } catch (err) {
      console.error('RANGE error:', err);
      res.status(500).json({ error: 'Internal server error' });
    }
  }
  
  async start(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.server = this.app.listen(this.port, () => {
        console.log(`HTTP server listening on port ${this.port}`);
        resolve();
      });
      
      this.server.on('error', (err: Error) => {
        reject(err);
      });
    });
  }
  
  async stop(): Promise<void> {
    return new Promise((resolve) => {
      if (this.server) {
        this.server.close(() => {
          console.log('HTTP server stopped');
          resolve();
        });
      } else {
        resolve();
      }
    });
  }
}
