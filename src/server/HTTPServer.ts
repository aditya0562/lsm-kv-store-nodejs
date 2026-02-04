/**
 * HTTP Server - REST API for KV store
 * 
 * Design Patterns:
 * 1. Adapter Pattern: Adapts IStorageEngine to HTTP protocol
 * 2. Dependency Injection: Storage engine injected via constructor
 * 
 * For Java developers: Similar to a Spring @RestController with
 * injected service dependency. Thin layer that maps HTTP to storage operations.
 */

import express, { Request, Response, NextFunction } from 'express';
import { IStorageEngine } from '../interfaces/Storage';

export class HTTPServer {
  private readonly app: express.Application;
  private readonly store: IStorageEngine;
  private readonly port: number;
  private server: ReturnType<express.Application['listen']> | null = null;
  
  /**
   * Constructor with dependency injection
   * 
   * @param store - Storage engine (interface, not concrete class)
   * @param port - HTTP port to listen on
   * 
   * Design: Depends on IStorageEngine interface, not LSMStore class.
   * This enables testing with mock implementations.
   */
  constructor(store: IStorageEngine, port: number) {
    this.store = store;
    this.port = port;
    this.app = express();
    this.setupMiddleware();
    this.setupRoutes();
    this.setupErrorHandling();
  }
  
  /**
   * Setup middleware
   */
  private setupMiddleware(): void {
    this.app.use(express.json({ limit: '10mb' }));
  }
  
  /**
   * Setup HTTP routes
   * 
   * RESTful design:
   * - POST /put - Create/Update
   * - GET /get/:key - Read
   * - DELETE /delete/:key - Delete
   */
  private setupRoutes(): void {
    // Health check endpoint
    this.app.get('/health', (_req: Request, res: Response) => {
      res.json({ status: 'ok', timestamp: Date.now() });
    });
    
    // PUT operation
    this.app.post('/put', this.handlePut.bind(this));
    
    // GET operation
    this.app.get('/get/:key', this.handleGet.bind(this));
    
    // DELETE operation
    this.app.delete('/delete/:key', this.handleDelete.bind(this));
  }
  
  /**
   * Setup error handling middleware
   */
  private setupErrorHandling(): void {
    this.app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
      console.error('Unhandled error:', err);
      res.status(500).json({ error: 'Internal server error' });
    });
  }
  
  /**
   * Handle PUT request
   */
  private async handlePut(req: Request, res: Response): Promise<void> {
    try {
      const { key, value } = req.body;
      
      // Input validation
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
   * Handle GET request
   */
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
  
  /**
   * Handle DELETE request
   */
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
  
  /**
   * Start HTTP server
   */
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
  
  /**
   * Stop HTTP server gracefully
   */
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
