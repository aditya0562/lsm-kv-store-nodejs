/**
 * Application Entry Point
 * 
 * Design: Composition Root - where dependency injection happens.
 * All components are wired together here, not scattered through the codebase.
 * 
 * For Java developers: Similar to main() method in Spring Boot where
 * the application context is initialized.
 */

import { LSMStore } from './storage/LSMStore';
import { HTTPServer } from './server/HTTPServer';
import { DEFAULT_CONFIG } from './common/Config';

async function main(): Promise<void> {
  console.log('Moniepoint KV Store - Starting...');
  
  // Composition Root: Create and wire dependencies
  const store = new LSMStore(DEFAULT_CONFIG);
  const server = new HTTPServer(store, 3000);
  
  // Initialize storage (includes WAL replay)
  await store.initialize();
  
  // Start HTTP server
  await server.start();
  
  console.log('KV Store is ready!');
  console.log('  - HTTP API: http://localhost:3000');
  console.log('  - Health check: http://localhost:3000/health');
  console.log('  - PUT: POST /put with JSON { "key": "...", "value": "..." }');
  console.log('  - GET: GET /get/:key');
  console.log('  - DELETE: DELETE /delete/:key');
  
  // Graceful shutdown handling
  const shutdown = async (): Promise<void> => {
    console.log('\nShutting down gracefully...');
    await server.stop();
    await store.close();
    console.log('Shutdown complete');
    process.exit(0);
  };
  
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
