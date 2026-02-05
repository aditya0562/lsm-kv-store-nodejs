import { LSMStore } from './storage/LSMStore';
import { HTTPServer } from './server/HTTPServer';
import { TCPServer } from './server/TCPServer';
import { DEFAULT_CONFIG } from './common/Config';

async function main(): Promise<void> {
  console.log('Moniepoint KV Store - Starting...');
  
  const store = new LSMStore(DEFAULT_CONFIG);
  const httpServer = new HTTPServer(store, DEFAULT_CONFIG.httpPort);
  const tcpServer = new TCPServer(store, { port: DEFAULT_CONFIG.tcpPort });
  
  await store.initialize();
  
  await httpServer.start();
  await tcpServer.start();
  
  console.log('KV Store is ready!');
  console.log(`  - HTTP API: http://localhost:${DEFAULT_CONFIG.httpPort}`);
  console.log(`  - TCP Streaming: localhost:${DEFAULT_CONFIG.tcpPort}`);
  console.log('  - PUT: POST /put with JSON { "key": "...", "value": "..." }');
  console.log('  - GET: GET /get/:key');
  console.log('  - DELETE: DELETE /delete/:key');
  
  const shutdown = async (): Promise<void> => {
    console.log('\nShutting down gracefully...');
    await tcpServer.stop();
    await httpServer.stop();
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
