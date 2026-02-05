export interface ITCPServer {
  start(): Promise<void>;
  stop(): Promise<void>;
  getPort(): number;
  getConnectionCount(): number;
}
