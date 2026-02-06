import { StorageConfig, SyncPolicy, DEFAULT_CONFIG } from '../common/Config';
import { ReplicationRole, ReplicationConfig } from '../replication/ReplicationTypes';

export interface CLIOptions {
  readonly config: StorageConfig;
  readonly help: boolean;
}

export class CLIParser {
  private readonly args: string[];

  constructor(args: string[] = process.argv.slice(2)) {
    this.args = args;
  }

  public parse(): CLIOptions {
    if (this.hasFlag('--help') || this.hasFlag('-h')) {
      return { config: DEFAULT_CONFIG, help: true };
    }

    const replication = this.parseReplicationConfig();

    const config: StorageConfig = {
      ...DEFAULT_CONFIG,
      dataDir: this.getString('--data-dir') ?? DEFAULT_CONFIG.dataDir,
      httpPort: this.getNumber('--http-port') ?? DEFAULT_CONFIG.httpPort,
      tcpPort: this.getNumber('--tcp-port') ?? DEFAULT_CONFIG.tcpPort,
      memTableSizeLimit: this.getNumber('--memtable-size') ?? DEFAULT_CONFIG.memTableSizeLimit,
      syncPolicy: this.parseSyncPolicy() ?? DEFAULT_CONFIG.syncPolicy,
      ...(replication && { replication }),
    };

    return { config, help: false };
  }

  private parseReplicationConfig(): ReplicationConfig | undefined {
    const roleStr = this.getString('--role');
    if (!roleStr) {
      return undefined;
    }

    const role = this.parseRole(roleStr);
    if (role === ReplicationRole.STANDALONE) {
      return undefined;
    }

    if (role === ReplicationRole.PRIMARY) {
      const backupHost = this.getString('--backup-host');
      const backupPort = this.getNumber('--backup-port');
      
      if (!backupHost || !backupPort) {
        throw new Error('PRIMARY role requires --backup-host and --backup-port');
      }

      return {
        role,
        backupHost,
        backupPort,
      };
    }

    if (role === ReplicationRole.BACKUP) {
      const replicationPort = this.getNumber('--replication-port');
      
      if (!replicationPort) {
        throw new Error('BACKUP role requires --replication-port');
      }

      return {
        role,
        replicationPort,
      };
    }

    return undefined;
  }

  private parseRole(value: string): ReplicationRole {
    const normalized = value.toLowerCase();
    switch (normalized) {
      case 'primary': return ReplicationRole.PRIMARY;
      case 'backup': return ReplicationRole.BACKUP;
      case 'standalone': return ReplicationRole.STANDALONE;
      default: throw new Error(`Invalid role: ${value}. Must be primary, backup, or standalone`);
    }
  }

  private parseSyncPolicy(): SyncPolicy | undefined {
    const value = this.getString('--sync-policy');
    if (!value) return undefined;

    const normalized = value.toLowerCase();
    switch (normalized) {
      case 'sync': return SyncPolicy.SYNC_EVERY_WRITE;
      case 'group': return SyncPolicy.GROUP_COMMIT_100MS;
      case 'periodic': return SyncPolicy.PERIODIC_10MS;
      default: throw new Error(`Invalid sync policy: ${value}. Must be sync, group, or periodic`);
    }
  }

  private getString(flag: string): string | undefined {
    const index = this.args.findIndex(arg => arg.startsWith(`${flag}=`));
    if (index !== -1) {
      return this.args[index]!.split('=')[1];
    }

    const flagIndex = this.args.indexOf(flag);
    if (flagIndex !== -1 && flagIndex + 1 < this.args.length) {
      return this.args[flagIndex + 1];
    }

    return undefined;
  }

  private getNumber(flag: string): number | undefined {
    const str = this.getString(flag);
    if (!str) return undefined;
    
    const num = parseInt(str, 10);
    if (isNaN(num)) {
      throw new Error(`Invalid number for ${flag}: ${str}`);
    }
    return num;
  }

  private hasFlag(flag: string): boolean {
    return this.args.includes(flag);
  }

  public static printHelp(): void {
    console.log(`
Moniepoint KV Store

Usage: node dist/index.js [options]

Options:
  --help, -h              Show this help message

Storage Options:
  --data-dir=PATH         Data directory (default: ./data)
  --http-port=PORT        HTTP server port (default: 3000)
  --tcp-port=PORT         TCP streaming port (default: 3001)
  --memtable-size=BYTES   MemTable size limit (default: 4MB)
  --sync-policy=POLICY    Sync policy: sync, group, periodic (default: group)

Replication Options:
  --role=ROLE             Replication role: standalone, primary, backup

  PRIMARY mode:
  --backup-host=HOST      Backup server hostname (required for primary)
  --backup-port=PORT      Backup server replication port (required for primary)

  BACKUP mode:
  --replication-port=PORT Port to listen for replication (required for backup)

Examples:
  # Standalone mode (default)
  node dist/index.js

  # Primary with backup at 192.168.1.2:3002
  node dist/index.js --role=primary --backup-host=192.168.1.2 --backup-port=3002

  # Backup listening on port 3002
  node dist/index.js --role=backup --replication-port=3002
`);
  }
}
