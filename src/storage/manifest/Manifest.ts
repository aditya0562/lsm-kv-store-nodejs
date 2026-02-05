
import * as fs from 'fs/promises';
import * as path from 'path';
import { SSTableMetadata } from '../sstable';
import { IManifest } from './IManifest';
import {
  ManifestState,
  VersionEdit,
  EMPTY_MANIFEST_STATE,
  MANIFEST_MAGIC,
  MANIFEST_VERSION,
} from './ManifestTypes';

interface ManifestFile {
  magic: number;
  version: number;
  state: ManifestState;
}

export class Manifest implements IManifest {
  private readonly dataDir: string;
  private readonly manifestPath: string;
  private readonly tempPath: string;
  
  private state: ManifestState;
  private loaded: boolean = false;

  constructor(dataDir: string) {
    this.dataDir = dataDir;
    this.manifestPath = path.join(dataDir, 'MANIFEST');
    this.tempPath = path.join(dataDir, 'MANIFEST.tmp');
    this.state = { ...EMPTY_MANIFEST_STATE };
  }

  public async load(): Promise<void> {
    if (this.loaded) {
      return;
    }

    try {
      await fs.mkdir(this.dataDir, { recursive: true });

      const content = await fs.readFile(this.manifestPath, 'utf8');
      const file: ManifestFile = JSON.parse(content);

      if (file.magic !== MANIFEST_MAGIC) {
        throw new Error('Invalid manifest magic number');
      }
      if (file.version !== MANIFEST_VERSION) {
        throw new Error(`Unsupported manifest version: ${file.version}`);
      }

      this.state = file.state;
      console.log(`Manifest: Loaded version ${this.state.version} with ${this.state.sstables.length} SSTables`);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
        console.log('Manifest: No existing manifest found, starting fresh');
        this.state = { ...EMPTY_MANIFEST_STATE, createdAt: Date.now() };
      } else {
        throw error;
      }
    }

    this.loaded = true;
  }

  public getState(): ManifestState {
    this.ensureLoaded();
    return { ...this.state };
  }

  public getSSTables(): SSTableMetadata[] {
    this.ensureLoaded();
    return [...this.state.sstables];
  }

  public getNextFileNumber(): number {
    this.ensureLoaded();
    return this.state.nextFileNumber;
  }

  public async applyEdit(edit: Partial<VersionEdit>): Promise<void> {
    this.ensureLoaded();

    let sstables = [...this.state.sstables];

    if (edit.removedFileNumbers && edit.removedFileNumbers.length > 0) {
      const removeSet = new Set(edit.removedFileNumbers);
      sstables = sstables.filter(s => !removeSet.has(s.fileNumber));
    }

    if (edit.addedSSTables && edit.addedSSTables.length > 0) {
      sstables.push(...edit.addedSSTables);
    }

    sstables.sort((a, b) => b.fileNumber - a.fileNumber);

    const maxFileNumber = sstables.reduce(
      (max, s) => Math.max(max, s.fileNumber),
      this.state.nextFileNumber - 1
    );
    const nextFileNumber = edit.nextFileNumber ?? maxFileNumber + 1;

    this.state = {
      sstables,
      nextFileNumber,
      lastFlushedSequence: edit.lastFlushedSequence ?? this.state.lastFlushedSequence,
      version: this.state.version + 1,
      createdAt: this.state.createdAt,
    };

    await this.save();
  }

  public async addSSTable(metadata: SSTableMetadata): Promise<void> {
    await this.applyEdit({
      addedSSTables: [metadata],
      nextFileNumber: metadata.fileNumber + 1,
    });
  }

  public async removeSSTables(fileNumbers: number[]): Promise<void> {
    await this.applyEdit({
      removedFileNumbers: fileNumbers,
    });
  }

  public async save(): Promise<void> {
    this.ensureLoaded();

    const file: ManifestFile = {
      magic: MANIFEST_MAGIC,
      version: MANIFEST_VERSION,
      state: this.state,
    };

    const content = JSON.stringify(file, null, 2);

    await fs.writeFile(this.tempPath, content, 'utf8');
    
    // fsync the temp file
    const handle = await fs.open(this.tempPath, 'r');
    await handle.sync();
    await handle.close();

    await fs.rename(this.tempPath, this.manifestPath);

    console.log(`Manifest: Saved version ${this.state.version}`);
  }

  private ensureLoaded(): void {
    if (!this.loaded) {
      throw new Error('Manifest not loaded. Call load() first.');
    }
  }
}
