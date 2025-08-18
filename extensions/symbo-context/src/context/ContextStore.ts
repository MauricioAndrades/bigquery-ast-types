import * as vscode from 'vscode';
import { v4 as uuidv4 } from 'uuid';
import { ContextBlock, PackingPlan } from '../chat/ChatProtocol';

export class ContextStore {
  private blocks: Map<string, ContextBlock> = new Map();
  private readonly storageKey = 'symboContext.blocks';
  private readonly onDidChangeEmitter = new vscode.EventEmitter<ContextBlock[]>();
  public readonly onDidChange = this.onDidChangeEmitter.event;

  constructor(private context: vscode.ExtensionContext) {
    this.loadFromStorage();
  }

  private loadFromStorage(): void {
    const stored = this.context.workspaceState.get<ContextBlock[]>(this.storageKey, []);
    this.blocks.clear();
    stored.forEach(block => {
      this.blocks.set(block.id, block);
    });
  }

  private async saveToStorage(): Promise<void> {
    const blocksArray = Array.from(this.blocks.values());
    await this.context.workspaceState.update(this.storageKey, blocksArray);
    this.onDidChangeEmitter.fire(blocksArray);
  }

  private estimateTokens(text: string): number {
    // Simple heuristic: roughly 4 characters per token
    return Math.ceil(text.length / 4);
  }

  private redactSecrets(content: string): string {
    // Basic secret redaction patterns
    const patterns = [
      /([A-Z_]+_KEY\s*=\s*)([^\s\n]+)/gi,
      /([A-Z_]+_SECRET\s*=\s*)([^\s\n]+)/gi,
      /([A-Z_]+_TOKEN\s*=\s*)([^\s\n]+)/gi,
      /(Bearer\s+)([a-zA-Z0-9._-]+)/gi,
      /(eyJ[a-zA-Z0-9._-]+)/gi, // JWT tokens
    ];

    let redacted = content;
    patterns.forEach(pattern => {
      redacted = redacted.replace(pattern, (match, prefix, secret) => {
        return prefix + '****';
      });
    });

    return redacted;
  }

  async addBlock(blockData: Partial<ContextBlock>): Promise<string> {
    const id = uuidv4();
    const now = Date.now();
    const content = this.redactSecrets(blockData.content || '');
    const chars = content.length;
    const estTokens = this.estimateTokens(content);

    const config = vscode.workspace.getConfiguration('symboContext');
    const defaultTTL = config.get<number>('defaultContextTTL', 3600000);

    const block: ContextBlock = {
      id,
      type: blockData.type || 'text',
      title: blockData.title || 'Untitled',
      source: blockData.source,
      language: blockData.language,
      content,
      chars,
      estTokens,
      include: blockData.include !== false,
      priority: blockData.priority || 5,
      ttlMs: blockData.ttlMs || defaultTTL,
      createdAt: now,
      updatedAt: now,
    };

    // Check for duplicates
    const existing = Array.from(this.blocks.values()).find(
      b => b.content === content && b.source === block.source
    );

    if (existing) {
      // Update existing block timestamp
      existing.updatedAt = now;
      this.blocks.set(existing.id, existing);
      await this.saveToStorage();
      return existing.id;
    }

    this.blocks.set(id, block);
    await this.saveToStorage();
    return id;
  }

  async removeBlock(id: string): Promise<void> {
    if (this.blocks.delete(id)) {
      await this.saveToStorage();
    }
  }

  async updateBlock(id: string, updates: Partial<ContextBlock>): Promise<void> {
    const block = this.blocks.get(id);
    if (!block) {
      return;
    }

    const updatedBlock = {
      ...block,
      ...updates,
      updatedAt: Date.now(),
    };

    // Recalculate tokens if content changed
    if (updates.content !== undefined) {
      updatedBlock.content = this.redactSecrets(updates.content);
      updatedBlock.chars = updatedBlock.content.length;
      updatedBlock.estTokens = this.estimateTokens(updatedBlock.content);
    }

    this.blocks.set(id, updatedBlock);
    await this.saveToStorage();
  }

  async toggleInclude(id: string): Promise<void> {
    const block = this.blocks.get(id);
    if (block) {
      block.include = !block.include;
      block.updatedAt = Date.now();
      this.blocks.set(id, block);
      await this.saveToStorage();
    }
  }

  async reorderBlocks(sourceId: string, targetId: string): Promise<void> {
    const sourceBlock = this.blocks.get(sourceId);
    const targetBlock = this.blocks.get(targetId);

    if (!sourceBlock || !targetBlock) {
      return;
    }

    // Simple reordering by swapping priorities
    const sourcePriority = sourceBlock.priority;
    sourceBlock.priority = targetBlock.priority;
    targetBlock.priority = sourcePriority;

    sourceBlock.updatedAt = Date.now();
    targetBlock.updatedAt = Date.now();

    this.blocks.set(sourceId, sourceBlock);
    this.blocks.set(targetId, targetBlock);
    await this.saveToStorage();
  }

  getBlock(id: string): ContextBlock | undefined {
    return this.blocks.get(id);
  }

  getAllBlocks(): ContextBlock[] {
    return Array.from(this.blocks.values()).sort((a, b) => a.priority - b.priority);
  }

  getIncludedBlocks(): ContextBlock[] {
    return this.getAllBlocks().filter(block => block.include);
  }

  createPackingPlan(): PackingPlan {
    const config = vscode.workspace.getConfiguration('symboContext');
    const tokenBudget = config.get<number>('tokenBudget', 8000);
    
    const includedBlocks = this.getIncludedBlocks();
    const totalTokens = includedBlocks.reduce((sum, block) => sum + block.estTokens, 0);
    
    const plan: PackingPlan = {
      selectedBlocks: includedBlocks,
      totalTokens,
      budgetExceeded: totalTokens > tokenBudget,
      suggestedRemovals: [],
    };

    if (plan.budgetExceeded) {
      // Sort by priority (higher number = lower priority)
      const sortedBlocks = [...includedBlocks].sort((a, b) => b.priority - a.priority);
      let currentTokens = totalTokens;
      
      for (const block of sortedBlocks) {
        if (currentTokens <= tokenBudget) {
          break;
        }
        plan.suggestedRemovals.push(block.id);
        currentTokens -= block.estTokens;
      }
    }

    return plan;
  }

  async clearAll(): Promise<void> {
    this.blocks.clear();
    await this.saveToStorage();
  }

  cleanupExpired(): void {
    const now = Date.now();
    let hasChanges = false;

    for (const [id, block] of this.blocks.entries()) {
      if (block.ttlMs && (now - block.createdAt) > block.ttlMs) {
        this.blocks.delete(id);
        hasChanges = true;
      }
    }

    if (hasChanges) {
      this.saveToStorage();
    }
  }

  async exportToJSON(): Promise<string> {
    const blocks = this.getAllBlocks();
    return JSON.stringify(blocks, null, 2);
  }

  async importFromJSON(json: string): Promise<void> {
    try {
      const blocks: ContextBlock[] = JSON.parse(json);
      this.blocks.clear();
      
      for (const block of blocks) {
        // Validate and sanitize imported blocks
        if (block.id && block.type && block.content) {
          this.blocks.set(block.id, {
            ...block,
            content: this.redactSecrets(block.content),
            chars: block.content.length,
            estTokens: this.estimateTokens(block.content),
            updatedAt: Date.now(),
          });
        }
      }
      
      await this.saveToStorage();
    } catch (error) {
      throw new Error(`Failed to import context: ${error}`);
    }
  }
}