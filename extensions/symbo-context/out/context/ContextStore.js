"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.ContextStore = void 0;
const vscode = __importStar(require("vscode"));
const uuid_1 = require("uuid");
class ContextStore {
    constructor(context) {
        this.context = context;
        this.blocks = new Map();
        this.storageKey = 'symboContext.blocks';
        this.onDidChangeEmitter = new vscode.EventEmitter();
        this.onDidChange = this.onDidChangeEmitter.event;
        this.loadFromStorage();
    }
    loadFromStorage() {
        const stored = this.context.workspaceState.get(this.storageKey, []);
        this.blocks.clear();
        stored.forEach(block => {
            this.blocks.set(block.id, block);
        });
    }
    async saveToStorage() {
        const blocksArray = Array.from(this.blocks.values());
        await this.context.workspaceState.update(this.storageKey, blocksArray);
        this.onDidChangeEmitter.fire(blocksArray);
    }
    estimateTokens(text) {
        // Simple heuristic: roughly 4 characters per token
        return Math.ceil(text.length / 4);
    }
    redactSecrets(content) {
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
    async addBlock(blockData) {
        const id = (0, uuid_1.v4)();
        const now = Date.now();
        const content = this.redactSecrets(blockData.content || '');
        const chars = content.length;
        const estTokens = this.estimateTokens(content);
        const config = vscode.workspace.getConfiguration('symboContext');
        const defaultTTL = config.get('defaultContextTTL', 3600000);
        const block = {
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
        const existing = Array.from(this.blocks.values()).find(b => b.content === content && b.source === block.source);
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
    async removeBlock(id) {
        if (this.blocks.delete(id)) {
            await this.saveToStorage();
        }
    }
    async updateBlock(id, updates) {
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
    async toggleInclude(id) {
        const block = this.blocks.get(id);
        if (block) {
            block.include = !block.include;
            block.updatedAt = Date.now();
            this.blocks.set(id, block);
            await this.saveToStorage();
        }
    }
    async reorderBlocks(sourceId, targetId) {
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
    getBlock(id) {
        return this.blocks.get(id);
    }
    getAllBlocks() {
        return Array.from(this.blocks.values()).sort((a, b) => a.priority - b.priority);
    }
    getIncludedBlocks() {
        return this.getAllBlocks().filter(block => block.include);
    }
    createPackingPlan() {
        const config = vscode.workspace.getConfiguration('symboContext');
        const tokenBudget = config.get('tokenBudget', 8000);
        const includedBlocks = this.getIncludedBlocks();
        const totalTokens = includedBlocks.reduce((sum, block) => sum + block.estTokens, 0);
        const plan = {
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
    async clearAll() {
        this.blocks.clear();
        await this.saveToStorage();
    }
    cleanupExpired() {
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
    async exportToJSON() {
        const blocks = this.getAllBlocks();
        return JSON.stringify(blocks, null, 2);
    }
    async importFromJSON(json) {
        try {
            const blocks = JSON.parse(json);
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
        }
        catch (error) {
            throw new Error(`Failed to import context: ${error}`);
        }
    }
}
exports.ContextStore = ContextStore;
//# sourceMappingURL=ContextStore.js.map