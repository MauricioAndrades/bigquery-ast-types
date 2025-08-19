import * as vscode from 'vscode';
import { ContextBlock, PackingPlan } from '../chat/ChatProtocol';
export declare class ContextStore {
    private context;
    private blocks;
    private readonly storageKey;
    private readonly onDidChangeEmitter;
    readonly onDidChange: vscode.Event<ContextBlock[]>;
    constructor(context: vscode.ExtensionContext);
    private loadFromStorage;
    private saveToStorage;
    private estimateTokens;
    private redactSecrets;
    addBlock(blockData: Partial<ContextBlock>): Promise<string>;
    removeBlock(id: string): Promise<void>;
    updateBlock(id: string, updates: Partial<ContextBlock>): Promise<void>;
    toggleInclude(id: string): Promise<void>;
    reorderBlocks(sourceId: string, targetId: string): Promise<void>;
    getBlock(id: string): ContextBlock | undefined;
    getAllBlocks(): ContextBlock[];
    getIncludedBlocks(): ContextBlock[];
    createPackingPlan(): PackingPlan;
    clearAll(): Promise<void>;
    cleanupExpired(): void;
    exportToJSON(): Promise<string>;
    importFromJSON(json: string): Promise<void>;
}
//# sourceMappingURL=ContextStore.d.ts.map