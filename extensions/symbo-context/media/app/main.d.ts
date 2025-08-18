declare const vscode: any;
interface ChatMessage {
    id: string;
    role: 'system' | 'developer' | 'user' | 'assistant';
    content: string;
    timestamp: number;
    contextBlocks?: ContextBlock[];
    streaming?: boolean;
}
interface ContextBlock {
    id: string;
    type: 'file' | 'selection' | 'diff' | 'text' | 'tool';
    title: string;
    source?: string;
    language?: string;
    content: string;
    chars: number;
    estTokens: number;
    include: boolean;
    priority: number;
    ttlMs?: number;
    createdAt: number;
    updatedAt: number;
}
interface ChatState {
    messages: ChatMessage[];
    contextBlocks: ContextBlock[];
    currentProvider: string;
    currentModel: string;
    tokenBudget: number;
    drawerOpen: boolean;
    streaming: boolean;
}
declare class ChatApp {
    private state;
    private chatContainer;
    private contextDrawer;
    private messageInput;
    private useContextCheckbox;
    private sendButton;
    constructor();
    private init;
    private setupElements;
    private setupEventListeners;
    private handleMessage;
    private handleStreamDelta;
    private handleStreamEnd;
    private showError;
    private render;
    private renderChat;
    private renderMessageHTML;
    private renderMessage;
    private formatMessageContent;
    private renderCodeBlock;
    private renderContextBlocks;
    private renderContextDrawer;
    private renderContextBlockItem;
    private getIncludedBlocks;
    private getTotalTokens;
    private sendMessage;
    private toggleDrawer;
    private openDrawer;
    private closeDrawer;
    private updateDrawerVisibility;
    private escapeHtml;
}
//# sourceMappingURL=main.d.ts.map