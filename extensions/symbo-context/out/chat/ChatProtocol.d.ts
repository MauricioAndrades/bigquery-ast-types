export interface ContextBlock {
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
export interface ChatMessage {
    id: string;
    role: 'system' | 'developer' | 'user' | 'assistant';
    content: string;
    timestamp: number;
    contextBlocks?: ContextBlock[];
    streaming?: boolean;
}
export interface LLMProvider {
    name: string;
    models: string[];
}
export interface ChatState {
    messages: ChatMessage[];
    contextBlocks: ContextBlock[];
    currentProvider: string;
    currentModel: string;
    tokenBudget: number;
    drawerOpen: boolean;
    streaming: boolean;
}
export type HostToWebviewMessage = {
    type: 'init';
    state: ChatState;
} | {
    type: 'streamDelta';
    messageId: string;
    delta: string;
} | {
    type: 'streamEnd';
    messageId: string;
} | {
    type: 'error';
    error: string;
} | {
    type: 'contextUpdate';
    blocks: ContextBlock[];
} | {
    type: 'settingsUpdate';
    settings: Record<string, any>;
};
export type WebviewToHostMessage = {
    type: 'ready';
} | {
    type: 'sendPrompt';
    prompt: string;
    useContext: boolean;
} | {
    type: 'openDrawer';
} | {
    type: 'closeDrawer';
} | {
    type: 'addContextBlock';
    block: Partial<ContextBlock>;
} | {
    type: 'removeContextBlock';
    id: string;
} | {
    type: 'toggleContextBlock';
    id: string;
} | {
    type: 'reorderContextBlocks';
    sourceId: string;
    targetId: string;
} | {
    type: 'updateContextBlock';
    id: string;
    updates: Partial<ContextBlock>;
} | {
    type: 'insertCode';
    code: string;
} | {
    type: 'createFile';
    code: string;
    filename?: string;
} | {
    type: 'stageAsPatch';
    code: string;
    filename?: string;
} | {
    type: 'copyCode';
    code: string;
} | {
    type: 'clearChat';
} | {
    type: 'openSettings';
} | {
    type: 'executeCommand';
    command: string;
    args?: any[];
};
export interface SlashCommand {
    name: string;
    description: string;
    handler: (args: string[]) => Promise<void>;
}
export interface PackingPlan {
    selectedBlocks: ContextBlock[];
    totalTokens: number;
    budgetExceeded: boolean;
    suggestedRemovals: string[];
}
export interface ToolDefinition {
    name: string;
    cmd: string;
    args: string[];
    description?: string;
}
export interface LLMResponse {
    content: string;
    model?: string;
    usage?: {
        promptTokens: number;
        completionTokens: number;
        totalTokens: number;
    };
}
export interface StreamingResponse {
    stream: ReadableStream<string>;
    controller: AbortController;
}
//# sourceMappingURL=ChatProtocol.d.ts.map