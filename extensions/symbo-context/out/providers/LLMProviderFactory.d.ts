import { ChatMessage, LLMResponse, StreamingResponse } from '../chat/ChatProtocol';
export interface ILLMProvider {
    getName(): string;
    getModels(): string[];
    sendMessage(messages: ChatMessage[], model?: string): Promise<LLMResponse>;
    sendMessageStreaming(messages: ChatMessage[], model?: string): Promise<StreamingResponse>;
}
export declare class LLMProviderFactory {
    private providers;
    constructor();
    getCurrentProvider(): ILLMProvider;
    getProvider(name: string): ILLMProvider | undefined;
    getAllProviders(): ILLMProvider[];
    validateConfiguration(): {
        valid: boolean;
        errors: string[];
    };
}
//# sourceMappingURL=LLMProviderFactory.d.ts.map