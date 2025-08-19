import { ChatMessage, LLMResponse, StreamingResponse } from '../chat/ChatProtocol';
import { ILLMProvider } from './LLMProviderFactory';
export declare class AzureOpenAIProvider implements ILLMProvider {
    getName(): string;
    getModels(): string[];
    private getConfig;
    private formatMessages;
    private getUrl;
    sendMessage(messages: ChatMessage[], model?: string): Promise<LLMResponse>;
    sendMessageStreaming(messages: ChatMessage[], model?: string): Promise<StreamingResponse>;
}
//# sourceMappingURL=AzureOpenAIProvider.d.ts.map