import { ChatMessage, LLMResponse, StreamingResponse } from '../chat/ChatProtocol';
import { ILLMProvider } from './LLMProviderFactory';
export declare class OpenAIProvider implements ILLMProvider {
    getName(): string;
    getModels(): string[];
    private getConfig;
    private formatMessages;
    sendMessage(messages: ChatMessage[], model?: string): Promise<LLMResponse>;
    sendMessageStreaming(messages: ChatMessage[], model?: string): Promise<StreamingResponse>;
}
//# sourceMappingURL=OpenAIProvider.d.ts.map