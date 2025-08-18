import { ChatMessage, LLMResponse, StreamingResponse } from '../chat/ChatProtocol';
import { ILLMProvider } from './LLMProviderFactory';
export declare class HTTPProvider implements ILLMProvider {
    getName(): string;
    getModels(): string[];
    private getConfig;
    private formatMessages;
    sendMessage(messages: ChatMessage[], model?: string): Promise<LLMResponse>;
    sendMessageStreaming(messages: ChatMessage[], model?: string): Promise<StreamingResponse>;
}
//# sourceMappingURL=HTTPProvider.d.ts.map