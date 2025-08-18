import * as vscode from 'vscode';
import { ChatMessage, LLMResponse, StreamingResponse } from '../chat/ChatProtocol';
import { OpenAIProvider } from './OpenAIProvider';
import { AzureOpenAIProvider } from './AzureOpenAIProvider';
import { HTTPProvider } from './HTTPProvider';

export interface ILLMProvider {
  getName(): string;
  getModels(): string[];
  sendMessage(messages: ChatMessage[], model?: string): Promise<LLMResponse>;
  sendMessageStreaming(messages: ChatMessage[], model?: string): Promise<StreamingResponse>;
}

export class LLMProviderFactory {
  private providers: Map<string, ILLMProvider> = new Map();

  constructor() {
    this.providers.set('openai', new OpenAIProvider());
    this.providers.set('azure', new AzureOpenAIProvider());
    this.providers.set('http', new HTTPProvider());
  }

  getCurrentProvider(): ILLMProvider {
    const config = vscode.workspace.getConfiguration('symboContext');
    const providerName = config.get<string>('provider', 'openai');
    
    const provider = this.providers.get(providerName);
    if (!provider) {
      throw new Error(`Provider ${providerName} not found`);
    }
    
    return provider;
  }

  getProvider(name: string): ILLMProvider | undefined {
    return this.providers.get(name);
  }

  getAllProviders(): ILLMProvider[] {
    return Array.from(this.providers.values());
  }

  validateConfiguration(): { valid: boolean; errors: string[] } {
    const config = vscode.workspace.getConfiguration('symboContext');
    const providerName = config.get<string>('provider', 'openai');
    const errors: string[] = [];

    switch (providerName) {
      case 'openai':
        const apiKey = config.get<string>('openai.apiKey', '');
        if (!apiKey) {
          errors.push('OpenAI API key is required');
        }
        break;

      case 'azure':
        const azureEndpoint = config.get<string>('azure.endpoint', '');
        const azureKey = config.get<string>('azure.apiKey', '');
        const deployment = config.get<string>('azure.deployment', '');
        
        if (!azureEndpoint) {
          errors.push('Azure OpenAI endpoint is required');
        }
        if (!azureKey) {
          errors.push('Azure OpenAI API key is required');
        }
        if (!deployment) {
          errors.push('Azure OpenAI deployment name is required');
        }
        break;

      case 'http':
        const httpEndpoint = config.get<string>('http.endpoint', '');
        if (!httpEndpoint) {
          errors.push('HTTP endpoint is required');
        }
        break;

      default:
        errors.push(`Unknown provider: ${providerName}`);
    }

    return {
      valid: errors.length === 0,
      errors,
    };
  }
}