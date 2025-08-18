import * as vscode from 'vscode';
import { ChatMessage, LLMResponse, StreamingResponse } from '../chat/ChatProtocol';
import { ILLMProvider } from './LLMProviderFactory';

export class AzureOpenAIProvider implements ILLMProvider {
  getName(): string {
    return 'Azure OpenAI';
  }

  getModels(): string[] {
    return [
      'gpt-4o',
      'gpt-4o-mini', 
      'gpt-4-turbo',
      'gpt-4',
      'gpt-35-turbo',
    ];
  }

  private getConfig() {
    const config = vscode.workspace.getConfiguration('symboContext');
    return {
      endpoint: config.get<string>('azure.endpoint', ''),
      apiKey: config.get<string>('azure.apiKey', ''),
      deployment: config.get<string>('azure.deployment', ''),
      apiVersion: '2024-02-15-preview',
    };
  }

  private formatMessages(messages: ChatMessage[]): any[] {
    return messages.map(msg => ({
      role: msg.role === 'developer' ? 'system' : msg.role,
      content: msg.content,
    }));
  }

  private getUrl(streaming: boolean = false): string {
    const config = this.getConfig();
    return `${config.endpoint}/openai/deployments/${config.deployment}/chat/completions?api-version=${config.apiVersion}`;
  }

  async sendMessage(messages: ChatMessage[], model?: string): Promise<LLMResponse> {
    const config = this.getConfig();
    
    if (!config.endpoint || !config.apiKey || !config.deployment) {
      throw new Error('Azure OpenAI configuration incomplete');
    }

    const response = await fetch(this.getUrl(), {
      method: 'POST',
      headers: {
        'api-key': config.apiKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        messages: this.formatMessages(messages),
        temperature: 0.7,
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Azure OpenAI API error: ${response.status} ${errorText}`);
    }

    const data = await response.json() as any;
    
    return {
      content: data.choices?.[0]?.message?.content || '',
      model: data.model,
      usage: data.usage ? {
        promptTokens: data.usage.prompt_tokens,
        completionTokens: data.usage.completion_tokens,
        totalTokens: data.usage.total_tokens,
      } : undefined,
    };
  }

  async sendMessageStreaming(messages: ChatMessage[], model?: string): Promise<StreamingResponse> {
    const config = this.getConfig();
    
    if (!config.endpoint || !config.apiKey || !config.deployment) {
      throw new Error('Azure OpenAI configuration incomplete');
    }

    const controller = new AbortController();

    const response = await fetch(this.getUrl(true), {
      method: 'POST',
      headers: {
        'api-key': config.apiKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        messages: this.formatMessages(messages),
        temperature: 0.7,
        stream: true,
      }),
      signal: controller.signal,
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Azure OpenAI API error: ${response.status} ${errorText}`);
    }

    const stream = new ReadableStream<string>({
      async start(controller_stream) {
        const reader = response.body!.getReader();
        const decoder = new TextDecoder();

        try {
          while (true) {
            const { done, value } = await reader.read();
            
            if (done) {
              break;
            }

            const chunk = decoder.decode(value, { stream: true });
            const lines = chunk.split('\n');

            for (const line of lines) {
              if (line.startsWith('data: ')) {
                const data = line.slice(6);
                
                if (data === '[DONE]') {
                  controller_stream.close();
                  return;
                }

                try {
                  const parsed = JSON.parse(data);
                  const delta = parsed.choices[0]?.delta?.content;
                  
                  if (delta) {
                    controller_stream.enqueue(delta);
                  }
                } catch (error) {
                  // Skip invalid JSON lines
                  continue;
                }
              }
            }
          }
        } catch (error) {
          controller_stream.error(error);
        } finally {
          reader.releaseLock();
        }
      },
    });

    return {
      stream,
      controller,
    };
  }
}