import * as vscode from 'vscode';
import { ChatMessage, LLMResponse, StreamingResponse } from '../chat/ChatProtocol';
import { ILLMProvider } from './LLMProviderFactory';

export class HTTPProvider implements ILLMProvider {
  getName(): string {
    return 'HTTP';
  }

  getModels(): string[] {
    return ['custom'];
  }

  private getConfig() {
    const config = vscode.workspace.getConfiguration('symboContext');
    return {
      endpoint: config.get<string>('http.endpoint', ''),
    };
  }

  private formatMessages(messages: ChatMessage[]): any[] {
    return messages.map(msg => ({
      role: msg.role === 'developer' ? 'system' : msg.role,
      content: msg.content,
    }));
  }

  async sendMessage(messages: ChatMessage[], model?: string): Promise<LLMResponse> {
    const config = this.getConfig();
    
    if (!config.endpoint) {
      throw new Error('HTTP endpoint not configured');
    }

    const response = await fetch(config.endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        messages: this.formatMessages(messages),
        stream: false,
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`HTTP provider error: ${response.status} ${errorText}`);
    }

    const data = await response.json() as any;
    
    // Handle various response formats
    let content = '';
    if (data.choices && data.choices[0]?.message?.content) {
      content = data.choices[0].message.content;
    } else if (data.response) {
      content = data.response;
    } else if (data.content) {
      content = data.content;
    } else if (typeof data === 'string') {
      content = data;
    }
    
    return {
      content,
      model: data.model || 'custom',
      usage: data.usage ? {
        promptTokens: data.usage.prompt_tokens || 0,
        completionTokens: data.usage.completion_tokens || 0,
        totalTokens: data.usage.total_tokens || 0,
      } : undefined,
    };
  }

  async sendMessageStreaming(messages: ChatMessage[], model?: string): Promise<StreamingResponse> {
    const config = this.getConfig();
    
    if (!config.endpoint) {
      throw new Error('HTTP endpoint not configured');
    }

    const controller = new AbortController();

    const response = await fetch(config.endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        messages: this.formatMessages(messages),
        stream: true,
      }),
      signal: controller.signal,
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`HTTP provider error: ${response.status} ${errorText}`);
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
            
            // Handle server-sent events format
            if (chunk.includes('data: ')) {
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
                    let delta = '';
                    
                    if (parsed.choices && parsed.choices[0]?.delta?.content) {
                      delta = parsed.choices[0].delta.content;
                    } else if (parsed.delta) {
                      delta = parsed.delta;
                    } else if (typeof parsed === 'string') {
                      delta = parsed;
                    }
                    
                    if (delta) {
                      controller_stream.enqueue(delta);
                    }
                  } catch (error) {
                    // Try to use chunk as-is if it's not JSON
                    if (data.trim()) {
                      controller_stream.enqueue(data);
                    }
                  }
                }
              }
            } else {
              // Handle raw streaming without SSE format
              controller_stream.enqueue(chunk);
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