import * as vscode from 'vscode';
import { ChatMessage, LLMResponse, StreamingResponse } from '../chat/ChatProtocol';
import { ILLMProvider } from './LLMProviderFactory';

export class OpenAIProvider implements ILLMProvider {
  getName(): string {
    return 'OpenAI';
  }

  getModels(): string[] {
    return [
      'gpt-4o',
      'gpt-4o-mini',
      'gpt-4-turbo',
      'gpt-4',
      'gpt-3.5-turbo',
    ];
  }

  private getConfig() {
    const config = vscode.workspace.getConfiguration('symboContext');
    return {
      apiKey: config.get<string>('openai.apiKey', ''),
      baseUrl: config.get<string>('openai.baseUrl', 'https://api.openai.com/v1'),
      model: config.get<string>('openai.model', 'gpt-4o-mini'),
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
    const apiKey = config.apiKey;
    
    if (!apiKey) {
      throw new Error('OpenAI API key not configured');
    }

    const response = await fetch(`${config.baseUrl}/chat/completions`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: model || config.model,
        messages: this.formatMessages(messages),
        temperature: 0.7,
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`OpenAI API error: ${response.status} ${errorText}`);
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
    const apiKey = config.apiKey;
    
    if (!apiKey) {
      throw new Error('OpenAI API key not configured');
    }

    const controller = new AbortController();

    const response = await fetch(`${config.baseUrl}/chat/completions`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: model || config.model,
        messages: this.formatMessages(messages),
        temperature: 0.7,
        stream: true,
      }),
      signal: controller.signal,
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`OpenAI API error: ${response.status} ${errorText}`);
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