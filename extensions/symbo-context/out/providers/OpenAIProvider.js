"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.OpenAIProvider = void 0;
const vscode = __importStar(require("vscode"));
class OpenAIProvider {
    getName() {
        return 'OpenAI';
    }
    getModels() {
        return [
            'gpt-4o',
            'gpt-4o-mini',
            'gpt-4-turbo',
            'gpt-4',
            'gpt-3.5-turbo',
        ];
    }
    getConfig() {
        const config = vscode.workspace.getConfiguration('symboContext');
        return {
            apiKey: config.get('openai.apiKey', ''),
            baseUrl: config.get('openai.baseUrl', 'https://api.openai.com/v1'),
            model: config.get('openai.model', 'gpt-4o-mini'),
        };
    }
    formatMessages(messages) {
        return messages.map(msg => ({
            role: msg.role === 'developer' ? 'system' : msg.role,
            content: msg.content,
        }));
    }
    async sendMessage(messages, model) {
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
        const data = await response.json();
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
    async sendMessageStreaming(messages, model) {
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
        const stream = new ReadableStream({
            async start(controller_stream) {
                const reader = response.body.getReader();
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
                                }
                                catch (error) {
                                    // Skip invalid JSON lines
                                    continue;
                                }
                            }
                        }
                    }
                }
                catch (error) {
                    controller_stream.error(error);
                }
                finally {
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
exports.OpenAIProvider = OpenAIProvider;
//# sourceMappingURL=OpenAIProvider.js.map