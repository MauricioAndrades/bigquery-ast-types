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
exports.LLMProviderFactory = void 0;
const vscode = __importStar(require("vscode"));
const OpenAIProvider_1 = require("./OpenAIProvider");
const AzureOpenAIProvider_1 = require("./AzureOpenAIProvider");
const HTTPProvider_1 = require("./HTTPProvider");
class LLMProviderFactory {
    constructor() {
        this.providers = new Map();
        this.providers.set('openai', new OpenAIProvider_1.OpenAIProvider());
        this.providers.set('azure', new AzureOpenAIProvider_1.AzureOpenAIProvider());
        this.providers.set('http', new HTTPProvider_1.HTTPProvider());
    }
    getCurrentProvider() {
        const config = vscode.workspace.getConfiguration('symboContext');
        const providerName = config.get('provider', 'openai');
        const provider = this.providers.get(providerName);
        if (!provider) {
            throw new Error(`Provider ${providerName} not found`);
        }
        return provider;
    }
    getProvider(name) {
        return this.providers.get(name);
    }
    getAllProviders() {
        return Array.from(this.providers.values());
    }
    validateConfiguration() {
        const config = vscode.workspace.getConfiguration('symboContext');
        const providerName = config.get('provider', 'openai');
        const errors = [];
        switch (providerName) {
            case 'openai':
                const apiKey = config.get('openai.apiKey', '');
                if (!apiKey) {
                    errors.push('OpenAI API key is required');
                }
                break;
            case 'azure':
                const azureEndpoint = config.get('azure.endpoint', '');
                const azureKey = config.get('azure.apiKey', '');
                const deployment = config.get('azure.deployment', '');
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
                const httpEndpoint = config.get('http.endpoint', '');
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
exports.LLMProviderFactory = LLMProviderFactory;
//# sourceMappingURL=LLMProviderFactory.js.map