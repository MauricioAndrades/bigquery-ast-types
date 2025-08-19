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
exports.ChatViewProvider = void 0;
const vscode = __importStar(require("vscode"));
const uuid_1 = require("uuid");
const SlashCommandProcessor_1 = require("./SlashCommandProcessor");
class ChatViewProvider {
    constructor(_extensionUri, contextStore, llmProviderFactory, gitService, toolBridge) {
        this._extensionUri = _extensionUri;
        this.contextStore = contextStore;
        this.llmProviderFactory = llmProviderFactory;
        this.gitService = gitService;
        this.toolBridge = toolBridge;
        this.messages = [];
        this.drawerOpen = false;
        this.slashProcessor = new SlashCommandProcessor_1.SlashCommandProcessor(this.contextStore, this.gitService, this.toolBridge);
        // Listen for context changes
        this.contextStore.onDidChange(() => {
            this.updateWebview();
        });
    }
    resolveWebviewView(webviewView, context, _token) {
        this._view = webviewView;
        webviewView.webview.options = {
            enableScripts: true,
            localResourceRoots: [this._extensionUri],
        };
        webviewView.webview.html = this.getHtmlForWebview(webviewView.webview);
        webviewView.webview.onDidReceiveMessage(async (data) => {
            await this.handleWebviewMessage(data);
        });
        // Initialize the webview
        this.updateWebview();
    }
    async handleWebviewMessage(message) {
        try {
            switch (message.type) {
                case 'ready':
                    this.updateWebview();
                    break;
                case 'sendPrompt':
                    await this.handleSendPrompt(message.prompt, message.useContext);
                    break;
                case 'openDrawer':
                    this.drawerOpen = true;
                    this.updateWebview();
                    break;
                case 'closeDrawer':
                    this.drawerOpen = false;
                    this.updateWebview();
                    break;
                case 'addContextBlock':
                    if (message.block.content) {
                        await this.contextStore.addBlock(message.block);
                    }
                    break;
                case 'removeContextBlock':
                    await this.contextStore.removeBlock(message.id);
                    break;
                case 'toggleContextBlock':
                    await this.contextStore.toggleInclude(message.id);
                    break;
                case 'reorderContextBlocks':
                    await this.contextStore.reorderBlocks(message.sourceId, message.targetId);
                    break;
                case 'updateContextBlock':
                    await this.contextStore.updateBlock(message.id, message.updates);
                    break;
                case 'insertCode':
                    await this.insertCodeIntoEditor(message.code);
                    break;
                case 'createFile':
                    await this.createFileWithCode(message.code, message.filename);
                    break;
                case 'stageAsPatch':
                    await this.stageCodeAsPatch(message.code, message.filename);
                    break;
                case 'copyCode':
                    await vscode.env.clipboard.writeText(message.code);
                    vscode.window.showInformationMessage('Code copied to clipboard');
                    break;
                case 'clearChat':
                    this.messages = [];
                    this.updateWebview();
                    break;
                case 'openSettings':
                    vscode.commands.executeCommand('workbench.action.openSettings', 'symboContext');
                    break;
                case 'executeCommand':
                    if (message.command) {
                        vscode.commands.executeCommand(message.command, ...(message.args || []));
                    }
                    break;
            }
        }
        catch (error) {
            console.error('Error handling webview message:', error);
            this.postMessage({
                type: 'error',
                error: `Error: ${error}`,
            });
        }
    }
    async handleSendPrompt(prompt, useContext) {
        // Check for slash commands
        if (prompt.startsWith('/')) {
            const result = await this.slashProcessor.processCommand(prompt);
            if (result.handled) {
                if (result.response) {
                    this.addMessage({
                        id: (0, uuid_1.v4)(),
                        role: 'system',
                        content: result.response,
                        timestamp: Date.now(),
                    });
                    this.updateWebview();
                }
                return;
            }
        }
        // Validate provider configuration
        const validation = this.llmProviderFactory.validateConfiguration();
        if (!validation.valid) {
            this.postMessage({
                type: 'error',
                error: `Configuration error: ${validation.errors.join(', ')}`,
            });
            return;
        }
        // Create user message
        const userMessage = {
            id: (0, uuid_1.v4)(),
            role: 'user',
            content: prompt,
            timestamp: Date.now(),
        };
        // Add context blocks if requested
        if (useContext) {
            const plan = this.contextStore.createPackingPlan();
            if (plan.budgetExceeded) {
                const confirm = await vscode.window.showWarningMessage(`Context exceeds token budget (${plan.totalTokens} > ${vscode.workspace.getConfiguration('symboContext').get('tokenBudget')}). Continue anyway?`, 'Continue', 'Cancel');
                if (confirm !== 'Continue') {
                    return;
                }
            }
            userMessage.contextBlocks = plan.selectedBlocks;
        }
        this.addMessage(userMessage);
        // Create assistant message placeholder
        const assistantMessage = {
            id: (0, uuid_1.v4)(),
            role: 'assistant',
            content: '',
            timestamp: Date.now(),
            streaming: true,
        };
        this.addMessage(assistantMessage);
        this.updateWebview();
        try {
            const provider = this.llmProviderFactory.getCurrentProvider();
            const messagesToSend = this.prepareMessagesForProvider(this.messages.slice(0, -1));
            const streamingResponse = await provider.sendMessageStreaming(messagesToSend);
            const reader = streamingResponse.stream.getReader();
            let accumulatedContent = '';
            try {
                while (true) {
                    const { done, value } = await reader.read();
                    if (done) {
                        break;
                    }
                    accumulatedContent += value;
                    // Update the assistant message
                    const messageIndex = this.messages.findIndex(m => m.id === assistantMessage.id);
                    if (messageIndex !== -1) {
                        this.messages[messageIndex].content = accumulatedContent;
                        this.postMessage({
                            type: 'streamDelta',
                            messageId: assistantMessage.id,
                            delta: value,
                        });
                    }
                }
            }
            finally {
                reader.releaseLock();
            }
            // Finalize the message
            const messageIndex = this.messages.findIndex(m => m.id === assistantMessage.id);
            if (messageIndex !== -1) {
                this.messages[messageIndex].streaming = false;
                this.postMessage({
                    type: 'streamEnd',
                    messageId: assistantMessage.id,
                });
            }
            this.updateWebview();
        }
        catch (error) {
            console.error('Error sending message:', error);
            // Remove the streaming message and show error
            this.messages = this.messages.filter(m => m.id !== assistantMessage.id);
            this.postMessage({
                type: 'error',
                error: `Failed to send message: ${error}`,
            });
            this.updateWebview();
        }
    }
    prepareMessagesForProvider(messages) {
        return messages.map(msg => {
            let content = msg.content;
            // Add context blocks to user messages
            if (msg.role === 'user' && msg.contextBlocks && msg.contextBlocks.length > 0) {
                const contextString = msg.contextBlocks
                    .map(block => `<context type="${block.type}" title="${block.title}"${block.source ? ` source="${block.source}"` : ''}>\n${block.content}\n</context>`)
                    .join('\n\n');
                content = `${content}\n\n--- Context ---\n${contextString}`;
            }
            return {
                ...msg,
                content,
                contextBlocks: undefined, // Remove from provider message
            };
        });
    }
    addMessage(message) {
        this.messages.push(message);
    }
    async insertCodeIntoEditor(code) {
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            vscode.window.showWarningMessage('No active editor');
            return;
        }
        await editor.edit(editBuilder => {
            editBuilder.insert(editor.selection.active, code);
        });
    }
    async createFileWithCode(code, filename) {
        const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
        if (!workspaceFolder) {
            vscode.window.showWarningMessage('No workspace folder');
            return;
        }
        if (!filename) {
            filename = await vscode.window.showInputBox({
                prompt: 'Enter filename',
                placeHolder: 'newfile.ts',
            });
        }
        if (!filename) {
            return;
        }
        const filePath = vscode.Uri.joinPath(workspaceFolder.uri, filename);
        try {
            await vscode.workspace.fs.writeFile(filePath, Buffer.from(code));
            const document = await vscode.workspace.openTextDocument(filePath);
            await vscode.window.showTextDocument(document);
        }
        catch (error) {
            vscode.window.showErrorMessage(`Failed to create file: ${error}`);
        }
    }
    async stageCodeAsPatch(code, filename) {
        const patchName = filename || `patch_${Date.now()}`;
        try {
            await this.gitService.createPatchFile(patchName, code);
            vscode.window.showInformationMessage(`Patch created: ${patchName}.patch`);
        }
        catch (error) {
            vscode.window.showErrorMessage(`Failed to create patch: ${error}`);
        }
    }
    openDrawer() {
        this.drawerOpen = true;
        this.updateWebview();
    }
    sendPrompt(prompt) {
        this.postMessage({
            type: 'init',
            state: this.getChatState(),
        });
        // Simulate typing the prompt
        setTimeout(() => {
            this.handleSendPrompt(prompt, true);
        }, 100);
    }
    getChatState() {
        const config = vscode.workspace.getConfiguration('symboContext');
        return {
            messages: this.messages,
            contextBlocks: this.contextStore.getAllBlocks(),
            currentProvider: config.get('provider', 'openai'),
            currentModel: config.get(`${config.get('provider', 'openai')}.model`, 'gpt-4o-mini'),
            tokenBudget: config.get('tokenBudget', 8000),
            drawerOpen: this.drawerOpen,
            streaming: this.messages.some(m => m.streaming),
        };
    }
    updateWebview() {
        if (this._view) {
            this.postMessage({
                type: 'init',
                state: this.getChatState(),
            });
        }
    }
    postMessage(message) {
        if (this._view) {
            this._view.webview.postMessage(message);
        }
    }
    getHtmlForWebview(webview) {
        const scriptUri = webview.asWebviewUri(vscode.Uri.joinPath(this._extensionUri, 'media', 'app', 'main.js'));
        const styleUri = webview.asWebviewUri(vscode.Uri.joinPath(this._extensionUri, 'media', 'app', 'styles.css'));
        const drawerStyleUri = webview.asWebviewUri(vscode.Uri.joinPath(this._extensionUri, 'media', 'app', 'drawer.css'));
        const codeThemeUri = webview.asWebviewUri(vscode.Uri.joinPath(this._extensionUri, 'media', 'app', 'codetheme.css'));
        return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link href="${styleUri}" rel="stylesheet">
    <link href="${drawerStyleUri}" rel="stylesheet">
    <link href="${codeThemeUri}" rel="stylesheet">
    <title>Symbo Context Chat</title>
</head>
<body>
    <div id="app">
        <div id="chat-container">
            <!-- Chat interface will be rendered here -->
        </div>
        <div id="context-drawer" class="drawer">
            <!-- Context drawer will be rendered here -->
        </div>
    </div>
    <script src="${scriptUri}"></script>
</body>
</html>`;
    }
}
exports.ChatViewProvider = ChatViewProvider;
//# sourceMappingURL=ChatViewProvider.js.map