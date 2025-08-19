import * as vscode from 'vscode';
import { ContextStore } from '../context/ContextStore';
import { LLMProviderFactory } from '../providers/LLMProviderFactory';
import { GitService } from '../utils/GitService';
import { ToolBridge } from '../utils/ToolBridge';
export declare class ChatViewProvider implements vscode.WebviewViewProvider {
    private readonly _extensionUri;
    private readonly contextStore;
    private readonly llmProviderFactory;
    private readonly gitService;
    private readonly toolBridge;
    private _view?;
    private messages;
    private drawerOpen;
    private slashProcessor;
    constructor(_extensionUri: vscode.Uri, contextStore: ContextStore, llmProviderFactory: LLMProviderFactory, gitService: GitService, toolBridge: ToolBridge);
    resolveWebviewView(webviewView: vscode.WebviewView, context: vscode.WebviewViewResolveContext, _token: vscode.CancellationToken): void;
    private handleWebviewMessage;
    private handleSendPrompt;
    private prepareMessagesForProvider;
    private addMessage;
    private insertCodeIntoEditor;
    private createFileWithCode;
    private stageCodeAsPatch;
    openDrawer(): void;
    sendPrompt(prompt: string): void;
    private getChatState;
    private updateWebview;
    private postMessage;
    private getHtmlForWebview;
}
//# sourceMappingURL=ChatViewProvider.d.ts.map