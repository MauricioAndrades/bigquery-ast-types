import * as vscode from 'vscode';
import { ChatViewProvider } from './chat/ChatViewProvider';
import { ContextStore } from './context/ContextStore';
import { LLMProviderFactory } from './providers/LLMProviderFactory';
import { GitService } from './utils/GitService';
import { ToolBridge } from './utils/ToolBridge';

export function activate(context: vscode.ExtensionContext) {
  console.log('Symbo Context Chat extension is now active');

  // Set context variable to enable views
  vscode.commands.executeCommand('setContext', 'symboContext:enabled', true);

  // Initialize core services
  const contextStore = new ContextStore(context);
  const llmProviderFactory = new LLMProviderFactory();
  const gitService = new GitService();
  const toolBridge = new ToolBridge();

  // Create the chat view provider
  const chatProvider = new ChatViewProvider(
    context.extensionUri,
    contextStore,
    llmProviderFactory,
    gitService,
    toolBridge
  );

  // Register the webview view provider
  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider('symboContext.chatView', chatProvider, {
      webviewOptions: {
        retainContextWhenHidden: true,
      },
    })
  );

  // Register commands
  const commands = [
    vscode.commands.registerCommand('symboContext.open', () => {
      vscode.commands.executeCommand('symboContext.chatView.focus');
    }),
    
    vscode.commands.registerCommand('symboContext.openDrawer', () => {
      chatProvider.openDrawer();
    }),
    
    vscode.commands.registerCommand('symboContext.sendPrompt', async (prompt?: string) => {
      if (!prompt) {
        const input = await vscode.window.showInputBox({
          placeHolder: 'Enter your prompt...',
          prompt: 'Send message to Symbo Context Chat',
        });
        if (!input) {return;}
        prompt = input;
      }
      chatProvider.sendPrompt(prompt);
    }),
    
    vscode.commands.registerCommand('symboContext.addSelection', async () => {
      const editor = vscode.window.activeTextEditor;
      if (!editor || editor.selection.isEmpty) {
        vscode.window.showWarningMessage('No text selected');
        return;
      }
      
      const selectedText = editor.document.getText(editor.selection);
      const fileName = editor.document.fileName;
      
      await contextStore.addBlock({
        type: 'selection',
        title: `Selection from ${fileName}`,
        source: fileName,
        language: editor.document.languageId,
        content: selectedText,
        include: true,
        priority: 1,
      });
      
      vscode.window.showInformationMessage('Selection added to context');
    }),
    
    vscode.commands.registerCommand('symboContext.addActiveFile', async () => {
      const editor = vscode.window.activeTextEditor;
      if (!editor) {
        vscode.window.showWarningMessage('No active file');
        return;
      }
      
      const content = editor.document.getText();
      const fileName = editor.document.fileName;
      
      await contextStore.addBlock({
        type: 'file',
        title: `File: ${fileName}`,
        source: fileName,
        language: editor.document.languageId,
        content,
        include: true,
        priority: 2,
      });
      
      vscode.window.showInformationMessage('Active file added to context');
    }),
    
    vscode.commands.registerCommand('symboContext.addFilesByGlob', async () => {
      const pattern = await vscode.window.showInputBox({
        placeHolder: '**/*.ts',
        prompt: 'Enter glob pattern for files to add',
      });
      
      if (!pattern) {return;}
      
      const files = await vscode.workspace.findFiles(pattern);
      let addedCount = 0;
      
      for (const file of files) {
        try {
          const doc = await vscode.workspace.openTextDocument(file);
          await contextStore.addBlock({
            type: 'file',
            title: `File: ${file.fsPath}`,
            source: file.fsPath,
            language: doc.languageId,
            content: doc.getText(),
            include: true,
            priority: 3,
          });
          addedCount++;
        } catch (error) {
          console.error(`Failed to add file ${file.fsPath}:`, error);
        }
      }
      
      vscode.window.showInformationMessage(`Added ${addedCount} files to context`);
    }),
    
    vscode.commands.registerCommand('symboContext.addGitDiff', async () => {
      try {
        const diff = await gitService.getStagedDiff();
        if (!diff) {
          vscode.window.showWarningMessage('No staged changes found');
          return;
        }
        
        await contextStore.addBlock({
          type: 'diff',
          title: 'Staged Git Diff',
          source: 'git',
          content: diff,
          include: true,
          priority: 1,
        });
        
        vscode.window.showInformationMessage('Git diff added to context');
      } catch (error) {
        vscode.window.showErrorMessage(`Failed to get git diff: ${error}`);
      }
    }),
    
    vscode.commands.registerCommand('symboContext.addText', async () => {
      const text = await vscode.window.showInputBox({
        placeHolder: 'Enter text to add to context...',
        prompt: 'Add arbitrary text to context',
      });
      
      if (!text) {return;}
      
      const title = await vscode.window.showInputBox({
        placeHolder: 'Context block title',
        prompt: 'Enter a title for this context block',
        value: 'Custom Text',
      });
      
      await contextStore.addBlock({
        type: 'text',
        title: title || 'Custom Text',
        content: text,
        include: true,
        priority: 2,
      });
      
      vscode.window.showInformationMessage('Text added to context');
    }),
    
    vscode.commands.registerCommand('symboContext.addToolOutput', async () => {
      const config = vscode.workspace.getConfiguration('symboContext');
      const tools = config.get<Array<{ name: string; cmd: string; args: string[] }>>('tools', []);
      
      if (tools.length === 0) {
        vscode.window.showWarningMessage('No tools configured. Add tools in settings.');
        return;
      }
      
      const toolNames = tools.map(t => t.name);
      const selectedTool = await vscode.window.showQuickPick(toolNames, {
        placeHolder: 'Select a tool to run',
      });
      
      if (!selectedTool) {return;}
      
      const tool = tools.find(t => t.name === selectedTool);
      if (!tool) {return;}
      
      try {
        const output = await toolBridge.runTool(tool.cmd, tool.args);
        
        await contextStore.addBlock({
          type: 'tool',
          title: `Tool: ${tool.name}`,
          source: tool.cmd,
          content: output,
          include: true,
          priority: 2,
        });
        
        vscode.window.showInformationMessage(`Tool output added to context`);
      } catch (error) {
        vscode.window.showErrorMessage(`Failed to run tool: ${error}`);
      }
    }),
    
    vscode.commands.registerCommand('symboContext.clearContext', async () => {
      const confirm = await vscode.window.showWarningMessage(
        'Clear all context blocks?',
        { modal: true },
        'Clear'
      );
      
      if (confirm === 'Clear') {
        contextStore.clearAll();
        vscode.window.showInformationMessage('Context cleared');
      }
    }),
    
    vscode.commands.registerCommand('symboContext.settings', () => {
      vscode.commands.executeCommand('workbench.action.openSettings', 'symboContext');
    }),
  ];

  context.subscriptions.push(...commands);

  // Clean up expired context blocks periodically
  const cleanupInterval = setInterval(() => {
    contextStore.cleanupExpired();
  }, 60000); // Check every minute

  context.subscriptions.push({
    dispose: () => clearInterval(cleanupInterval),
  });
}

export function deactivate() {
  // Extension deactivation cleanup
}