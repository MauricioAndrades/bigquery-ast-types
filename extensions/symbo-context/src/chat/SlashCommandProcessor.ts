import * as vscode from 'vscode';
import { ContextStore } from '../context/ContextStore';
import { GitService } from '../utils/GitService';
import { ToolBridge } from '../utils/ToolBridge';

export interface CommandResult {
  handled: boolean;
  response?: string;
}

export class SlashCommandProcessor {
  constructor(
    private contextStore: ContextStore,
    private gitService: GitService,
    private toolBridge: ToolBridge
  ) {}

  async processCommand(input: string): Promise<CommandResult> {
    const trimmed = input.trim();
    if (!trimmed.startsWith('/')) {
      return { handled: false };
    }

    const parts = trimmed.slice(1).split(/\s+/);
    const command = parts[0].toLowerCase();
    const args = parts.slice(1);

    switch (command) {
      case 'add-context':
      case 'add':
        return this.handleAddContext(args);

      case 'add-selection':
        return this.handleAddSelection();

      case 'add-file':
        return this.handleAddFile(args);

      case 'add-diff':
        return this.handleAddDiff(args);

      case 'add-text':
        return this.handleAddText(args);

      case 'add-tool':
        return this.handleAddTool(args);

      case 'clear':
        return this.handleClear();

      case 'list':
      case 'show':
        return this.handleList();

      case 'remove':
      case 'rm':
        return this.handleRemove(args);

      case 'settings':
        return this.handleSettings();

      case 'help':
        return this.handleHelp();

      case 'status':
        return this.handleStatus();

      case 'export':
        return this.handleExport();

      case 'import':
        return this.handleImport(args);

      default:
        return {
          handled: true,
          response: `Unknown command: /${command}. Type /help for available commands.`,
        };
    }
  }

  private async handleAddContext(args: string[]): Promise<CommandResult> {
    if (args.length === 0) {
      return {
        handled: true,
        response: 'Usage: /add-context <selection|file|diff|text|tool>',
      };
    }

    const type = args[0].toLowerCase();
    const subArgs = args.slice(1);

    switch (type) {
      case 'selection':
        return this.handleAddSelection();
      case 'file':
        return this.handleAddFile(subArgs);
      case 'diff':
        return this.handleAddDiff(subArgs);
      case 'text':
        return this.handleAddText(subArgs);
      case 'tool':
        return this.handleAddTool(subArgs);
      default:
        return {
          handled: true,
          response: `Unknown context type: ${type}. Available: selection, file, diff, text, tool`,
        };
    }
  }

  private async handleAddSelection(): Promise<CommandResult> {
    const editor = vscode.window.activeTextEditor;
    if (!editor || editor.selection.isEmpty) {
      return {
        handled: true,
        response: 'No text selected in active editor',
      };
    }

    const selectedText = editor.document.getText(editor.selection);
    const fileName = editor.document.fileName;

    await this.contextStore.addBlock({
      type: 'selection',
      title: `Selection from ${fileName}`,
      source: fileName,
      language: editor.document.languageId,
      content: selectedText,
      include: true,
      priority: 1,
    });

    return {
      handled: true,
      response: `Added selection from ${fileName} to context (${selectedText.length} chars)`,
    };
  }

  private async handleAddFile(args: string[]): Promise<CommandResult> {
    if (args.length === 0) {
      const editor = vscode.window.activeTextEditor;
      if (!editor) {
        return {
          handled: true,
          response: 'No active file. Usage: /add-file <path>',
        };
      }

      const content = editor.document.getText();
      const fileName = editor.document.fileName;

      await this.contextStore.addBlock({
        type: 'file',
        title: `File: ${fileName}`,
        source: fileName,
        language: editor.document.languageId,
        content,
        include: true,
        priority: 2,
      });

      return {
        handled: true,
        response: `Added active file ${fileName} to context (${content.length} chars)`,
      };
    }

    const filePath = args[0];
    
    try {
      const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
      if (!workspaceFolder) {
        return {
          handled: true,
          response: 'No workspace folder found',
        };
      }

      const fileUri = vscode.Uri.joinPath(workspaceFolder.uri, filePath);
      const document = await vscode.workspace.openTextDocument(fileUri);

      await this.contextStore.addBlock({
        type: 'file',
        title: `File: ${filePath}`,
        source: fileUri.fsPath,
        language: document.languageId,
        content: document.getText(),
        include: true,
        priority: 2,
      });

      return {
        handled: true,
        response: `Added file ${filePath} to context (${document.getText().length} chars)`,
      };
    } catch (error) {
      return {
        handled: true,
        response: `Failed to add file: ${error}`,
      };
    }
  }

  private async handleAddDiff(args: string[]): Promise<CommandResult> {
    const type = args.length > 0 ? args[0] : 'staged';
    
    try {
      let diff: string | null = null;
      let title = '';

      switch (type) {
        case 'staged':
          diff = await this.gitService.getStagedDiff();
          title = 'Staged Git Diff';
          break;
        case 'working':
          diff = await this.gitService.getWorkingDiff();
          title = 'Working Git Diff';
          break;
        case 'full':
          diff = await this.gitService.getFullDiff();
          title = 'Full Git Diff (HEAD)';
          break;
        default:
          return {
            handled: true,
            response: 'Usage: /add-diff [staged|working|full]',
          };
      }

      if (!diff) {
        return {
          handled: true,
          response: `No ${type} changes found`,
        };
      }

      await this.contextStore.addBlock({
        type: 'diff',
        title,
        source: 'git',
        content: diff,
        include: true,
        priority: 1,
      });

      return {
        handled: true,
        response: `Added ${type} git diff to context (${diff.length} chars)`,
      };
    } catch (error) {
      return {
        handled: true,
        response: `Failed to get git diff: ${error}`,
      };
    }
  }

  private async handleAddText(args: string[]): Promise<CommandResult> {
    const text = args.join(' ');
    if (!text) {
      return {
        handled: true,
        response: 'Usage: /add-text <your text here>',
      };
    }

    await this.contextStore.addBlock({
      type: 'text',
      title: 'Custom Text',
      content: text,
      include: true,
      priority: 2,
    });

    return {
      handled: true,
      response: `Added text to context (${text.length} chars)`,
    };
  }

  private async handleAddTool(args: string[]): Promise<CommandResult> {
    if (args.length === 0) {
      const available = await this.toolBridge.getAvailableTools();
      return {
        handled: true,
        response: `Available tools: ${available.join(', ')} or specify custom command`,
      };
    }

    const toolName = args[0];
    const toolArgs = args.slice(1);

    try {
      // Check if it's a predefined tool
      const config = vscode.workspace.getConfiguration('symboContext');
      const tools = config.get<Array<{ name: string; cmd: string; args: string[] }>>('tools', []);
      const predefinedTool = tools.find(t => t.name === toolName);

      let output: string;
      let title: string;

      if (predefinedTool) {
        output = await this.toolBridge.runPredefinedTool(toolName, toolArgs);
        title = `Tool: ${toolName}`;
      } else {
        // Treat as custom command
        output = await this.toolBridge.runTool(toolName, toolArgs);
        title = `Command: ${toolName}`;
      }

      output = this.toolBridge.sanitizeOutput(output);

      await this.contextStore.addBlock({
        type: 'tool',
        title,
        source: toolName,
        content: output,
        include: true,
        priority: 2,
      });

      return {
        handled: true,
        response: `Added tool output to context (${output.length} chars)`,
      };
    } catch (error) {
      return {
        handled: true,
        response: `Failed to run tool: ${error}`,
      };
    }
  }

  private async handleClear(): Promise<CommandResult> {
    await this.contextStore.clearAll();
    return {
      handled: true,
      response: 'All context blocks cleared',
    };
  }

  private async handleList(): Promise<CommandResult> {
    const blocks = this.contextStore.getAllBlocks();
    
    if (blocks.length === 0) {
      return {
        handled: true,
        response: 'No context blocks',
      };
    }

    const plan = this.contextStore.createPackingPlan();
    const includedCount = plan.selectedBlocks.length;
    
    const summary = blocks.map((block, idx) => {
      const included = block.include ? '✓' : '○';
      return `${idx + 1}. ${included} [${block.type}] ${block.title} (${block.estTokens} tokens)`;
    }).join('\n');

    return {
      handled: true,
      response: `Context blocks (${includedCount}/${blocks.length} included, ${plan.totalTokens} tokens):\n\n${summary}`,
    };
  }

  private async handleRemove(args: string[]): Promise<CommandResult> {
    if (args.length === 0) {
      return {
        handled: true,
        response: 'Usage: /remove <block-number>',
      };
    }

    const blockNum = parseInt(args[0]) - 1;
    const blocks = this.contextStore.getAllBlocks();

    if (blockNum < 0 || blockNum >= blocks.length) {
      return {
        handled: true,
        response: `Invalid block number. Use /list to see available blocks.`,
      };
    }

    const block = blocks[blockNum];
    await this.contextStore.removeBlock(block.id);

    return {
      handled: true,
      response: `Removed context block: ${block.title}`,
    };
  }

  private async handleSettings(): Promise<CommandResult> {
    vscode.commands.executeCommand('workbench.action.openSettings', 'symboContext');
    return {
      handled: true,
      response: 'Opening settings...',
    };
  }

  private async handleStatus(): Promise<CommandResult> {
    const blocks = this.contextStore.getAllBlocks();
    const plan = this.contextStore.createPackingPlan();
    const config = vscode.workspace.getConfiguration('symboContext');
    const provider = config.get<string>('provider', 'openai');
    const model = config.get<string>(`${provider}.model`, 'unknown');
    
    const gitRepo = await this.gitService.isGitRepository();
    const branch = gitRepo ? await this.gitService.getCurrentBranch() : null;
    
    const status = [
      `Provider: ${provider} (${model})`,
      `Context blocks: ${plan.selectedBlocks.length}/${blocks.length} included`,
      `Token usage: ${plan.totalTokens}/${config.get('tokenBudget', 8000)} ${plan.budgetExceeded ? '⚠️' : '✓'}`,
      `Git repository: ${gitRepo ? `✓ (${branch})` : '○'}`,
    ].join('\n');

    return {
      handled: true,
      response: status,
    };
  }

  private async handleExport(): Promise<CommandResult> {
    try {
      const json = await this.contextStore.exportToJSON();
      await vscode.env.clipboard.writeText(json);
      
      return {
        handled: true,
        response: 'Context exported to clipboard as JSON',
      };
    } catch (error) {
      return {
        handled: true,
        response: `Export failed: ${error}`,
      };
    }
  }

  private async handleImport(args: string[]): Promise<CommandResult> {
    try {
      const clipboardText = await vscode.env.clipboard.readText();
      if (!clipboardText) {
        return {
          handled: true,
          response: 'No JSON found in clipboard',
        };
      }

      await this.contextStore.importFromJSON(clipboardText);
      
      return {
        handled: true,
        response: 'Context imported from clipboard',
      };
    } catch (error) {
      return {
        handled: true,
        response: `Import failed: ${error}`,
      };
    }
  }

  private async handleHelp(): Promise<CommandResult> {
    const help = `Available slash commands:

Context Management:
/add-context <type>     - Add context (selection, file, diff, text, tool)  
/add-selection          - Add current selection to context
/add-file [path]        - Add file to context (current or specified)
/add-diff [type]        - Add git diff (staged, working, full)
/add-text <text>        - Add arbitrary text to context
/add-tool <tool> [args] - Run tool and add output to context

Management:
/list                   - Show all context blocks
/remove <number>        - Remove context block by number
/clear                  - Clear all context blocks
/status                 - Show extension status
/export                 - Export context to clipboard
/import                 - Import context from clipboard

Other:
/settings               - Open extension settings
/help                   - Show this help`;

    return {
      handled: true,
      response: help,
    };
  }
}