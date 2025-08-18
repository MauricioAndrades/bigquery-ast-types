import * as vscode from 'vscode';
import * as child_process from 'child_process';
import * as util from 'util';

const exec = util.promisify(child_process.exec);

export interface ToolResult {
  stdout: string;
  stderr: string;
  exitCode: number;
  duration: number;
}

export class ToolBridge {
  async runTool(command: string, args: string[] = []): Promise<string> {
    const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
    if (!workspaceFolder) {
      throw new Error('No workspace folder found');
    }

    const startTime = Date.now();
    const fullCommand = `${command} ${args.join(' ')}`;

    try {
      const { stdout, stderr } = await exec(fullCommand, {
        cwd: workspaceFolder.uri.fsPath,
        timeout: 60000, // 1 minute timeout
        maxBuffer: 1024 * 1024 * 10, // 10MB buffer
      });

      const duration = Date.now() - startTime;
      
      // Log the execution for debugging
      console.log(`Tool executed: ${fullCommand} (${duration}ms)`);
      
      if (stderr && stderr.trim()) {
        console.warn(`Tool stderr: ${stderr}`);
        // Include stderr in output if it's not empty but don't throw
        return `${stdout}\n\n--- stderr ---\n${stderr}`;
      }

      return stdout;
    } catch (error: any) {
      const duration = Date.now() - startTime;
      console.error(`Tool failed: ${fullCommand} (${duration}ms)`, error);
      
      if (error.code === 'ETIMEDOUT') {
        throw new Error(`Tool timed out after 60 seconds: ${command}`);
      }
      
      // Include error output if available
      const errorOutput = error.stdout || error.stderr || error.message;
      throw new Error(`Tool execution failed: ${errorOutput}`);
    }
  }

  async runToolDetailed(command: string, args: string[] = []): Promise<ToolResult> {
    const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
    if (!workspaceFolder) {
      throw new Error('No workspace folder found');
    }

    const startTime = Date.now();
    const fullCommand = `${command} ${args.join(' ')}`;

    return new Promise((resolve) => {
      const child = child_process.exec(
        fullCommand,
        {
          cwd: workspaceFolder.uri.fsPath,
          timeout: 60000,
          maxBuffer: 1024 * 1024 * 10,
        },
        (error, stdout, stderr) => {
          const duration = Date.now() - startTime;
          const exitCode = error ? (error as any).code || 1 : 0;
          
          resolve({
            stdout: stdout || '',
            stderr: stderr || '',
            exitCode,
            duration,
          });
        }
      );
    });
  }

  async validateTool(command: string): Promise<boolean> {
    try {
      // Try to run the command with --help or --version to see if it exists
      const helpCommands = ['--help', '--version', '-h', '-v'];
      
      for (const helpFlag of helpCommands) {
        try {
          await exec(`${command} ${helpFlag}`, { timeout: 5000 });
          return true;
        } catch {
          // Try next flag
          continue;
        }
      }
      
      // Try running the command directly with no args
      try {
        await exec(command, { timeout: 5000 });
        return true;
      } catch {
        return false;
      }
    } catch {
      return false;
    }
  }

  async getAvailableTools(): Promise<string[]> {
    const config = vscode.workspace.getConfiguration('symboContext');
    const tools = config.get<Array<{ name: string; cmd: string; args: string[] }>>('tools', []);
    
    const availableTools: string[] = [];
    
    for (const tool of tools) {
      if (await this.validateTool(tool.cmd)) {
        availableTools.push(tool.name);
      }
    }
    
    return availableTools;
  }

  async runPredefinedTool(toolName: string, additionalArgs: string[] = []): Promise<string> {
    const config = vscode.workspace.getConfiguration('symboContext');
    const tools = config.get<Array<{ name: string; cmd: string; args: string[] }>>('tools', []);
    
    const tool = tools.find(t => t.name === toolName);
    if (!tool) {
      throw new Error(`Tool '${toolName}' not found in configuration`);
    }
    
    const allArgs = [...tool.args, ...additionalArgs];
    return this.runTool(tool.cmd, allArgs);
  }

  sanitizeOutput(output: string): string {
    // Remove ANSI escape codes
    let sanitized = output.replace(/\x1b\[[0-9;]*m/g, '');
    
    // Basic secret redaction (same patterns as ContextStore)
    const secretPatterns = [
      /([A-Z_]+_KEY\s*[=:]\s*)([^\s\n]+)/gi,
      /([A-Z_]+_SECRET\s*[=:]\s*)([^\s\n]+)/gi,
      /([A-Z_]+_TOKEN\s*[=:]\s*)([^\s\n]+)/gi,
      /(Bearer\s+)([a-zA-Z0-9._-]+)/gi,
      /(eyJ[a-zA-Z0-9._-]+)/gi, // JWT tokens
    ];

    secretPatterns.forEach(pattern => {
      sanitized = sanitized.replace(pattern, (match, prefix, secret) => {
        return prefix + '****';
      });
    });

    return sanitized;
  }

  async executeContextQuery(query: string): Promise<string> {
    // This is a placeholder for context-specific queries
    // In a real implementation, this might interface with
    // external context analysis tools
    
    const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
    if (!workspaceFolder) {
      throw new Error('No workspace folder found');
    }

    try {
      // Example: run a simple grep-based context query
      const grepCommand = `grep -r "${query}" --include="*.ts" --include="*.js" --include="*.py" . | head -20`;
      const result = await this.runTool('bash', ['-c', grepCommand]);
      
      return this.sanitizeOutput(result);
    } catch (error) {
      console.error('Context query failed:', error);
      return `Context query failed: ${error}`;
    }
  }
}