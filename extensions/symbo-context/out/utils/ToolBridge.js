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
exports.ToolBridge = void 0;
const vscode = __importStar(require("vscode"));
const child_process = __importStar(require("child_process"));
const util = __importStar(require("util"));
const exec = util.promisify(child_process.exec);
class ToolBridge {
    async runTool(command, args = []) {
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
        }
        catch (error) {
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
    async runToolDetailed(command, args = []) {
        const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
        if (!workspaceFolder) {
            throw new Error('No workspace folder found');
        }
        const startTime = Date.now();
        const fullCommand = `${command} ${args.join(' ')}`;
        return new Promise((resolve) => {
            const child = child_process.exec(fullCommand, {
                cwd: workspaceFolder.uri.fsPath,
                timeout: 60000,
                maxBuffer: 1024 * 1024 * 10,
            }, (error, stdout, stderr) => {
                const duration = Date.now() - startTime;
                const exitCode = error ? error.code || 1 : 0;
                resolve({
                    stdout: stdout || '',
                    stderr: stderr || '',
                    exitCode,
                    duration,
                });
            });
        });
    }
    async validateTool(command) {
        try {
            // Try to run the command with --help or --version to see if it exists
            const helpCommands = ['--help', '--version', '-h', '-v'];
            for (const helpFlag of helpCommands) {
                try {
                    await exec(`${command} ${helpFlag}`, { timeout: 5000 });
                    return true;
                }
                catch {
                    // Try next flag
                    continue;
                }
            }
            // Try running the command directly with no args
            try {
                await exec(command, { timeout: 5000 });
                return true;
            }
            catch {
                return false;
            }
        }
        catch {
            return false;
        }
    }
    async getAvailableTools() {
        const config = vscode.workspace.getConfiguration('symboContext');
        const tools = config.get('tools', []);
        const availableTools = [];
        for (const tool of tools) {
            if (await this.validateTool(tool.cmd)) {
                availableTools.push(tool.name);
            }
        }
        return availableTools;
    }
    async runPredefinedTool(toolName, additionalArgs = []) {
        const config = vscode.workspace.getConfiguration('symboContext');
        const tools = config.get('tools', []);
        const tool = tools.find(t => t.name === toolName);
        if (!tool) {
            throw new Error(`Tool '${toolName}' not found in configuration`);
        }
        const allArgs = [...tool.args, ...additionalArgs];
        return this.runTool(tool.cmd, allArgs);
    }
    sanitizeOutput(output) {
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
    async executeContextQuery(query) {
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
        }
        catch (error) {
            console.error('Context query failed:', error);
            return `Context query failed: ${error}`;
        }
    }
}
exports.ToolBridge = ToolBridge;
//# sourceMappingURL=ToolBridge.js.map