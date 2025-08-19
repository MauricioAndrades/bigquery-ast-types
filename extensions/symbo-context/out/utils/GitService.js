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
exports.GitService = void 0;
const vscode = __importStar(require("vscode"));
const child_process = __importStar(require("child_process"));
const util = __importStar(require("util"));
const exec = util.promisify(child_process.exec);
class GitService {
    async execGit(command) {
        const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
        if (!workspaceFolder) {
            throw new Error('No workspace folder found');
        }
        try {
            const { stdout } = await exec(`git ${command}`, {
                cwd: workspaceFolder.uri.fsPath,
                timeout: 30000,
            });
            return stdout.trim();
        }
        catch (error) {
            throw new Error(`Git command failed: ${error.message}`);
        }
    }
    async isGitRepository() {
        try {
            await this.execGit('rev-parse --git-dir');
            return true;
        }
        catch {
            return false;
        }
    }
    async getStagedDiff() {
        if (!(await this.isGitRepository())) {
            return null;
        }
        try {
            const diff = await this.execGit('diff --cached');
            return diff || null;
        }
        catch (error) {
            console.error('Failed to get staged diff:', error);
            return null;
        }
    }
    async getWorkingDiff() {
        if (!(await this.isGitRepository())) {
            return null;
        }
        try {
            const diff = await this.execGit('diff');
            return diff || null;
        }
        catch (error) {
            console.error('Failed to get working diff:', error);
            return null;
        }
    }
    async getFullDiff() {
        if (!(await this.isGitRepository())) {
            return null;
        }
        try {
            const diff = await this.execGit('diff HEAD');
            return diff || null;
        }
        catch (error) {
            console.error('Failed to get full diff:', error);
            return null;
        }
    }
    async getDiffBetweenCommits(from, to) {
        if (!(await this.isGitRepository())) {
            return null;
        }
        try {
            const diff = await this.execGit(`diff ${from}..${to}`);
            return diff || null;
        }
        catch (error) {
            console.error('Failed to get diff between commits:', error);
            return null;
        }
    }
    async getCurrentBranch() {
        if (!(await this.isGitRepository())) {
            return null;
        }
        try {
            const branch = await this.execGit('branch --show-current');
            return branch || null;
        }
        catch (error) {
            console.error('Failed to get current branch:', error);
            return null;
        }
    }
    async getRemoteUrl() {
        if (!(await this.isGitRepository())) {
            return null;
        }
        try {
            const url = await this.execGit('config --get remote.origin.url');
            return url || null;
        }
        catch (error) {
            console.error('Failed to get remote URL:', error);
            return null;
        }
    }
    async getLastCommitHash() {
        if (!(await this.isGitRepository())) {
            return null;
        }
        try {
            const hash = await this.execGit('rev-parse HEAD');
            return hash || null;
        }
        catch (error) {
            console.error('Failed to get last commit hash:', error);
            return null;
        }
    }
    async getRecentCommits(count = 10) {
        if (!(await this.isGitRepository())) {
            return [];
        }
        try {
            const log = await this.execGit(`log --oneline --format="%H|%s|%an|%ad" --date=short -n ${count}`);
            return log.split('\n').filter(line => line.trim()).map(line => {
                const [hash, message, author, date] = line.split('|');
                return { hash, message, author, date };
            });
        }
        catch (error) {
            console.error('Failed to get recent commits:', error);
            return [];
        }
    }
    async stageAllChanges() {
        if (!(await this.isGitRepository())) {
            throw new Error('Not in a git repository');
        }
        await this.execGit('add .');
    }
    async createPatchFile(filename, content) {
        const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
        if (!workspaceFolder) {
            throw new Error('No workspace folder found');
        }
        const patchPath = vscode.Uri.joinPath(workspaceFolder.uri, `${filename}.patch`);
        await vscode.workspace.fs.writeFile(patchPath, Buffer.from(content));
        // Open the patch file in diff view
        const patchDocument = await vscode.workspace.openTextDocument(patchPath);
        await vscode.window.showTextDocument(patchDocument);
    }
}
exports.GitService = GitService;
//# sourceMappingURL=GitService.js.map