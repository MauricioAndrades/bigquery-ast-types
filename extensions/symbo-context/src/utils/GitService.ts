import * as vscode from 'vscode';
import * as child_process from 'child_process';
import * as util from 'util';

const exec = util.promisify(child_process.exec);

export class GitService {
  private async execGit(command: string): Promise<string> {
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
    } catch (error: any) {
      throw new Error(`Git command failed: ${error.message}`);
    }
  }

  async isGitRepository(): Promise<boolean> {
    try {
      await this.execGit('rev-parse --git-dir');
      return true;
    } catch {
      return false;
    }
  }

  async getStagedDiff(): Promise<string | null> {
    if (!(await this.isGitRepository())) {
      return null;
    }

    try {
      const diff = await this.execGit('diff --cached');
      return diff || null;
    } catch (error) {
      console.error('Failed to get staged diff:', error);
      return null;
    }
  }

  async getWorkingDiff(): Promise<string | null> {
    if (!(await this.isGitRepository())) {
      return null;
    }

    try {
      const diff = await this.execGit('diff');
      return diff || null;
    } catch (error) {
      console.error('Failed to get working diff:', error);
      return null;
    }
  }

  async getFullDiff(): Promise<string | null> {
    if (!(await this.isGitRepository())) {
      return null;
    }

    try {
      const diff = await this.execGit('diff HEAD');
      return diff || null;
    } catch (error) {
      console.error('Failed to get full diff:', error);
      return null;
    }
  }

  async getDiffBetweenCommits(from: string, to: string): Promise<string | null> {
    if (!(await this.isGitRepository())) {
      return null;
    }

    try {
      const diff = await this.execGit(`diff ${from}..${to}`);
      return diff || null;
    } catch (error) {
      console.error('Failed to get diff between commits:', error);
      return null;
    }
  }

  async getCurrentBranch(): Promise<string | null> {
    if (!(await this.isGitRepository())) {
      return null;
    }

    try {
      const branch = await this.execGit('branch --show-current');
      return branch || null;
    } catch (error) {
      console.error('Failed to get current branch:', error);
      return null;
    }
  }

  async getRemoteUrl(): Promise<string | null> {
    if (!(await this.isGitRepository())) {
      return null;
    }

    try {
      const url = await this.execGit('config --get remote.origin.url');
      return url || null;
    } catch (error) {
      console.error('Failed to get remote URL:', error);
      return null;
    }
  }

  async getLastCommitHash(): Promise<string | null> {
    if (!(await this.isGitRepository())) {
      return null;
    }

    try {
      const hash = await this.execGit('rev-parse HEAD');
      return hash || null;
    } catch (error) {
      console.error('Failed to get last commit hash:', error);
      return null;
    }
  }

  async getRecentCommits(count: number = 10): Promise<Array<{ hash: string; message: string; author: string; date: string }>> {
    if (!(await this.isGitRepository())) {
      return [];
    }

    try {
      const log = await this.execGit(`log --oneline --format="%H|%s|%an|%ad" --date=short -n ${count}`);
      return log.split('\n').filter(line => line.trim()).map(line => {
        const [hash, message, author, date] = line.split('|');
        return { hash, message, author, date };
      });
    } catch (error) {
      console.error('Failed to get recent commits:', error);
      return [];
    }
  }

  async stageAllChanges(): Promise<void> {
    if (!(await this.isGitRepository())) {
      throw new Error('Not in a git repository');
    }

    await this.execGit('add .');
  }

  async createPatchFile(filename: string, content: string): Promise<void> {
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