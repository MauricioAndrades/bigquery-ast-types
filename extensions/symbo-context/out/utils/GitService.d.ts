export declare class GitService {
    private execGit;
    isGitRepository(): Promise<boolean>;
    getStagedDiff(): Promise<string | null>;
    getWorkingDiff(): Promise<string | null>;
    getFullDiff(): Promise<string | null>;
    getDiffBetweenCommits(from: string, to: string): Promise<string | null>;
    getCurrentBranch(): Promise<string | null>;
    getRemoteUrl(): Promise<string | null>;
    getLastCommitHash(): Promise<string | null>;
    getRecentCommits(count?: number): Promise<Array<{
        hash: string;
        message: string;
        author: string;
        date: string;
    }>>;
    stageAllChanges(): Promise<void>;
    createPatchFile(filename: string, content: string): Promise<void>;
}
//# sourceMappingURL=GitService.d.ts.map