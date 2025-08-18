export interface ToolResult {
    stdout: string;
    stderr: string;
    exitCode: number;
    duration: number;
}
export declare class ToolBridge {
    runTool(command: string, args?: string[]): Promise<string>;
    runToolDetailed(command: string, args?: string[]): Promise<ToolResult>;
    validateTool(command: string): Promise<boolean>;
    getAvailableTools(): Promise<string[]>;
    runPredefinedTool(toolName: string, additionalArgs?: string[]): Promise<string>;
    sanitizeOutput(output: string): string;
    executeContextQuery(query: string): Promise<string>;
}
//# sourceMappingURL=ToolBridge.d.ts.map