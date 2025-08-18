import { ContextStore } from '../context/ContextStore';
import { GitService } from '../utils/GitService';
import { ToolBridge } from '../utils/ToolBridge';
export interface CommandResult {
    handled: boolean;
    response?: string;
}
export declare class SlashCommandProcessor {
    private contextStore;
    private gitService;
    private toolBridge;
    constructor(contextStore: ContextStore, gitService: GitService, toolBridge: ToolBridge);
    processCommand(input: string): Promise<CommandResult>;
    private handleAddContext;
    private handleAddSelection;
    private handleAddFile;
    private handleAddDiff;
    private handleAddText;
    private handleAddTool;
    private handleClear;
    private handleList;
    private handleRemove;
    private handleSettings;
    private handleStatus;
    private handleExport;
    private handleImport;
    private handleHelp;
}
//# sourceMappingURL=SlashCommandProcessor.d.ts.map