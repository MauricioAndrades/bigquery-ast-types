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
const assert = __importStar(require("assert"));
const vscode = __importStar(require("vscode"));
const ContextStore_1 = require("../../src/context/ContextStore");
suite('Extension Test Suite', () => {
    vscode.window.showInformationMessage('Start all tests.');
    test('Extension should be present', () => {
        assert.ok(vscode.extensions.getExtension('symbo.symbo-context-chat'));
    });
    test('Should activate', async () => {
        const extension = vscode.extensions.getExtension('symbo.symbo-context-chat');
        await extension.activate();
        assert.strictEqual(extension.isActive, true);
    });
    test('Commands should be registered', async () => {
        const commands = await vscode.commands.getCommands(true);
        const expectedCommands = [
            'symboContext.open',
            'symboContext.openDrawer',
            'symboContext.addSelection',
            'symboContext.addActiveFile',
            'symboContext.clearContext',
        ];
        expectedCommands.forEach(cmd => {
            assert.ok(commands.includes(cmd), `Command ${cmd} not found`);
        });
    });
});
suite('ContextStore Tests', () => {
    let contextStore;
    setup(() => {
        // Create a mock context for testing
        const mockContext = {
            workspaceState: {
                get: () => [],
                update: () => Promise.resolve(),
            },
        };
        contextStore = new ContextStore_1.ContextStore(mockContext);
    });
    test('Should add context blocks', async () => {
        const blockId = await contextStore.addBlock({
            type: 'text',
            title: 'Test Block',
            content: 'Test content',
            include: true,
            priority: 1,
        });
        assert.ok(blockId);
        const block = contextStore.getBlock(blockId);
        assert.ok(block);
        assert.strictEqual(block.title, 'Test Block');
        assert.strictEqual(block.content, 'Test content');
    });
    test('Should create packing plan', async () => {
        await contextStore.addBlock({
            type: 'text',
            title: 'Small Block',
            content: 'Short text',
            include: true,
            priority: 1,
        });
        await contextStore.addBlock({
            type: 'text',
            title: 'Large Block',
            content: 'Very '.repeat(1000) + 'long text',
            include: true,
            priority: 2,
        });
        const plan = contextStore.createPackingPlan();
        assert.ok(plan.selectedBlocks.length === 2);
        assert.ok(plan.totalTokens > 0);
    });
    test('Should toggle block inclusion', async () => {
        const blockId = await contextStore.addBlock({
            type: 'text',
            title: 'Toggle Test',
            content: 'Content',
            include: true,
            priority: 1,
        });
        let block = contextStore.getBlock(blockId);
        assert.strictEqual(block.include, true);
        await contextStore.toggleInclude(blockId);
        block = contextStore.getBlock(blockId);
        assert.strictEqual(block.include, false);
    });
    test('Should remove blocks', async () => {
        const blockId = await contextStore.addBlock({
            type: 'text',
            title: 'Remove Test',
            content: 'Content',
            include: true,
            priority: 1,
        });
        assert.ok(contextStore.getBlock(blockId));
        await contextStore.removeBlock(blockId);
        assert.strictEqual(contextStore.getBlock(blockId), undefined);
    });
    test('Should redact secrets', async () => {
        const blockId = await contextStore.addBlock({
            type: 'text',
            title: 'Secret Test',
            content: 'API_KEY=secret123\nBEARER_TOKEN=abc456',
            include: true,
            priority: 1,
        });
        const block = contextStore.getBlock(blockId);
        assert.ok(block.content.includes('API_KEY=****'));
        assert.ok(block.content.includes('BEARER_TOKEN=****'));
        assert.ok(!block.content.includes('secret123'));
        assert.ok(!block.content.includes('abc456'));
    });
});
//# sourceMappingURL=extension.test.js.map