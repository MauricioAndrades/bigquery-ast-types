import * as assert from 'assert';
import * as vscode from 'vscode';
import { ContextStore } from '../../src/context/ContextStore';

suite('Extension Test Suite', () => {
  vscode.window.showInformationMessage('Start all tests.');

  test('Extension should be present', () => {
    assert.ok(vscode.extensions.getExtension('symbo.symbo-context-chat'));
  });

  test('Should activate', async () => {
    const extension = vscode.extensions.getExtension('symbo.symbo-context-chat')!;
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
  let contextStore: ContextStore;

  setup(() => {
    // Create a mock context for testing
    const mockContext = {
      workspaceState: {
        get: () => [],
        update: () => Promise.resolve(),
      },
    } as any;
    contextStore = new ContextStore(mockContext);
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
    assert.strictEqual(block!.include, true);

    await contextStore.toggleInclude(blockId);
    block = contextStore.getBlock(blockId);
    assert.strictEqual(block!.include, false);
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
    assert.ok(block!.content.includes('API_KEY=****'));
    assert.ok(block!.content.includes('BEARER_TOKEN=****'));
    assert.ok(!block!.content.includes('secret123'));
    assert.ok(!block!.content.includes('abc456'));
  });
});