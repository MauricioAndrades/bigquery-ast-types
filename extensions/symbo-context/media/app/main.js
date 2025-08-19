class ChatApp {
    constructor() {
        this.state = {
            messages: [],
            contextBlocks: [],
            currentProvider: 'openai',
            currentModel: 'gpt-4o-mini',
            tokenBudget: 8000,
            drawerOpen: false,
            streaming: false,
        };
        this.init();
    }
    init() {
        document.addEventListener('DOMContentLoaded', () => {
            this.setupElements();
            this.setupEventListeners();
            this.render();
            // Notify extension that webview is ready
            vscode.postMessage({ type: 'ready' });
        });
        // Listen for messages from extension
        window.addEventListener('message', event => {
            const message = event.data;
            this.handleMessage(message);
        });
    }
    setupElements() {
        this.chatContainer = document.getElementById('chat-container');
        this.contextDrawer = document.getElementById('context-drawer');
    }
    setupEventListeners() {
        // Handle keyboard shortcuts
        document.addEventListener('keydown', (e) => {
            if (e.ctrlKey || e.metaKey) {
                switch (e.key) {
                    case 'd':
                        e.preventDefault();
                        this.toggleDrawer();
                        break;
                    case 'Enter':
                        if (this.messageInput && document.activeElement === this.messageInput) {
                            e.preventDefault();
                            this.sendMessage();
                        }
                        break;
                }
            }
        });
    }
    handleMessage(message) {
        switch (message.type) {
            case 'init':
                this.state = message.state;
                this.render();
                break;
            case 'streamDelta':
                this.handleStreamDelta(message.messageId, message.delta);
                break;
            case 'streamEnd':
                this.handleStreamEnd(message.messageId);
                break;
            case 'error':
                this.showError(message.error);
                break;
            case 'contextUpdate':
                this.state.contextBlocks = message.blocks;
                this.renderContextDrawer();
                break;
        }
    }
    handleStreamDelta(messageId, delta) {
        const messageEl = document.querySelector(`[data-message-id="${messageId}"]`);
        if (messageEl) {
            const contentEl = messageEl.querySelector('.message-content');
            if (contentEl) {
                contentEl.textContent += delta;
                // Auto-scroll to bottom
                this.chatContainer.scrollTop = this.chatContainer.scrollHeight;
            }
        }
    }
    handleStreamEnd(messageId) {
        const messageEl = document.querySelector(`[data-message-id="${messageId}"]`);
        if (messageEl) {
            messageEl.classList.remove('streaming');
            // Re-render the message to apply syntax highlighting
            const message = this.state.messages.find(m => m.id === messageId);
            if (message) {
                message.streaming = false;
                this.renderMessage(message, messageEl);
            }
        }
    }
    showError(error) {
        const errorEl = document.createElement('div');
        errorEl.className = 'error-message';
        errorEl.textContent = error;
        const messagesEl = this.chatContainer.querySelector('.messages');
        if (messagesEl) {
            messagesEl.appendChild(errorEl);
        }
        // Auto-remove after 5 seconds
        setTimeout(() => {
            errorEl.remove();
        }, 5000);
    }
    render() {
        this.renderChat();
        this.renderContextDrawer();
        this.updateDrawerVisibility();
    }
    renderChat() {
        this.chatContainer.innerHTML = `
      <div class="chat-header">
        <div class="chat-title">
          <span class="provider-info">${this.state.currentProvider}/${this.state.currentModel}</span>
        </div>
        <div class="chat-actions">
          <button id="drawer-toggle" class="icon-button" title="Toggle Context Drawer">
            <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
              <path d="M2 3h12v1H2V3zm0 4h12v1H2V7zm0 4h8v1H2v-1z"/>
            </svg>
          </button>
          <button id="settings-button" class="icon-button" title="Settings">
            <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
              <path d="M8 4.5a3.5 3.5 0 100 7 3.5 3.5 0 000-7zM6.5 8a1.5 1.5 0 103 0 1.5 1.5 0 00-3 0z"/>
              <path d="M8 1a7 7 0 100 14A7 7 0 008 1zM1 8a7 7 0 1114 0A7 7 0 011 8z"/>
            </svg>
          </button>
        </div>
      </div>
      <div class="messages">
        ${this.state.messages.map(msg => this.renderMessageHTML(msg)).join('')}
      </div>
      <div class="chat-input">
        <div class="input-controls">
          <label class="checkbox-label">
            <input type="checkbox" id="use-context" ${this.getIncludedBlocks().length > 0 ? 'checked' : ''}>
            <span>Include context (${this.getIncludedBlocks().length} blocks, ${this.getTotalTokens()} tokens)</span>
          </label>
        </div>
        <div class="input-row">
          <textarea 
            id="message-input" 
            placeholder="Type your message... (use / for commands)"
            rows="3"
          ></textarea>
          <button id="send-button" ${this.state.streaming ? 'disabled' : ''}>
            ${this.state.streaming ? 'Sending...' : 'Send'}
          </button>
        </div>
      </div>
    `;
        // Set up input elements
        this.messageInput = document.getElementById('message-input');
        this.useContextCheckbox = document.getElementById('use-context');
        this.sendButton = document.getElementById('send-button');
        // Set up event listeners
        document.getElementById('drawer-toggle')?.addEventListener('click', () => {
            this.toggleDrawer();
        });
        document.getElementById('settings-button')?.addEventListener('click', () => {
            vscode.postMessage({ type: 'openSettings' });
        });
        this.sendButton?.addEventListener('click', () => {
            this.sendMessage();
        });
        this.messageInput?.addEventListener('keydown', (e) => {
            if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
                e.preventDefault();
                this.sendMessage();
            }
        });
        // Auto-resize textarea
        this.messageInput?.addEventListener('input', () => {
            this.messageInput.style.height = 'auto';
            this.messageInput.style.height = this.messageInput.scrollHeight + 'px';
        });
        // Scroll to bottom
        setTimeout(() => {
            const messagesEl = this.chatContainer.querySelector('.messages');
            if (messagesEl) {
                messagesEl.scrollTop = messagesEl.scrollHeight;
            }
        }, 0);
    }
    renderMessageHTML(message) {
        return `
      <div class="message ${message.role} ${message.streaming ? 'streaming' : ''}" data-message-id="${message.id}">
        <div class="message-header">
          <span class="message-role">${message.role}</span>
          <span class="message-time">${new Date(message.timestamp).toLocaleTimeString()}</span>
        </div>
        <div class="message-content">${this.formatMessageContent(message.content)}</div>
        ${message.contextBlocks ? this.renderContextBlocks(message.contextBlocks) : ''}
      </div>
    `;
    }
    renderMessage(message, element) {
        element.innerHTML = `
      <div class="message-header">
        <span class="message-role">${message.role}</span>
        <span class="message-time">${new Date(message.timestamp).toLocaleTimeString()}</span>
      </div>
      <div class="message-content">${this.formatMessageContent(message.content)}</div>
      ${message.contextBlocks ? this.renderContextBlocks(message.contextBlocks) : ''}
    `;
    }
    formatMessageContent(content) {
        // Basic markdown-like formatting
        let formatted = content
            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
            .replace(/\*(.*?)\*/g, '<em>$1</em>')
            .replace(/`([^`]+)`/g, '<code>$1</code>');
        // Handle code blocks
        formatted = formatted.replace(/```(\w+)?\n([\s\S]*?)```/g, (match, lang, code) => {
            return this.renderCodeBlock(code.trim(), lang);
        });
        return formatted.replace(/\n/g, '<br>');
    }
    renderCodeBlock(code, language) {
        const id = 'code-' + Math.random().toString(36).substr(2, 9);
        return `
      <div class="code-block">
        <div class="code-header">
          <span class="code-lang">${language || 'text'}</span>
          <div class="code-actions">
            <button class="code-action" onclick="copyCode('${id}')">Copy</button>
            <button class="code-action" onclick="insertCode('${id}')">Insert</button>
            <button class="code-action" onclick="createFile('${id}')">Create File</button>
            <button class="code-action" onclick="stageAsPatch('${id}')">Stage Patch</button>
          </div>
        </div>
        <pre><code id="${id}">${this.escapeHtml(code)}</code></pre>
      </div>
    `;
    }
    renderContextBlocks(blocks) {
        if (!blocks || blocks.length === 0) {
            return '';
        }
        return `
      <div class="context-blocks">
        <div class="context-header">Context used (${blocks.length} blocks):</div>
        ${blocks.map(block => `
          <div class="context-block-summary">
            <span class="context-type">[${block.type}]</span>
            <span class="context-title">${block.title}</span>
            <span class="context-tokens">${block.estTokens} tokens</span>
          </div>
        `).join('')}
      </div>
    `;
    }
    renderContextDrawer() {
        const includedBlocks = this.getIncludedBlocks();
        const totalTokens = this.getTotalTokens();
        const budgetExceeded = totalTokens > this.state.tokenBudget;
        this.contextDrawer.innerHTML = `
      <div class="drawer-header">
        <h3>Context Blocks</h3>
        <button id="close-drawer" class="close-button">×</button>
      </div>
      <div class="drawer-content">
        <div class="context-summary">
          <div class="token-budget ${budgetExceeded ? 'exceeded' : ''}">
            ${totalTokens} / ${this.state.tokenBudget} tokens ${budgetExceeded ? '⚠️' : '✓'}
          </div>
          <div class="block-count">
            ${includedBlocks.length} / ${this.state.contextBlocks.length} blocks included
          </div>
        </div>
        
        <div class="add-context-actions">
          <button class="add-button" onclick="addSelection()">+ Selection</button>
          <button class="add-button" onclick="addActiveFile()">+ Active File</button>
          <button class="add-button" onclick="addGitDiff()">+ Git Diff</button>
          <button class="add-button" onclick="addText()">+ Text</button>
        </div>

        <div class="context-blocks-list">
          ${this.state.contextBlocks.map(block => this.renderContextBlockItem(block)).join('')}
        </div>
      </div>
    `;
        // Set up drawer event listeners
        document.getElementById('close-drawer')?.addEventListener('click', () => {
            this.closeDrawer();
        });
    }
    renderContextBlockItem(block) {
        const age = Math.round((Date.now() - block.createdAt) / 1000 / 60); // minutes
        return `
      <div class="context-block-item ${block.include ? 'included' : 'excluded'}" data-block-id="${block.id}">
        <div class="block-header">
          <div class="block-info">
            <span class="block-type">[${block.type}]</span>
            <span class="block-title">${block.title}</span>
          </div>
          <div class="block-actions">
            <button class="action-button toggle" onclick="toggleBlock('${block.id}')" title="Toggle Include">
              ${block.include ? '✓' : '○'}
            </button>
            <button class="action-button" onclick="previewBlock('${block.id}')" title="Preview">👁</button>
            <button class="action-button" onclick="removeBlock('${block.id}')" title="Remove">×</button>
          </div>
        </div>
        <div class="block-meta">
          <span class="block-size">${block.chars} chars, ${block.estTokens} tokens</span>
          <span class="block-age">${age}m ago</span>
          ${block.source ? `<span class="block-source">${block.source}</span>` : ''}
        </div>
      </div>
    `;
    }
    getIncludedBlocks() {
        return this.state.contextBlocks.filter(block => block.include);
    }
    getTotalTokens() {
        return this.getIncludedBlocks().reduce((sum, block) => sum + block.estTokens, 0);
    }
    sendMessage() {
        const message = this.messageInput?.value.trim();
        if (!message || this.state.streaming) {
            return;
        }
        const useContext = this.useContextCheckbox?.checked || false;
        vscode.postMessage({
            type: 'sendPrompt',
            prompt: message,
            useContext,
        });
        // Clear input
        this.messageInput.value = '';
        this.messageInput.style.height = 'auto';
    }
    toggleDrawer() {
        if (this.state.drawerOpen) {
            this.closeDrawer();
        }
        else {
            this.openDrawer();
        }
    }
    openDrawer() {
        this.state.drawerOpen = true;
        vscode.postMessage({ type: 'openDrawer' });
        this.updateDrawerVisibility();
    }
    closeDrawer() {
        this.state.drawerOpen = false;
        vscode.postMessage({ type: 'closeDrawer' });
        this.updateDrawerVisibility();
    }
    updateDrawerVisibility() {
        if (this.state.drawerOpen) {
            this.contextDrawer.classList.add('open');
        }
        else {
            this.contextDrawer.classList.remove('open');
        }
    }
    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}
// Global functions for button actions
window.copyCode = function (codeId) {
    const codeEl = document.getElementById(codeId);
    if (codeEl) {
        vscode.postMessage({
            type: 'copyCode',
            code: codeEl.textContent || '',
        });
    }
};
window.insertCode = function (codeId) {
    const codeEl = document.getElementById(codeId);
    if (codeEl) {
        vscode.postMessage({
            type: 'insertCode',
            code: codeEl.textContent || '',
        });
    }
};
window.createFile = function (codeId) {
    const codeEl = document.getElementById(codeId);
    if (codeEl) {
        const filename = prompt('Enter filename:');
        vscode.postMessage({
            type: 'createFile',
            code: codeEl.textContent || '',
            filename,
        });
    }
};
window.stageAsPatch = function (codeId) {
    const codeEl = document.getElementById(codeId);
    if (codeEl) {
        const filename = prompt('Enter patch name (optional):');
        vscode.postMessage({
            type: 'stageAsPatch',
            code: codeEl.textContent || '',
            filename,
        });
    }
};
window.toggleBlock = function (blockId) {
    vscode.postMessage({
        type: 'toggleContextBlock',
        id: blockId,
    });
};
window.removeBlock = function (blockId) {
    vscode.postMessage({
        type: 'removeContextBlock',
        id: blockId,
    });
};
window.previewBlock = function (blockId) {
    // TODO: Implement preview modal
    console.log('Preview block:', blockId);
};
window.addSelection = function () {
    vscode.postMessage({ type: 'executeCommand', command: 'symboContext.addSelection' });
};
window.addActiveFile = function () {
    vscode.postMessage({ type: 'executeCommand', command: 'symboContext.addActiveFile' });
};
window.addGitDiff = function () {
    vscode.postMessage({ type: 'executeCommand', command: 'symboContext.addGitDiff' });
};
window.addText = function () {
    vscode.postMessage({ type: 'executeCommand', command: 'symboContext.addText' });
};
// Initialize the app
new ChatApp();
