# Symbo Context Chat

A powerful VS Code and Cursor extension that provides a chat interface with intelligent context management. Features a slide-over context drawer for curating and managing context blocks to enhance AI conversations.

## Features

### 🚀 Core Chat Interface
- **Streaming AI responses** with real-time token display
- **Code block rendering** with syntax highlighting  
- **Code actions**: Copy, Insert into editor, Create new file, Stage as patch
- **Multi-provider support**: OpenAI, Azure OpenAI, Custom HTTP endpoints
- **Message history** with system, developer, user, and assistant roles

### 📋 Context Management
- **Slide-over drawer** with context block management
- **Smart token budgeting** with overflow handling
- **Context sources**: File contents, code selections, git diffs, tool outputs, arbitrary text
- **Include/exclude toggles** for granular context control
- **Drag & drop reordering** with priority management
- **Auto-deduplication** and TTL-based cleanup

### ⚡ Slash Commands
- `/add-context <type>` - Add context blocks
- `/add-selection` - Add current selection
- `/add-file [path]` - Add file contents
- `/add-diff [type]` - Add git changes (staged/working/full)
- `/add-text <text>` - Add arbitrary text
- `/add-tool <tool>` - Run CLI tools and add output
- `/clear` - Clear all context
- `/list` - Show context blocks
- `/status` - Show extension status

### 🔧 Developer Tools
- **Git integration** with diff support
- **CLI tool bridge** for custom context generation
- **Secret redaction** for security
- **Export/import** context as JSON
- **Workspace state persistence**

## Installation

### From VSIX
1. Download the `.vsix` file from releases
2. In VS Code: `Extensions > Install from VSIX...`
3. In Cursor: Drag and drop the `.vsix` file

### Manual Build
```bash
cd extensions/symbo-context
pnpm install
pnpm compile
pnpm vsix
```

## Quick Start

### 1. Configure Provider
Open settings (`Ctrl/Cmd + ,`) and search for "Symbo Context":

**For OpenAI:**
```json
{
  "symboContext.provider": "openai",
  "symboContext.openai.apiKey": "your-api-key",
  "symboContext.openai.model": "gpt-4o-mini"
}
```

**For Azure OpenAI:**
```json
{
  "symboContext.provider": "azure", 
  "symboContext.azure.endpoint": "https://your-resource.openai.azure.com",
  "symboContext.azure.apiKey": "your-key",
  "symboContext.azure.deployment": "your-deployment-name"
}
```

**For Custom HTTP:**
```json
{
  "symboContext.provider": "http",
  "symboContext.http.endpoint": "http://localhost:8080/chat"
}
```

### 2. Open Chat
- **Activity Bar**: Click the Symbo Context icon
- **Command**: `Ctrl/Cmd + Alt + C` or `Symbo Context: Open Chat`
- **Command Palette**: `F1` → `Symbo Context: Open Chat`

### 3. Add Context
- **Context Drawer**: `Ctrl/Cmd + Alt + D` or click drawer icon
- **Quick Add**: `Ctrl/Cmd + Alt + A` (selection), `Ctrl/Cmd + Alt + G` (git diff)
- **Slash Commands**: Type `/add-selection`, `/add-file`, etc. in chat

### 4. Chat with Context
1. Add relevant files/selections to context drawer
2. Toggle "Include context" checkbox in chat input
3. Type your message and send
4. AI receives your message + selected context blocks

## Settings Reference

| Setting | Description | Default |
|---------|-------------|---------|
| `symboContext.provider` | LLM provider (openai/azure/http) | `"openai"` |
| `symboContext.tokenBudget` | Max tokens for context | `8000` |
| `symboContext.defaultContextTTL` | Block lifetime (ms) | `3600000` |
| `symboContext.enableTelemetry` | Enable usage telemetry | `false` |
| `symboContext.tools` | CLI tools for context generation | `[]` |

### OpenAI Settings
- `symboContext.openai.apiKey` - API key
- `symboContext.openai.baseUrl` - Base URL (optional)  
- `symboContext.openai.model` - Model name

### Azure OpenAI Settings
- `symboContext.azure.endpoint` - Azure endpoint
- `symboContext.azure.apiKey` - API key
- `symboContext.azure.deployment` - Deployment name

### HTTP Settings
- `symboContext.http.endpoint` - Custom endpoint URL

## Context Block Types

| Type | Description | Source Examples |
|------|-------------|-----------------|
| **file** | Complete file contents | Active editor, file picker |
| **selection** | Code selections | Editor selection |
| **diff** | Git changes | Staged, working, HEAD |
| **text** | Arbitrary text | User input, notes |
| **tool** | CLI command output | git log, npm list, custom scripts |

## CLI Tools Configuration

Add custom tools to generate context automatically:

```json
{
  "symboContext.tools": [
    {
      "name": "git-status", 
      "cmd": "git",
      "args": ["status", "--porcelain"]
    },
    {
      "name": "deps-check",
      "cmd": "npm",
      "args": ["list", "--depth=0"]
    },
    {
      "name": "context-query",
      "cmd": "python3",
      "args": ["./scripts/context-query.py"]
    }
  ]
}
```

Use with `/add-tool <name>` or via context drawer.

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl/Cmd + Alt + C` | Open chat |
| `Ctrl/Cmd + Alt + D` | Toggle context drawer |  
| `Ctrl/Cmd + Alt + A` | Add selection to context |
| `Ctrl/Cmd + Alt + G` | Add git diff to context |
| `Ctrl/Cmd + Enter` | Send message (in chat input) |

## Code Actions

Code blocks in chat responses include action buttons:

- **Copy**: Copy code to clipboard
- **Insert**: Insert at cursor position  
- **Create File**: Save as new file in workspace
- **Stage Patch**: Create `.patch` file for review

## Token Management

The extension tracks token usage and provides budget management:

- **Token Estimation**: ~4 characters per token (configurable)
- **Budget Alerts**: Warning when context exceeds limit
- **Smart Packing**: Automatic prioritization and trimming
- **Real-time Display**: Live token counts in UI

## Git Integration

Seamless git workflow integration:

- **Diff Context**: Add staged, working, or HEAD diffs
- **Branch Awareness**: Context includes current branch info
- **Patch Generation**: Create patch files from code suggestions
- **Secret Detection**: Automatic redaction of sensitive data

## Troubleshooting

### API Key Issues
```
Error: OpenAI API key not configured
```
→ Set `symboContext.openai.apiKey` in settings

### Context Budget Exceeded
```
Warning: Context exceeds token budget (12000 > 8000)
```
→ Remove context blocks or increase `symboContext.tokenBudget`

### Git Commands Fail
```
Error: Git command failed
```
→ Ensure workspace has git repository and git is in PATH

### Extension Not Loading
1. Check VS Code version ≥ 1.85
2. Reload window (`Ctrl/Cmd + R`)
3. Check extension is enabled in Extensions panel

### HTTP Provider Issues
```
Error: HTTP provider error: 404
```
→ Verify `symboContext.http.endpoint` URL is correct and accessible

## Development

### Build from Source
```bash
git clone <repo>
cd extensions/symbo-context
pnpm install
pnpm compile
```

### Run Tests  
```bash
pnpm test
```

### Package Extension
```bash
pnpm vsix
```

### Debug
1. Open in VS Code
2. Press `F5` to launch Extension Development Host
3. Test extension in new window

## API Compatibility

### VS Code API
- Minimum engine: `^1.85.0`
- Uses stable APIs only (no proposed APIs)
- Compatible with VS Code and Cursor

### HTTP Provider Format
Expected request/response format for custom providers:

**Request:**
```json
{
  "messages": [
    {"role": "user", "content": "Hello"},
    {"role": "assistant", "content": "Hi there!"}
  ],
  "stream": true
}
```

**Streaming Response:**
```
data: {"choices":[{"delta":{"content":"Hello"}}]}
data: {"choices":[{"delta":{"content":" world"}}]}
data: [DONE]
```

## Privacy & Security

- **Local Storage**: Context blocks stored in VS Code workspace state
- **Secret Redaction**: Automatic redaction of API keys, tokens, JWTs
- **Opt-in Telemetry**: Usage analytics disabled by default
- **No Data Sharing**: Extension doesn't transmit data except to configured LLM provider

## Contributing

1. Fork repository
2. Create feature branch
3. Add tests for new functionality  
4. Ensure all tests pass
5. Submit pull request

## License

MIT License - see LICENSE file for details.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history.

---

**Made with ❤️ for the VS Code and Cursor communities**