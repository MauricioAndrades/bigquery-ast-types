# Change Log

All notable changes to the "Symbo Context Chat" extension will be documented in this file.

## [0.1.0] - 2025-01-18

### Added
- Initial release of Symbo Context Chat extension
- Complete chat interface with streaming AI responses
- Slide-over context drawer for context block management  
- Multi-provider LLM support (OpenAI, Azure OpenAI, HTTP)
- Context block types: file, selection, diff, text, tool
- Comprehensive slash command system
- Git integration with diff support
- CLI tool bridge for custom context generation
- Code block rendering with syntax highlighting
- Code action buttons (copy, insert, create file, stage patch)
- Token budget management and estimation
- Secret redaction for security
- Context block include/exclude toggles
- Drag & drop reordering (UI foundation)
- Auto-deduplication and TTL cleanup
- Workspace state persistence
- Export/import context as JSON
- Keyboard shortcuts and accessibility support
- Comprehensive test suite
- Documentation and examples

### Features
- **Chat Interface**: Real-time streaming responses with message history
- **Context Management**: Smart context block system with budget controls
- **Provider Support**: OpenAI API, Azure OpenAI, and custom HTTP endpoints
- **Git Integration**: Add staged, working, and HEAD diffs to context
- **Tool Integration**: Run CLI commands and add output to context
- **Code Actions**: Insert code, create files, generate patches
- **Settings**: Extensive configuration options
- **Security**: Automatic secret redaction and secure handling

### Commands
- `symboContext.open` - Open chat panel
- `symboContext.openDrawer` - Toggle context drawer
- `symboContext.addSelection` - Add current selection
- `symboContext.addActiveFile` - Add active file
- `symboContext.addFilesByGlob` - Add files by pattern
- `symboContext.addGitDiff` - Add git changes
- `symboContext.addText` - Add arbitrary text
- `symboContext.addToolOutput` - Run tool and add output
- `symboContext.clearContext` - Clear all context blocks
- `symboContext.settings` - Open extension settings

### Slash Commands
- `/add-context <type>` - Add context blocks
- `/add-selection` - Add current selection
- `/add-file [path]` - Add file contents
- `/add-diff [type]` - Add git diffs
- `/add-text <text>` - Add text
- `/add-tool <tool>` - Run CLI tools
- `/clear` - Clear context
- `/list` - Show context blocks
- `/remove <number>` - Remove block
- `/status` - Show extension status
- `/export` - Export context to JSON
- `/import` - Import context from JSON
- `/help` - Show command help

### Configuration
- Provider selection and API configuration
- Token budget and TTL settings
- Custom CLI tool definitions
- Telemetry and privacy controls

### Technical
- TypeScript strict mode implementation
- ESLint and Prettier integration
- Comprehensive error handling
- Webview-based UI with CSS theming
- VS Code API compatibility (engine ^1.85.0)
- Cursor compatibility verified

### Testing
- Unit tests for core functionality
- Integration tests with VS Code test framework
- ContextStore functionality testing
- Mock provider testing

### Documentation
- Complete README with setup instructions
- Configuration examples and troubleshooting
- API documentation for HTTP providers
- Development setup and contribution guide

## [Unreleased]

### Planned Features
- Enhanced drag & drop reordering implementation
- Context block preview modal
- Advanced token estimation with model-specific calculations
- Batch context operations
- Context templates and presets
- Integration with more LLM providers
- Enhanced git integration (blame, history)
- Workspace-wide context search
- Performance optimizations for large context sets
- Mobile/web extension support

### Known Issues
- Drag & drop reordering UI needs implementation
- Preview modal for context blocks not yet implemented  
- Large file handling could be optimized
- Some edge cases in secret redaction patterns

### Feedback Welcome
- Report issues on GitHub
- Suggest new features
- Contribute improvements
- Share usage patterns and workflows

---

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).