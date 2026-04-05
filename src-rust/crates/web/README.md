# AGI Web Interface

A Claude-like browser UI powered by the `claurst` Rust backend.

## Running

```bash
# Set your Anthropic API key
export ANTHROPIC_API_KEY=sk-ant-...

# Run the server (default port 3000)
cargo run -p agi-web

# Or on a custom port
AGI_PORT=8080 cargo run -p agi-web
```

Open your browser at **http://localhost:3000**.

## Features

- 🤖 **Claude-like interface** — clean chat UI with sidebar, conversation history, and streaming responses
- ⚡ **Streaming** — real-time token-by-token streaming via Server-Sent Events (SSE)
- 💾 **Local persistence** — conversations saved in `localStorage` (no server-side DB needed)
- 🎨 **Dark / light theme** — toggle with the 🌓 button
- 📋 **Code blocks** — syntax-highlighted, one-click copy
- 📱 **Responsive** — works on mobile with collapsible sidebar
- 🔍 **Model picker** — fetches live model list from the Anthropic API
- 🧮 **Token usage** — shows input/output token counts per turn

## Architecture

```
Browser ──SSE──► GET  /api/chat   ──► cc-api::AnthropicClient (streaming)
         JSON►  POST /api/chat
                GET  /api/models  ──► cc-api::AnthropicClient (list models)
                GET  /            ──► static/index.html (embedded)
```

The server is a thin Axum HTTP layer that wires the browser directly to the existing
`cc-api` crate (Anthropic streaming client) used by the terminal TUI.
