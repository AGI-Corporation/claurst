// agi-web: AGI – Claude-like web interface server.
//
// Serves a browser-based chat UI (HTML/CSS/JS) and exposes two API endpoints
// that integrate deeply with the existing cc-* crates:
//
//   GET  /              → Single-page AGI interface (HTML)
//   GET  /api/models    → JSON list of available models
//   POST /api/chat      → SSE-streamed chat completion (via cc-api)

use axum::{
    Router,
    extract::{Json, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response, Sse},
    response::sse::{Event, KeepAlive},
    routing::{get, post},
};
use cc_api::{
    AnthropicClient, ApiMessage, CreateMessageRequest, StreamEvent,
    client::ClientConfig,
    streaming::NullStreamHandler,
};
use cc_core::{
    config::{Config, Settings},
    constants::{DEFAULT_MODEL, DEFAULT_MAX_TOKENS},
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio_stream::wrappers::ReceiverStream;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

// ---------------------------------------------------------------------------
// Shared application state
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AppState {
    client: Arc<AnthropicClient>,
    #[allow(dead_code)]
    config: Arc<Config>,
}

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct ChatRequest {
    messages: Vec<ChatMessage>,
    model: Option<String>,
    max_tokens: Option<u32>,
    system: Option<String>,
}

#[derive(Debug, Serialize)]
struct ModelInfo {
    id: String,
    name: String,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("agi_web=info".parse()?)
                .add_directive("tower_http=warn".parse()?),
        )
        .init();

    let config = Arc::new(
        Settings::load()
            .await
            .map(|s| s.config)
            .unwrap_or_default(),
    );

    let api_key = config
        .resolve_api_key()
        .or_else(|| std::env::var("ANTHROPIC_API_KEY").ok())
        .unwrap_or_default();

    let client = Arc::new(AnthropicClient::new(ClientConfig {
        api_key: api_key.clone(),
        ..Default::default()
    })?);

    let state = AppState { client, config };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/", get(serve_index))
        .route("/api/models", get(list_models))
        .route("/api/chat", post(chat_stream))
        .layer(cors)
        .with_state(state);

    let port: u16 = std::env::var("AGI_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3000);

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    info!("AGI server listening on http://{}", addr);
    println!("🤖 AGI is running → http://localhost:{}", port);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Serve the embedded single-page AGI interface.
async fn serve_index() -> impl IntoResponse {
    let html = include_str!("../static/index.html");
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));
    (StatusCode::OK, headers, html)
}

/// Return a JSON list of supported Claude models.
async fn list_models(State(state): State<AppState>) -> impl IntoResponse {
    // Try to fetch live models from the API; fall back to a hardcoded list.
    let models: Vec<ModelInfo> = state
        .client
        .fetch_available_models()
        .await
        .map(|ms| {
            ms.into_iter()
                .map(|m| ModelInfo {
                    name: m.display_name.clone().unwrap_or_else(|| m.id.clone()),
                    id: m.id,
                })
                .collect()
        })
        .unwrap_or_else(|_| {
            vec![
                ModelInfo { id: "claude-opus-4-5".into(), name: "Claude Opus 4.5".into() },
                ModelInfo { id: "claude-sonnet-4-5".into(), name: "Claude Sonnet 4.5".into() },
                ModelInfo { id: "claude-haiku-4-5".into(), name: "Claude Haiku 4.5".into() },
                ModelInfo { id: DEFAULT_MODEL.into(), name: "Claude (default)".into() },
            ]
        });

    Json(json!({ "models": models }))
}

/// Stream a chat completion as Server-Sent Events.
async fn chat_stream(
    State(state): State<AppState>,
    Json(req): Json<ChatRequest>,
) -> Response {
    let model = req.model.unwrap_or_else(|| DEFAULT_MODEL.to_string());
    let max_tokens = req.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS);

    let api_messages: Vec<ApiMessage> = req
        .messages
        .into_iter()
        .map(|m| ApiMessage {
            role: m.role,
            content: Value::String(m.content),
        })
        .collect();

    let system = req.system.map(cc_api::SystemPrompt::Text);

    let api_req = CreateMessageRequest {
        model,
        max_tokens,
        messages: api_messages,
        system,
        tools: None,
        temperature: None,
        top_p: None,
        top_k: None,
        stop_sequences: None,
        stream: true,
        thinking: None,
    };

    let handler = Arc::new(NullStreamHandler);

    let client = state.client.clone();

    // Stream conversion: mpsc::Receiver<StreamEvent> → SSE events
    let sse_stream = async_stream::stream! {
        let rx = match client.create_message_stream(api_req, handler).await {
            Ok(rx) => rx,
            Err(e) => {
                let payload = json!({"type":"error","error": e.to_string()});
                yield Ok::<Event, Infallible>(
                    Event::default().event("error").data(payload.to_string())
                );
                return;
            }
        };

        let mut stream = ReceiverStream::new(rx);
        while let Some(event) = stream.next().await {
            let sse_event = match &event {
                StreamEvent::MessageStart { id, model, usage } => {
                    let payload = json!({
                        "type": "message_start",
                        "id": id,
                        "model": model,
                        "usage": {
                            "input_tokens": usage.input_tokens,
                            "output_tokens": usage.output_tokens
                        }
                    });
                    Event::default().event("message_start").data(payload.to_string())
                }
                StreamEvent::ContentBlockStart { index, content_block } => {
                    let payload = json!({
                        "type": "content_block_start",
                        "index": index,
                        "content_block": content_block
                    });
                    Event::default().event("content_block_start").data(payload.to_string())
                }
                StreamEvent::ContentBlockDelta { index, delta } => {
                    use cc_api::streaming::ContentDelta;
                    let (delta_type, delta_text) = match delta {
                        ContentDelta::TextDelta { text } => ("text_delta", text.clone()),
                        ContentDelta::ThinkingDelta { thinking } => ("thinking_delta", thinking.clone()),
                        ContentDelta::InputJsonDelta { partial_json } => ("input_json_delta", partial_json.clone()),
                        ContentDelta::SignatureDelta { signature } => ("signature_delta", signature.clone()),
                    };
                    let payload = json!({
                        "type": "content_block_delta",
                        "index": index,
                        "delta": { "type": delta_type, "text": delta_text }
                    });
                    Event::default().event("content_block_delta").data(payload.to_string())
                }
                StreamEvent::ContentBlockStop { index } => {
                    let payload = json!({ "type": "content_block_stop", "index": index });
                    Event::default().event("content_block_stop").data(payload.to_string())
                }
                StreamEvent::MessageDelta { stop_reason, usage } => {
                    let payload = json!({
                        "type": "message_delta",
                        "stop_reason": stop_reason,
                        "usage": usage.as_ref().map(|u| json!({
                            "input_tokens": u.input_tokens,
                            "output_tokens": u.output_tokens
                        }))
                    });
                    Event::default().event("message_delta").data(payload.to_string())
                }
                StreamEvent::MessageStop => {
                    let payload = json!({ "type": "message_stop" });
                    yield Ok::<Event, Infallible>(
                        Event::default().event("message_stop").data(payload.to_string())
                    );
                    return;
                }
                StreamEvent::Error { error_type, message } => {
                    let payload = json!({
                        "type": "error",
                        "error_type": error_type,
                        "error": message
                    });
                    yield Ok::<Event, Infallible>(
                        Event::default().event("error").data(payload.to_string())
                    );
                    return;
                }
                StreamEvent::Ping => {
                    Event::default().event("ping").data("{\"type\":\"ping\"}")
                }
            };
            yield Ok::<Event, Infallible>(sse_event);
        }
    };

    Sse::new(sse_stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}
