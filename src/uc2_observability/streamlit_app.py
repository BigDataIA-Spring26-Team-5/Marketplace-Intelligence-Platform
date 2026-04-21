"""
UC2 Observability Layer — Streamlit Shell

Two-column layout:
  LEFT  — Grafana dashboard embedded as an iframe
  RIGHT — RAG chatbot backed by ChromaDB + Claude + MCP tools

Run with:
    streamlit run src/uc2_observability/streamlit_app.py --server.port 8502
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from dotenv import load_dotenv
load_dotenv("/home/bhavyalikhitha_bbl/bhavya-workspace/.env")

import chromadb
import requests
import streamlit as st
from anthropic import Anthropic
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

# ── configuration ──────────────────────────────────────────────────────────────

VM_IP = os.environ.get("VM_IP", "35.239.47.242")
GRAFANA_URL = f"http://{VM_IP}:3000/d/uc1-pipeline"
CHROMA_HOST = "localhost"
CHROMA_PORT = 8000
CHROMA_COLLECTION = "audit_corpus"
EMBED_MODEL = "all-MiniLM-L6-v2"
MCP_BASE_URL = "http://localhost:8001"
TOP_K = 5

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = "claude-sonnet-4-6"

# ── MCP tool definitions sent to Claude ───────────────────────────────────────

MCP_TOOLS: list[dict] = [
    {
        "name": "get_run_metrics",
        "description": "Retrieve all Prometheus metrics (rows_in, rows_out, null_rate, dq_score, llm_cost, etc.) for a specific pipeline run_id.",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string", "description": "Pipeline run identifier"},
                "source": {"type": "string", "description": "Data source: OFF, USDA, openFDA, or ESCI"},
            },
            "required": ["run_id"],
        },
    },
    {
        "name": "get_block_trace",
        "description": "Retrieve the block-level execution trace (rows in/out, null rates, duration per block) from Postgres for a pipeline run.",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "source": {"type": "string"},
            },
            "required": ["run_id"],
        },
    },
    {
        "name": "get_source_stats",
        "description": "Retrieve aggregated Prometheus stats (DQ score, null rate, row counts, dedup rate) for a specific data source.",
        "input_schema": {
            "type": "object",
            "properties": {
                "source": {"type": "string", "description": "Data source: OFF, USDA, openFDA, or ESCI"},
                "run_id": {"type": "string"},
            },
            "required": ["source"],
        },
    },
    {
        "name": "get_anomalies",
        "description": "Retrieve anomaly detection reports from Postgres — shows which runs were flagged and why.",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "source": {"type": "string"},
                "limit":  {"type": "integer", "description": "Max rows to return"},
            },
        },
    },
    {
        "name": "get_cost_report",
        "description": "Retrieve LLM cost metrics (cost_usd, llm_calls, enrichment tier counts) from Prometheus.",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "source": {"type": "string"},
            },
        },
    },
    {
        "name": "get_quarantine",
        "description": "Retrieve rows quarantined during a pipeline run, with their failure reasons.",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "source": {"type": "string"},
                "limit":  {"type": "integer"},
            },
            "required": ["run_id"],
        },
    },
    {
        "name": "get_dedup_stats",
        "description": "Retrieve deduplication cluster statistics from Postgres — shows cluster IDs, canonical rows, and merged members.",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "source": {"type": "string"},
                "limit":  {"type": "integer"},
            },
        },
    },
]


# ── cached singletons ─────────────────────────────────────────────────────────

@st.cache_resource
def _get_embed_model() -> SentenceTransformer:
    return SentenceTransformer(EMBED_MODEL)


@st.cache_resource
def _get_chroma_collection():
    client = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
    return client.get_or_create_collection(
        name=CHROMA_COLLECTION,
        metadata={"hnsw:space": "cosine"},
    )


@st.cache_resource
def _get_anthropic_client() -> Anthropic:
    return Anthropic(api_key=ANTHROPIC_API_KEY)


# ── RAG retrieval ──────────────────────────────────────────────────────────────

def _retrieve_chunks(question: str, k: int = TOP_K) -> list[dict[str, Any]]:
    """Embed question and retrieve top-k chunks from ChromaDB."""
    model = _get_embed_model()
    collection = _get_chroma_collection()

    q_embedding = model.encode([question], show_progress_bar=False)[0].tolist()
    results = collection.query(
        query_embeddings=[q_embedding],
        n_results=k,
        include=["documents", "metadatas", "distances"],
    )

    chunks = []
    for i, (doc, meta, dist) in enumerate(zip(
        results["documents"][0],
        results["metadatas"][0],
        results["distances"][0],
    )):
        chunks.append({
            "chunk_id": results["ids"][0][i],
            "text":     doc,
            "metadata": meta,
            "distance": dist,
        })
    return chunks


# ── MCP tool execution ─────────────────────────────────────────────────────────

def _call_mcp_tool(tool_name: str, tool_input: dict) -> Any:
    """Call the MCP server tool endpoint and return the result data."""
    url = f"{MCP_BASE_URL}/tools/{tool_name}"
    try:
        resp = requests.post(url, json=tool_input, timeout=15)
        resp.raise_for_status()
        return resp.json().get("data", {})
    except requests.RequestException as exc:
        logger.warning("MCP tool %s failed: %s", tool_name, exc)
        return {"error": str(exc)}


# ── Claude agentic loop ────────────────────────────────────────────────────────

def _ask_claude(question: str, chunks: list[dict]) -> tuple[str, list[str]]:
    """
    Send question + retrieved chunks to Claude with MCP tool definitions.
    Handles tool-use responses by calling MCP server and continuing the loop.
    Returns (answer_text, cited_chunk_ids).
    """
    client = _get_anthropic_client()

    # Build RAG context block
    context_lines = []
    for chunk in chunks:
        cid = chunk["chunk_id"]
        context_lines.append(f"[{cid}]\n{chunk['text']}")
    rag_context = "\n\n---\n\n".join(context_lines)

    cited_ids = [c["chunk_id"] for c in chunks]

    system_prompt = (
        "You are an observability assistant for the UC1 Marketplace Intelligence Pipeline. "
        "You have access to retrieved audit log chunks and live tool endpoints. "
        "When answering, cite chunk IDs as [chunk-id] and mention which tool results you used. "
        "Be concise and evidence-based."
    )

    messages: list[dict] = [
        {
            "role": "user",
            "content": (
                f"Retrieved audit log context:\n\n{rag_context}\n\n"
                f"---\n\nUser question: {question}"
            ),
        }
    ]

    # Agentic loop: up to 5 tool-use rounds
    for _ in range(5):
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=2048,
            system=system_prompt,
            tools=MCP_TOOLS,
            messages=messages,
        )

        if response.stop_reason == "end_turn":
            # Extract text from response
            answer = " ".join(
                block.text
                for block in response.content
                if hasattr(block, "text")
            )
            return answer, cited_ids

        if response.stop_reason == "tool_use":
            # Process all tool calls, then continue
            assistant_content = response.content
            messages.append({"role": "assistant", "content": assistant_content})

            tool_results = []
            for block in assistant_content:
                if block.type != "tool_use":
                    continue
                tool_result = _call_mcp_tool(block.name, block.input)
                tool_results.append({
                    "type":        "tool_result",
                    "tool_use_id": block.id,
                    "content":     json.dumps(tool_result, default=str),
                })

            messages.append({"role": "user", "content": tool_results})
            continue

        # Any other stop reason — return what we have
        answer = " ".join(
            block.text
            for block in response.content
            if hasattr(block, "text")
        )
        return answer or "(No response generated.)", cited_ids

    return "Reached maximum tool-use rounds without a final answer.", cited_ids


# ── Streamlit UI ───────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(
        page_title="UC2 Observability — Marketplace Intelligence",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    st.title("UC2 Observability Layer")
    st.caption("Real-time pipeline monitoring + RAG chatbot")

    left_col, right_col = st.columns([3, 2], gap="large")

    # ── LEFT: Grafana iframe ──────────────────────────────────────────────────
    with left_col:
        st.subheader("Live Pipeline Dashboard")
        grafana_params = (
            "?orgId=1"
            "&refresh=15s"
            "&theme=light"
            "&kiosk=tv"
        )
        grafana_src = GRAFANA_URL + grafana_params
        st.components.v1.iframe(
            src=grafana_src,
            height=750,
            scrolling=True,
        )
        st.caption(
            f"Dashboard source: [{grafana_src}]({grafana_src}) — "
            "auto-refreshes every 15 s"
        )

    # ── RIGHT: RAG chatbot ────────────────────────────────────────────────────
    with right_col:
        st.subheader("Ask the Pipeline")

        if not ANTHROPIC_API_KEY:
            st.error(
                "ANTHROPIC_API_KEY environment variable is not set. "
                "The chatbot will not function without it."
            )

        # Chat history in session state
        if "chat_history" not in st.session_state:
            st.session_state["chat_history"] = []

        # Display existing conversation
        for entry in st.session_state["chat_history"]:
            with st.chat_message(entry["role"]):
                st.markdown(entry["content"])
                if entry.get("citations"):
                    with st.expander("Evidence chunks"):
                        for cid in entry["citations"]:
                            st.code(cid, language="text")

        # Question input
        question = st.chat_input(
            placeholder=(
                "e.g. Why was OFF run 2 flagged as an anomaly? "
                "Which block spiked null brand_owner values?"
            )
        )

        if question:
            # Show user message immediately
            st.session_state["chat_history"].append({"role": "user", "content": question})
            with st.chat_message("user"):
                st.markdown(question)

            with st.chat_message("assistant"):
                with st.spinner("Retrieving context and calling tools …"):
                    try:
                        chunks = _retrieve_chunks(question, k=TOP_K)
                        answer, cited_ids = _ask_claude(question, chunks)
                    except Exception as exc:
                        answer = f"Error: {exc}"
                        cited_ids = []

                st.markdown(answer)
                if cited_ids:
                    with st.expander(f"Evidence — {len(cited_ids)} chunk(s) retrieved"):
                        for cid in cited_ids:
                            # Find the matching chunk text
                            chunk_text = next(
                                (c["text"] for c in chunks if c["chunk_id"] == cid),
                                cid,
                            )
                            st.text_area(
                                label=cid,
                                value=chunk_text,
                                height=80,
                                disabled=True,
                                key=f"chunk_{cid}_{len(st.session_state['chat_history'])}",
                            )

            st.session_state["chat_history"].append({
                "role":      "assistant",
                "content":   answer,
                "citations": cited_ids,
            })

        # Sidebar: quick example questions
        with st.sidebar:
            st.header("Example Questions")
            examples = [
                "Why did the March 28 run produce fewer rows than March 21?",
                "Which block spiked null brand_owner values?",
                "How many rows were quarantined last run and why?",
                "What did S4 LLM enrich that rules couldn't handle?",
                "Is today's DQ distribution normal vs the last 5 runs?",
                "Show me runs where duplicate rate was anomalous.",
                "Which source contributed most to the enriched catalog?",
            ]
            for ex in examples:
                if st.button(ex, key=f"ex_{hash(ex)}"):
                    # Inject into chat input by re-running with the question
                    st.session_state["_prefill_question"] = ex
                    st.rerun()

            if "_prefill_question" in st.session_state:
                # This is handled above via `question` from chat_input;
                # for sidebar buttons we surface it as a note
                q = st.session_state.pop("_prefill_question")
                st.info(f"Type this question in the chat box:\n\n_{q}_")

            if st.button("Clear chat history"):
                st.session_state["chat_history"] = []
                st.rerun()

            st.markdown("---")
            st.caption(
                "MCP server: localhost:8001  \n"
                "ChromaDB: localhost:8000  \n"
                "Grafana: localhost:3000  \n"
                "Prometheus: localhost:9090"
            )


if __name__ == "__main__":
    main()
