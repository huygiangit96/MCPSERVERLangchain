import asyncio
import traceback
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from dotenv import load_dotenv

load_dotenv()

from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.prebuilt import create_react_agent
from langchain_ollama import ChatOllama
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

# ---- Config Ollama ----
ollama_url = "http://143.110.217.48:11434/"
model = ChatOllama(model="gpt-1", base_url=ollama_url, reasoning=True)

app = FastAPI()

# Cho phép mọi domain kết nối (dev)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global vars
_agent = None
_config = None
_saver = None
_checkpointer = None

# Serve file index.html tại "/"
@app.get("/")
async def serve_index():
    return FileResponse("static/index.html")

# WebSocket chat
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    global _agent, _config
    await websocket.accept()

    if _agent is None:
        await websocket.send_text("__err__Agent chưa sẵn sàng. Vui lòng thử lại.")
        await websocket.close()
        return

    try:
        while True:
            user_input = await websocket.receive_text()
            await websocket.send_text("__received__")
            try:
                async for token, metadata in _agent.astream(
                    {"messages": [{"role": "user", "content": user_input}]},
                    stream_mode="messages",
                    config=_config
                ):
                    text = getattr(token, "content", str(token))
                    await websocket.send_text(text)
                await websocket.send_text("__done__")
            except Exception as e:
                err_msg = f"Lỗi khi chạy agent: {e}"
                print(err_msg)
                traceback.print_exc()
                await websocket.send_text(f"__err__{err_msg}")
    except WebSocketDisconnect:
        print("Client disconnected")

# Khởi động agent khi server start
@app.on_event("startup")
async def startup_event():
    global _agent, _config, _saver, _checkpointer
    try:
        client = MultiServerMCPClient({
            "analyze_data": {
                "url": "http://localhost:8080/mcp",
                "transport": "streamable_http",
            }
        })
        _saver = AsyncSqliteSaver.from_conn_string("checkpoints.db")
        _checkpointer = await _saver.__aenter__()
        tools = await client.get_tools()
        _agent = create_react_agent(model, tools, checkpointer=_checkpointer)
        _config = {"configurable": {"thread_id": "2", "recursion_limit": 100}}
        print("✅ Agent sẵn sàng.")
    except Exception:
        print("❌ Lỗi khi khởi tạo agent:")
        traceback.print_exc()

# Đóng checkpointer khi server tắt
@app.on_event("shutdown")
async def shutdown_event():
    global _saver
    if _saver is not None:
        try:
            await _saver.__aexit__(None, None, None)
            print("Checkpointer closed.")
        except Exception as e:
            print("Lỗi khi đóng checkpointer:", e)
