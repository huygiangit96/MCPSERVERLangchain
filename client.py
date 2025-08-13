from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.prebuilt import create_react_agent
from langchain_ollama import ChatOllama
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver


from dotenv import load_dotenv
load_dotenv()

ollama_url = "http://143.110.217.48:11434/"

model=ChatOllama(model="gpt-oss-beta",base_url=ollama_url ,reasoning=False)

import asyncio

async def main():
    client=MultiServerMCPClient(
        {
            # "math":{
            #     "command":"python",
            #     "args":["mathserver.py"], ## Ensure correct absolute path
            #     "transport":"stdio",
            
            # },
            # "weather": {
            #     "url": "http://localhost:8000/mcp",  # Ensure server is running here
            #     "transport": "streamable_http",
            # },
            "analyze_data":{
                "url": "http://localhost:8080/mcp",  # Ensure server is running here
                "transport": "streamable_http",
            }



        }
    )
    saver = AsyncSqliteSaver.from_conn_string("checkpoints.db")
    checkpointer = await saver.__aenter__()
    tools=await client.get_tools()
    agent=create_react_agent(
        model,tools,checkpointer=checkpointer
    )
    config = {"configurable": {"thread_id": "2"}}

    async def run_agent_and_stream(_input):
        async for token, metadata in agent.astream({"messages": [{"role": "user", "content": _input}]}, stream_mode="messages", config=config):
            print(token.content, end="")



    while True:
        _input = input("\nCâu hỏi:")

        await run_agent_and_stream(_input)



asyncio.run(main())
