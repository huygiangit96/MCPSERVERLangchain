1. Install Ollama
	https://github.com/ollama/ollama
2. Public
Environment="OLLAMA_HOST=0.0.0.0:11434"
to /etc/systemd/system/ollama.service

3. Clone https://github.com/huygiangit96/MCPSERVERLangchain.git
4. Install requirements
5. Run mcp
6. uvicorn app:app --host 0.0.0.0 --port 8000
