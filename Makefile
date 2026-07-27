COMPOSE = docker compose -f infrastructure/docker-compose.yml

.PHONY: start stop restart status logs health test lint clean

start:
	$(COMPOSE) up --build -d

stop:
	$(COMPOSE) down

restart:
	$(COMPOSE) down && $(COMPOSE) up --build -d

status:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f

health:
	@echo "=== 1. API Health ==="
	@curl -sf http://localhost:8000/health || echo "API: DOWN"
	@echo "\n=== 2. OpenSearch Health ==="
	@curl -sf http://localhost:9200/_cluster/health || echo "OpenSearch: DOWN"
	@echo "\n=== 3. OpenSearch Dashboards ==="
	@curl -sf http://localhost:5601 || echo "Dashboards: DOWN"
	@echo "\n=== 4. Redis Health ==="
	@docker exec $$(docker ps -qf name=redis) redis-cli ping 2>/dev/null || echo "Redis: DOWN"
	@echo "\n=== 5. Ollama Health ==="
	@curl -sf http://localhost:11434/api/tags || echo "Ollama: DOWN"
	@echo "\n=== 6. Airflow Webserver Health ==="
	@curl -sf http://localhost:8080/health || echo "Airflow: DOWN"

test:
	uv run pytest tests/ -v

lint:
	uv run ruff check src/ tests/

clean:
	$(COMPOSE) down -v
	find . -type d -name __pycache__ -exec rm -rf {} +
