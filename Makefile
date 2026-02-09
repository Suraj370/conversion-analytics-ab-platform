.PHONY: install run-collector simulate transform analyze export dashboard test lint clean demo

# Python environment
PYTHON := python
PIP := pip

install:
	$(PIP) install -r requirements.txt

# Event collector
run-collector:
	$(PYTHON) -m uvicorn src.collector.app:app --reload --port 8000

# User simulator
simulate:
	$(PYTHON) -m src.simulator.generate --experiment

# dbt transformations
transform:
	cd dbt && dbt run

# Experiment analysis
analyze:
	$(PYTHON) -m src.analysis.run

# Export dashboard data
export:
	$(PYTHON) -m src.analysis.export

# Dashboard (run export first to generate data.json)
dashboard: export
	@echo "Dashboard available at http://localhost:8080"
	$(PYTHON) -m http.server 8080 --directory src/dashboard

# Full demo: simulate -> export -> analyze
demo: simulate export analyze

# Testing
test:
	$(PYTHON) -m pytest tests/ -v

lint:
	$(PYTHON) -m ruff check src/ tests/

# Cleanup
clean:
	rm -f data/*.duckdb
	rm -f src/dashboard/data.json
	rm -rf dbt/target dbt/logs
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
