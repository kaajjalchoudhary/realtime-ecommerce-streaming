PYTHON ?= python
export PYTHONPATH := src

.PHONY: install up down produce consume report demo test lint

install:
	$(PYTHON) -m pip install -r requirements.txt

up:
	docker compose up -d --wait

down:
	docker compose down -v

produce:
	$(PYTHON) -m streaming.producer --events 3000 --simulate-minutes 15

consume:
	$(PYTHON) -m streaming.consumer --idle-timeout 15

report:
	$(PYTHON) -m streaming.report --limit 15

demo: up produce consume report

test:
	pytest -v

lint:
	ruff check src tests
