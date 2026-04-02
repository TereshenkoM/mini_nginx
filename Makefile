.PHONY: run run-upstreams

run:
	uv run python3 main.py

run-upstreams:
	uv run uvicorn tests.test_server:app --host 127.0.0.1 --port 9001 --log-level info --access-log &
	uv run uvicorn tests.test_server:app --host 127.0.0.1 --port 9002 --log-level info --access-log &
	wait
