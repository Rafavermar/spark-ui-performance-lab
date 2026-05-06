# Troubleshooting

## Docker Not Running

Start Docker Desktop, then run:

```bash
./scripts/up.sh
```

## Ports Already In Use

The lab uses `8080`, `8081`, `8082`, `4040`, `18080` and optionally `9092`. Stop the conflicting process or change the port mapping in `docker-compose.yml`.

## Spark UI Worker Or Log Link Does Not Open

Spark sometimes renders links using Docker-internal hostnames such as:

```text
spark-worker-1:8081
spark-worker-2:8082
```

Those names work inside the Docker network, but not always from your host browser.

Use the host port mappings instead:

- Worker 1 UI: <http://localhost:8081>
- Worker 2 UI: <http://localhost:8082>

If executor `stdout` or `stderr` links from <http://localhost:4040> do not open, navigate through the Worker UI directly or use Docker logs for low-level debugging.

## Spark App Finished Before Opening Live UI

Open Spark History Server at <http://localhost:18080>. Event logs are persisted in the shared `spark-events` volume.

## Unsupported Mode Or Typo

Use only:

```bash
baseline
optimized
```

Case `17_real_time_mode` also accepts:

```bash
advanced
```

If you type `optmized` or another typo, `scripts/run-case.sh` stops before `spark-submit` and prints a clear mode error.

## Git Bash Converts Container Paths On Windows

Symptom:

```text
Local jar /workspace/C:/Program Files/Git/workspace/target/... does not exist
Error: Failed to load class lab.Main.
```

Cause:

Git Bash/MSYS can automatically convert Linux-style paths such as `/workspace/...` into Windows paths. Those paths are wrong inside Docker containers.

Fix:

The project scripts set `MSYS_NO_PATHCONV=1` and `MSYS2_ARG_CONV_EXCL=*` before calling Docker. If you run `docker compose exec ... spark-submit` manually from Git Bash, set the same variables first:

```bash
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"
```

## Event Logs Not Appearing

Check that `spark-history-server` is running:

```bash
docker compose ps
```

Then rerun a case and wait a few seconds after it exits. The History Server scans `file:/opt/spark-events`.

## Redpanda Broker Not Running

Streaming cases require:

```bash
./scripts/up-streaming.sh
./scripts/create-topics.sh
```

Batch cases do not require Redpanda.

## Topics Missing

Run:

```bash
./scripts/create-topics.sh
```

Then produce deterministic input:

```bash
./scripts/produce-streaming-data.sh
```

## Streaming Checkpoint Cleanup

Use:

```bash
./scripts/reset-streaming.sh
```

This deletes and recreates streaming topics and removes streaming checkpoints.

## Reset Generated Data

```bash
./scripts/clean.sh
./scripts/generate-data.sh
```

## Rerun A Case From Scratch

For batch:

```bash
./scripts/clean.sh
./scripts/generate-data.sh
./scripts/run-case.sh 03_shuffle_explosion baseline
```

For streaming:

```bash
./scripts/reset-streaming.sh
./scripts/produce-streaming-data.sh
./scripts/run-case.sh 15_structured_streaming_backlog baseline
```
