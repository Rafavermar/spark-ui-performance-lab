.PHONY: up up-streaming down build generate-data run run-all create-topics produce-streaming-data reset-streaming export-metrics clean

up:
	bash scripts/up.sh

up-streaming:
	bash scripts/up-streaming.sh

down:
	bash scripts/down.sh

build:
	bash scripts/build.sh

generate-data:
	bash scripts/generate-data.sh

run:
	bash scripts/run-case.sh $(CASE) $(MODE)

run-all:
	bash scripts/run-all.sh

create-topics:
	bash scripts/create-topics.sh

produce-streaming-data:
	bash scripts/produce-streaming-data.sh

reset-streaming:
	bash scripts/reset-streaming.sh

export-metrics:
	bash scripts/export-metrics.sh $(CASE) $(MODE)

clean:
	bash scripts/clean.sh
