.PHONY: produce process sentiment up down all

up:
	docker compose up -d 

down:
	docker compose down

produce:
	@echo "Running Ingestion"
	uv run -m Ingestion.producer  

process:
	@echo "Running java processor"
	cd Processing && mvn exec:java 

sentiment:
	@echo "Running Sentiment"
	uv run -m Sentiment.SentimentConsumer

all:
	chmod +x runmake.sh && docker compose up -d && ./runmake.sh
