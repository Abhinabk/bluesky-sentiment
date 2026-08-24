# bluesky-sentiment

A real-time streaming pipeline that watches the Bluesky firehose, filters for posts
that mention tracked terms, scores their sentiment, and lands the results in ClickHouse
for live dashboarding.

---

## Pipeline

```
Bluesky Jetstream (WebSocket)
        │
        ▼
[ Python ingestion ]              feed.py  → producer.py
        │                         async firehose consumer, flatten events,
        │                         Avro-serialize against Schema Registry
        │   topic: posts.raw      (Avro)
        ▼
[ Java Kafka Streams ]            Main.java + BrandFilterProcessor.java
        │                         GlobalKTable (brands.config) as a state store,
        │                         Processor API + word-boundary regex per term,
        │                         matched posts re-emitted as JSON
        │   topic: posts.enriched (JSON)
        ▼
[ Python sentiment ]             SentimentConsumer.py
        │                         DistilBERT (SST-2) via HuggingFace pipeline,
        │                         adds {sentiment, score}
        │   topic: posts.scored   (JSON)
        ▼
[ ClickHouse ]                   Kafka engine table → materialized view → MergeTree
        │                         mentions_queue → mentions_mv → mentions
        ▼
[ Grafana ]                      ClickHouse datasource, time-series panels
```

Kafka is the language boundary. Python does the WebSocket ingestion and the
model inference; Java does the stateful stream processing.

---

## Design decisions 

**Avro + Schema Registry as the producer↔consumer contract.** The Python producer registers
`Schemas/post.avsc` and serializes against it; the Java Streams app deserializes the same
schema by ID via `GenericAvroSerde`. The value here isn't byte savings — it's the enforced
contract across two languages. Change the schema on one side and the other side finds out
at deserialize time instead of silently reading garbage.

**Runtime-configurable brands via a GlobalKTable.** Tracked terms live in `brands.config`,
a **log-compacted, single-partition** topic seeded by `seed_brands.py`. The Streams app loads
it as a `GlobalKTable` (full replication, guaranteed bootstrap before processing) and the
`BrandFilterProcessor` reads that global store on every record. GlobalKTable over KTable
because the term set is tiny — full replication cost is irrelevant, and it sidesteps
co-partitioning entirely. Single partition + compaction because the topic is a keyed
key/value config log, not a stream.

**Processor API, not the DSL, for the filter.** The DSL operators only see `(key, value)`;
they have no handle on the processor context, so they can't reach a named state store to
loop over the brand list. The Processor API gives that context, which is why
the filter is written as a `Processor` rather than a `filter()` call.

**Word-boundary matching, not substring.** Filtering uses `\bterm\b` (case-insensitive),
not `String.contains()`. `contains()` matches substrings — `"sad"` fires inside
`"disadvantaged"` — which quietly pollutes the data. Regex word boundaries fix the obvious case; genuine natural-language ambiguity (`"apple"` in `"apple pie"`) is out of scope for v1.

**ClickHouse ingests straight off Kafka.** `mentions_queue` is a Kafka-engine table — a
transient read buffer that consumes `posts.scored` and holds nothing. `mentions_mv` is a
materialized view that fires on each block and inserts into `mentions`, the MergeTree table
that actually stores rows (`ORDER BY (brand, ts)`). Microsecond `time_us` is converted to
`DateTime64(6)` in the view. **Always query `mentions`, never the queue table** — reading the
Kafka engine table directly consumes messages.

**Idempotent producers.** All producers run with `enable.idempotence=true` / `acks=all` to
avoid duplicate writes on retry. This is idempotent *production*, not end-to-end
exactly-once across the Streams app.

---

## Stack

| Layer          | Tech |
|----------------|------|
| Ingestion      | Python 3.14 (uv), `websockets`, `confluent-kafka[schemaregistry]`, `fastavro` |
| Streaming      | Apache Kafka (KRaft, single broker), Confluent Schema Registry, Kafka Streams 4.2.0 |
| Serialization  | Avro (`GenericAvroSerde`) in, JSON out for downstream simplicity |
| Sentiment      | HuggingFace `transformers` + `torch`, `distilbert-base-uncased-finetuned-sst-2-english` |
| Storage        | ClickHouse (Kafka engine + materialized view + MergeTree), `clickhouse-connect` |
| Dashboard      | Grafana + official ClickHouse datasource plugin |
| Local infra    | Docker Compose (Kafka, Schema Registry, ClickHouse, Grafana, Kafka-UI) |

---

## Repo layout

```
.
├── docker-compose.yml          full local stack (Kafka KRaft, SR, ClickHouse, Grafana, Kafka-UI)
├── config.py                   shared broker / topic / schema-registry config
├── makefile                    task runner (up, produce, process, sentiment, clickhouse)
├── runmake.sh                  tmux orchestration — runs the 3 app processes side by side
├── Schemas/
│   └── post.avsc               Avro schema for posts.raw (the cross-language contract)
├── Ingestion/
│   ├── feed.py                 async Jetstream WebSocket consumer
│   ├── producer.py             flatten events + Avro-produce to posts.raw
│   └── seed_brands.py          one-shot seeding of the compacted brands.config topic
├── KafkaWrappers/
│   ├── AdminWrapper.py         topic/partition admin helper
│   └── ProducerWrapper.py      thin producer with pluggable (de)serializers
├── Processing/                 Java Kafka Streams app (Maven, Java 21)
│   └── src/main/java/org/abhinab/bluesky/
│       ├── Main.java           topology: posts.raw → filter → posts.enriched
│       └── BrandFilterProcessor.java   Processor API + GlobalKTable lookup + regex
├── Sentiment/
│   └── SentimentConsumer.py    posts.enriched → DistilBERT → posts.scored
└── Clickhouse/
    ├── connect.py              applies the SQL files in order
    └── sql/                    create_database / create_table / create_kafka_table / mv_insert
```

---

## Running it locally

Requires Docker, Java 21 (Temurin), Maven, and [uv](https://docs.astral.sh/uv/). The
makefile assumes `zsh` + `tmux` for the combined `all` target.
Pre-req (tested in ubuntu ec2 instance)
make setup.sh and past the following it that and run bash setup.sh
```sh
#stops on first error
set -e

#uv
curl -LsSf https://astral.sh/uv/install.sh | sh
#make 
sudo apt install make  
# Java 21 + Maven
sudo apt update && sudo apt install -y openjdk-21-jdk maven
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64   # session-only; add to ~/.bashrc to persist

# tmux (+ zsh only if you'll use `make all`)
sudo apt install -y tmux zsh

# ---- Docker ----
# without or true set e stops the progam if  previosdocker not present
sudo apt purge -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin docker-ce-rootless-extras || true 
sudo rm -rf /var/lib/docker /var/lib/containerd
sudo rm -f /etc/apt/sources.list.d/docker.sources /etc/apt/keyrings/docker.asc

# GPG key
sudo apt update
sudo apt install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# repo — heredoc so the file is written AND the $(...) get expanded
sudo tee /etc/apt/sources.list.d/docker.sources > /dev/null <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "$VERSION_CODENAME")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

sudo systemctl enable --now docker
sudo usermod -aG docker $USER
source $HOME/.local/bin/env
```
>  may have to exit and log back in again. Ec2 needs a restart to add docker to groups
> so that docker commands works without sudo

```bash
#0 downloads all python libaries
uv sync

# 1. bring up the stack (Kafka, Schema Registry, ClickHouse, Grafana, Kafka-UI)
make up

# 2. seed the tracked terms into the compacted brands.config topic (run once)
make initialize_brands

# 3. create the ClickHouse objects (database, MergeTree, Kafka engine, materialized view)
make clickhouse

# 4a. run the three app processes together in a tmux layout …
make all

# 4b. … or run them in separate terminals
make produce      # Python ingestion  → posts.raw
make process      # Java Streams       → posts.enriched
make sentiment    # Python sentiment   → posts.scored
```

Then:

- **Kafka-UI** — http://localhost:8090 — watch topics and messages flow.
- **ClickHouse** — http://localhost:8123 (default/admin)
-  **Grafana** — http://localhost:3000 (`admin` / `admin`) — the ClickHouse datasource plugin is auto-installed; 
  
> the Java app and the Python sentiment consumer run on the host, so they
> connect to `localhost:9092`. 
> ClickHouse runs inside the compose network, 
> so its Kafka-engine table uses the internal listener `kafka:29092`. 

---

## Status & roadmap

The pipeline is working end-to-end from firehose to ClickHouse: real posts flow in, get
filtered against the live brand set, scored, and land as rows inclichouse table
`bluesky.mentions`.
 The remaining work:

- [ ] **Grafana time-series panels**
- [ ] **Runtime brand control plane** — a small API to push brand add/remove records into
      `brands.config` live, so the GlobalKTable picks them up without a restart. The
      streaming side already supports this; the control endpoint is what's missing.
- [ ] Additional panels: sentiment breakdown, share-of-voice, top-negative feed.
---

## Engineering notes 

- **`Unknown magic byte!`** — I started `posts.raw` as plain JSON, then switched the producer
  to Avro *without deleting the topic*. The consumer choked on the old JSON records because
  they lacked the Schema Registry magic-byte header. Only fixable from the producer side by
  wiping the topic and re-producing. 

- **`time_us` overflow** — the Jetstream microsecond timestamp overflows a 32-bit int. The
  Avro field has to be a `long`, and ClickHouse ingests it as `UInt64` before converting to
  `DateTime64(6)` via `fromUnixTimestamp64Micro`.

- **`filter()` throws away the matched brand** — a boolean predicate can't also *return* which
  term matched. The filter had to move into JSON construction (emit the record tagged with the
  brand, or emit null and drop it downstream) so the surviving record carries the brand name.
  This is part of why the filter is a Processor, not a stream `filter()`.


- **`.contains()` substring bug** —. Silent data pollution that looks fine until you read the matches. Witch ocntains sad matches in disadvantage no word boundary

- **ClickHouse `IF NOT EXISTS` silently skips edits** — with `IF NOT EXISTS`, changing a table
  or view definition does nothing on an existing object; you have to `DROP` first. This was the
  main debugging loop while getting the materialized view right.

- **Applying SQL in order without a `USE` statement** — `clickhouse-connect` doesn't carry a
  session database across `command()` calls, so every object is fully qualified (`bluesky.x`) where x is the table name and the sql files are applied in a fixed order via an ordered dict.