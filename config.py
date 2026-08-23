from collections import  namedtuple

BOOTSTRAP_SERVERS=["localhost:9092"]
GROUP_ID = "scored"
Topics = namedtuple("Topics",["raw","enriched","scored","brands"])
TOPICS=Topics(raw="posts.raw",enriched="posts.enriched",scored="posts.scored",brands="brands.config")

SCHEMA_REGISTRY_URL=["http://localhost:8081"]