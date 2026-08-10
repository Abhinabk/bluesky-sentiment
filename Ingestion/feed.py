""" Connect to bluesky firehose using jetstream and output the post feeds """

import  asyncio
import  websockets
import  json
import  logging
from pprint import  pprint

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

url = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

async def get_feed():
    async with websockets.connect(url) as websocket:
        try:
            async for events in websocket:
                data = json.loads(events)
                # filter for kind == commit (account event type don't have operation)
                if(data.get("kind")=="commit"
                    and data.get("commit",{}).get("operation") == "create"
                    and data.get("commit",{}).get("record") is not None):
                    yield  data
        except websockets.ConnectionClosed as e:
            logger.warning("Connection closed: %s",e)
        except Exception as e:
            logger.warning("Error %s",e)

async def main():
    async for i in get_feed():
        pprint(i)

if __name__ == "__main__":
    asyncio.run(main())





