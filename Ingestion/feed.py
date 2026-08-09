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
                pprint(data)
        except websockets.ConnectionClosed as e:
            logger.warning("Connection closed: %s",e)
        except websockets.exceptions as e:
            logger.warning("Connection Error %s",e)
        except Exception as e:
            logger.warning("Error %s",e)

def main():
    asyncio.run(get_feed())

if __name__ == "__main__":
    main()





