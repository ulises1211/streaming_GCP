from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from google.cloud import pubsub_v1
import datetime
import os
import json
import base64
import requests


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=r"C:\Users\USUARIO\Desktop\Twitter API\gcp-pubsub\winter-gear-322318-5a91e8be803b.json"

def publish(client, pubsub_topic, data_lines):
    """Publish to the given pubsub topic."""
    messages = []
    for line in data_lines:
        messages.append({'data': line})
    body = {'messages': messages}
    #str_body = json.dumps(body)
    str_body = json.dumps([line])
    
    data = base64.urlsafe_b64encode(bytearray(str_body, 'utf8'))    
    pubsub_message = base64.urlsafe_b64decode(data).decode('utf-8')
    client.publish(topic=pubsub_topic, data=data)
    #client.publish(topic=pubsub_topic, data=str_body)
    #print(pubsub_message)

class TweetStreamListener(StreamListener):
    """
    A listener handles tweets that are received from the stream.
    This listener dumps the tweets into a PubSub topic
    """
    client = pubsub_v1.PublisherClient()
    pubsub_topic = client.topic_path("winter-gear-322318", "streamingdata")
    count = 0
    tweets = []
    batch_size = 1
    # total_tweets = 10000
    total_tweets = 10000

    def write_to_pubsub(self, tweets):
        publish(self.client, self.pubsub_topic, tweets)

    def on_status(self, status):


         # Converting the time to isoformat for serialisation
        created_at = status.created_at.isoformat()
        id_str = status.id_str
        text = status.text
        source = status.source
        user_name = status.user.name
        user_screen_name = status.user.screen_name
        loc = status.user.location
        bio = status.user.description
        # cantidad=data1.public_metrics.retweet_count


     
        # api-endpoint
        URL = "https://api.twitter.com/2/tweets?ids="+id_str+"&tweet.fields=public_metrics&expansions=attachments.media_keys&media.fields=public_metrics"

        # defining a params dict for the parameters to be sent to the API
        #PARAMS = {'ids': '1417669885665054722', 'tweet.fields': 'public_metrics', 'expansions': 'attachments.media_keys', 'media.fields': 'public_metrics'}

        token_twitter= 'AAAAAAAAAAAAAAAAAAAAADV4RwEAAAAAF%2F7YRjthCBCJnZGVSwq%2BswwP3Ek%3DNnl8ririOXGLD2obJziNG4Phg56O3LZAeWyXPVcpCiPs2xPRGN'
        headersAuth = {
        'Authorization': 'Bearer ' + str(token_twitter)
        }

        # sending get request and saving the response as response object
        r = requests.get(url = URL, headers = headersAuth)

        # extracting data in json format
        data1 = r.json()

        #print(data1)
        
        if 'data' in data1:            

            for i in data1['data']:
                
                #print(i['public_metrics']['retweet_count'])

                tw = dict(text=text, bio=bio, created_at=created_at, tweet_id=id_str,
                    location=loc, user_name=user_name,
                    user_screen_name=user_screen_name,
                    source=source,cantidad_retweet=i['public_metrics']['retweet_count'],
                    cantidad_like=i['public_metrics']['like_count'],
                    cantidad_repuesta=i['public_metrics']['reply_count'])
                

            self.tweets.append(tw)
            if len(self.tweets) >= self.batch_size:
                self.write_to_pubsub(self.tweets)
                print(self.tweets)
                self.tweets = []

            self.count += 1
            if self.count >= self.total_tweets:
                return False
            if (self.count % 5) == 0:
                print("count is: {} at {}".format(self.count, datetime.datetime.now()))
            return True




    def on_error(self, status_code):
        print(status_code)


if __name__ == '__main__':
    print('data stream')

    auth = OAuthHandler("TbZmAJCZLfZeEzSZBcuRvQAMD", "mE75C2L65vcSOU65N0ARPzRo7fBEMLvmTNVLlYryUMG7Qbvh1R")
    auth.set_access_token("1377854622082170882-mgDuF8uSIhazSNdBOBrrhXWUaEJWPf", "kUTQ2MNwX3JPlAEmWwO8GSS1F4RtQlKya5EQ6Bf228BoD")

    stream_listener = TweetStreamListener()
    stream = Stream(auth, stream_listener)

    stream.filter(
        track=['soccer','#futbol','futbol','libertadores','cancha','deportes','descentralizado','boca junior','copa','copa libertadores'], languages=["es"]
    )


