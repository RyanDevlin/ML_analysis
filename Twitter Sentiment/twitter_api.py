from TwitterSearch import *
import pandas as pd



class Twitter_ETL():

    def __init__(self, payload, keywords, language):
        self.df = pd.DataFrame()
        self.payload = payload
        self.keywords = keywords
        self.language = language
        self.consumer_secret = payload['consumer_secret']
        self.consumer_key = payload['consumer_key'] 
        self.access_token = payload['access_token']
        self.access_token_secret = payload['access_token_secret']


    def Extract_Tweets(self):
        #returns dataframe 

        try:
            tso = TwitterSearchOrder() # create a TwitterSearchOrder object
            tso.set_keywords(self.keywords)
            tso.set_language(self.language)
            tso.set_include_entities(False) # and don't give us all those entity information

            ts = TwitterSearch(
                consumer_key = self.consumer_key,
                consumer_secret = self.consumer_secret,
                access_token = self.access_token,
                access_token_secret = self.access_token_secret
                )

        except TwitterSearchException as e:
            #print(e)
            return e


        for tweet in ts.search_tweets_iterable(tso):
            # import pdb;pdb.set_trace()
            self.df['user'] = tweet['user']['name']
            self.df['text'] = tweet['text']
            self.df['id'] = tweet['id']  #use for retrieving later
            self.df['creation_time'] = tweet['created_at']
            self.df['favorite_count'] = tweet['favorite_count']
            self.df['rewteet_count'] = tweet['retweet_count']
            self.geo = tweet['geo']
            self.coordinates = tweet['coordinates'] 
            
        

            # print('user: {}, text: {}'.format(tweet['user'], tweet['text']))

        def Transform_Tweets(self):
            pass


if __name__ == "__main__":

    # ''' payload for given user '''
    payload = {
        'consumer_key': 'i8OyeJXxEtkns5gaMM5UL8gLa',
        'consumer_secret': 'YEj5Gdrp5lu12WhHBQhuQZT8fSfP26k8u8OH3MHu4eoqKNVV4s',
        'access_token': '457945249-G0vnf3fnyYeqrCnKm2Bldb6fnD6JeJykkCu6yApg',
        'access_token_secret': '6pbqFfAK4D6nU2h9tv3PRf9UxdzIQ6acgRyAT9fqiTHeE'
    }


    df = Twitter_ETL(payload, ['Bloomberg'], 'en').Extract_Tweets()
    import pdb;pdb.set_trace()

    # yesterday = ...

    # while (type(df) is not str) and (df['creation_time'].min(skipna=True) > yesterday):
    #     df.append(Twitter_ETL(payload, ['Bloomberg'], 'en').Extract_Tweets())
