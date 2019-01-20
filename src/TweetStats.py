import redis
import time,datetime
from datetime import timedelta

POOL = redis.ConnectionPool(host='127.0.0.1',port=6379,db=0)
TAGS = 'tags'
TAG_COUNT = '_tagcount'
FASHION = 'fashion'
TIMELINE = "timeline"
current_milli_time = lambda: int(round(time.time() * 1000))
stat_server = redis.Redis(connection_pool=POOL)


def setHashTags(tagCollection,time):
    stat_server = redis.Redis(connection_pool=POOL)
    for key,value in tagCollection.items():
        if key == FASHION:
            incrFashionTagCount(time)
        else:
            stat_server.zincrby(TAGS,value,key)


def getTopHashTags(count):
    output=stat_server.zrevrange(TAGS,1,count,withscores=True)
    decoded_output = decodeHashTags(output)
    return decoded_output


def incrFashionTagCount(key):
    incrHashTagCount(FASHION+TAG_COUNT,convertTimeStampToDate(key))


def incrHashTagCount(hash,key):
    stat_server = redis.Redis(connection_pool=POOL)
    stat_server.hincrby(hash,key,amount=1)

def getHashTagCount(list_keys):
    output = stat_server.hmget(FASHION+TAG_COUNT,list_keys)
    return output

#Decodes key in dic
def decodeHashTags(output):
    return [(element[0].decode('utf-8'),element[1]) for element in output]

#Decodes elements in list
def decodeHashTagCount(count_list):
    return [element.decode('utf-8') for element in count_list if element != None]

def convertTimeStampToDate(key):
    date = datetime.datetime.fromtimestamp(key/1000.0)
    date = date.strftime('%Y%m%d%H%M')
    return date


def mergeTimeStampHashtagCount(timeline,tag_count):
    merged_output = [(datetime.datetime.strptime(timeline[index], '%Y%m%d%H%M').strftime('%H:%M'),count) for index,count in enumerate(tag_count)]
    return merged_output

def sortKey(key):
    return key[0]

def getSortedHashTagOutput(unsorted_output):
    unsorted_output.sort(key=sortKey,reverse=True)
    return [(datetime.datetime.strptime(element[0], '%Y%m%d%H%M').strftime('%H:%M'),element[1])for element in unsorted_output]

def getHashTagCountLastHour():
    date = datetime.datetime.now()
    timekeys=[]
    for timestamp in range(61):
        minutegranular = date - timedelta(minutes=timestamp)
        timekeys.append(minutegranular.strftime('%Y%m%d%H%M'))
    output = getHashTagCount(timekeys)
    result = decodeHashTagCount(output)
    return mergeTimeStampHashtagCount(timekeys,result)


print(getTopHashTags(100))

print(getHashTagCountLastHour())

