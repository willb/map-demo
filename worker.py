from pyspark import SparkSession

from functools import reduce
import re
import urllib2
import zlib


from numpy import ndarray
import pymongo
from bson.binary import Binary


def cleanstr(s):
    noPunct = re.sub("[^a-z ]", " ", s.lower())
    collapsedWhitespace = re.sub("(^ )|( $)", "", re.sub("  *", " ", noPunct))
    return collapsedWhitespace


def url2df(spark, url):
    sc = spark.sparkContext
    if url.startswith("http") or url.startswith("ftp"):
        response = urllib2.urlopen(url)
        text = response.read()
        rdd = sc.parallelize(text.split("\n"))
    else:
        rdd = sc.textFile(url)

    return spark.read.json(rdd)

def convertGarmin(df):
    """ Converts a garmin-style record (i.e., speed in m/s, UNIX timestamps in ms)
        to the style we'd like """
    return df.select(df["timestamp"], (floor(df["timestamp"] / 1000 / 60) * 60).alias("minute"), ((df["speed"] * 60 * 60) / 1600).alias("mph"), df["latlong.lat"], df["latlong.lon"])

def convertObd(df):
    """ Prepares Mike's OBD records for summarization """
    df = df.withColumn("timestamp", unix_timestamp(df["timestamp"] * 1000))
    return df.select(df["timestamp"], (floor(df["timestamp"] / 1000 / 60) * 60).alias("minute"), df["speed"].alias("mph"), df["latitude"].alias("lat"), df["longitude"].alias("lon"))


def firstAndLast(withMinutes):
    aggs = withMinutes.select(withMinutes["timestamp"], withMinutes["minute"], withMinutes["mph"]).groupBy("minute").agg(min(column("timestamp")).alias("minTs"), max(column("timestamp")).alias("maxTs"), max(column("mph")).alias("maxMph"))
    
    fl = withMinutes.join(aggs, (withMinutes.minute == aggs.minute) & ((withMinutes.timestamp == aggs.minTs) | (withMinutes.timestamp == aggs.maxTs))).select(withMinutes.minute, "maxMph", "lat", "lon", when(column("timestamp") == column("maxTs"), "last").otherwise("first").alias("position")).cache()
    
    firsts = fl.filter(fl.position == "first").select(fl.minute, fl.maxMph, fl.lat.alias("firstLat"), fl.lon.alias("firstLon"))
    lasts = fl.filter(fl.position == "last").select(fl.minute, fl.lat.alias("lastLat"), fl.lon.alias("lastLon"))
    
    return firsts.join(lasts, firsts.minute == lasts.minute).select(firsts.minute, firsts.maxMph, "firstLat", "firstLon", "lastLat", "lastLon")



def polyline(fldf, binfunc=None, binct=5, colors=["#5e3c99", "#b2abd2", "#f7f7f7", "#fdb863", "#e66101"]):
    import numpy
    
    tups = [((f.firstLat, f.firstLon), (f.lastLat, f.lastLon), f.maxMph) for f in fldf.orderBy("minute").collect()]
    if binfunc is None:
        cts,bins = numpy.histogram([tup[2] for tup in tups], bins=binct)
        bins = zip(list(bins)[1:], range(binct))
        def bf(sample):
            for ceiling, bn in bins:
                print (sample, ceiling)
                if sample <= ceiling:
                    return bn
            return bn
        binfunc = bf
    result = []
    lastbin = None
    
    for start, fin, mph in tups:
        bn = binfunc(mph)
        result.append({"color": colors[bn], "points": [list(start), list(fin)]})
    return result



def train(spark, url):
    original = url2df(spark, url)
    df = None
    if 'vin' in original.columns:
        df = convertObd(original)
    else:
        df = convertGarmin(original)
    
    result = polyline(firstAndLast(df))
    

def workloop(master, inq, outq, dburl):
    spark = SparkSession.builder.master(master).getOrCreate()
    sc = spark.sparkContext
    
    if dburl is not None:
        db = pymongo.MongoClient(dburl).mapdemo

    outq.put("ready")

    while True:
        job = inq.get()
        urls = job["urls"]
        mid = job["_id"]
        model = train(sc, urls)

        items = model.getVectors().items()
        words, vecs = zip(*[(w, list(v)) for w, v in items])

        # XXX: do something with callback here

        if dburl is not None:
            ndvecs = ndarray([len(words), len(vecs[0])])
            for i in range(len(vecs)):
                ndvecs[i] = vecs[i]

            ns = ndvecs.dumps()
            zns = zlib.compress(ns, 9)

            print("len(ns) == %d; len(zns) == %d" % (len(ns), len(zns)))

            db.models.update_one(
                {"_id": mid},
                {"$set": {"status": "ready",
                          "model": {"words": list(words), "zndvecs": Binary(zns)}},
                 "$currentDate": {"last_updated": True}}
            )

        outq.put((mid, job["name"]))
