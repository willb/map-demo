from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from functools import reduce
import re
import urllib2

import pymongo
from bson.binary import Binary

from uuid import uuid4, UUID

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
    df = df.withColumn("timestamp", unix_timestamp(df["timestamp"]) * 1000)
    return df.select(df["timestamp"], (floor(df["timestamp"] / 1000 / 60) * 60).alias("minute"), df["speed"].cast("double").alias("mph"), df["latitude"].cast("double").alias("lat"), df["longitude"].cast("double").alias("lon"))


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
    """ returns summary data and JSON polyline """
    original = url2df(spark, url)
    df = None
    if 'vin' in original.columns:
        df = convertObd(original)
    else:
        df = convertGarmin(original)

    faldf = firstAndLast(df)
    summary = faldf.rdd.map(lambda r: [long(r.minute), float(r.firstLat), float(r.firstLon), float(r.lastLat), float(r.lastLon), float(r.maxMph)]).collect()

    return (summary, polyline(faldf))
    


def prime(dirname, inq, db):
    """ sets up summaries for existing data """
    import glob, os, os.path

    print("priming db with data from %s/%s" % (os.getcwd(), dirname))
    
    for js in glob.glob(os.getcwd() + "/%s/*.json" % dirname):
        print(js)
        job = {"url": js, "_id": uuid4(),
               "name": os.path.basename(js), "status": "training"}
        db.summaries.insert_one(job)
        inq.put(job)

def workloop(master, inq, outq, dburl):
    spark = SparkSession.builder.master(master).getOrCreate()
    sc = spark.sparkContext

    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    
    if dburl is not None:
        db = pymongo.MongoClient(dburl).mapdemo

    prime("static", inq, db)
    
    outq.put("ready")

    while True:
        job = inq.get()
        url = job["url"]
        mid = job["_id"]
        summary, polyline = train(spark, url)

        # XXX: do something with callback here
        print("processing job for %s" % url)
        
        if dburl is not None:
            db.summaries.update_one(
                {"_id": mid},
                {"$set": {"status": "ready",
                          "polyline": polyline, "summary": summary},
                 "$currentDate": {"last_updated": True}}
            )

        outq.put((mid, job["name"]))
