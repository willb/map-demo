import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *

def firstAndLast(df):
    # assumes speed in m/s (Garmin fitness device style)
    withMinutes = df.select(df["timestamp"], (floor(df["timestamp"] / 1000 / 60) * 60).alias("minute"), ((df["speed"] * 60 * 60) / 1600).alias("mph"), df["latlong.lat"], df["latlong.lon"])
    
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
        
if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
