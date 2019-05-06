from pyspark import SparkContext

def processTweets(pid, records):
    import re
    import geopandas as gpd
    import sys
    import shapely.geometry as geo
    import rtree
    import pyproj
    import fiona.crs

    drugs1 = 'drug_sched2.txt'
    drugs2 = 'drug_illegal.txt'
    cityData = '500cities_tracts.geojson'
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    separator = re.compile('\W+')

    drugs = (open(drugs1, 'r').readlines() + open(drugs2, 'r').readlines())
    drugs = set(map(lambda x: x.strip(),drugs))

    cities = gpd.read_file(cityData).to_crs(fiona.crs.from_epsg(2263))
    count = {}
    index1 = rtree.Rtree()
    for idx,geometry in enumerate(cities.geometry):
        index1.insert(idx, geometry.bounds)

    for row in records:

        pdt = row.split('|')
        try:
            try:
                p1 = geo.Point(proj(float(pdt[2]), float(pdt[1])))
            except:
                pass
            line =pdt[-1]
            line = separator.split(line.lower())

            tract = 0
            match1 =  list(index1.intersection((p1.x, p1.y)))
            population = 1
            found = None
            for drug in drugs:
                if len(set(drug.split()).intersection(line))==len(drug.split()):
                    for idx in match1:
                        shape = cities.geometry[idx]
                        if shape.contains(p1):
                            try:
                                found = idx
                                tract = cities.plctract10[idx]
                                population = cities.plctrpop10[idx]
                                break
                            except:
                                pass
                    if found:
                         count[tract] = count.get(tract, 0) + (1/population)
        except:
            pass

    return list((count.items()))

if __name__ == "__main__":

    import sys
    sc = SparkContext()
    tweets = sys.argv[1]
    counts = sc.textFile(tweets).mapPartitionsWithIndex(processTweets).\
    reduceByKey(lambda x,y: x+y).filter(lambda x: x is not None).sortBy(lambda x: -x[1])
    #counts.saveAsTextFile('counts')
    print(counts.collect())
    
#running command:
#park-submit --conf spark.executorEnv.LD_LIBRARY_PATH=$LD_LIBRARY_PATH --executor-memory 15GB --executor-cores 5 --num-executors 10 --files hdfs:///tmp/bdm/500cities_tracts.geojson,hdfs:///tmp/bdm/drug_sched2.txt,hdfs:///tmp/bdm/drug_illegal.txt \challenge2.py testfile.csv