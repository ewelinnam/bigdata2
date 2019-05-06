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
    cityData = '500Cities_Tracts_11082016/500Cities_Tracts_Clip.shp'
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

            p1 = geo.Point(proj(float(pdt[2]), float(pdt[1])))
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
                            found = idx
                            tract = cities.plctract10[idx]
                            population = cities.PlcTrPop10[idx]
                            break
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
