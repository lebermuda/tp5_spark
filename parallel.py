import json
import numpy as np

import pyspark
from pyspark import SparkContext, SparkConf

def parallel_pageRank2(filename,iteration,d,p):
    config = SparkConf().setMaster("local["+str(p)+"]")
    sc = SparkContext.getOrCreate(conf=config)

    my_RDD_strings = sc.textFile("data/" + filename)
    my_RDD_dictionaries = my_RDD_strings.map(json.loads)
    rdd=sc.parallelize(my_RDD_dictionaries.collect()[0])

    urls = rdd.map(lambda x : (x['id'], x['url']))
    n = len(urls.collect())
    ranks = rdd.map(lambda x : (x['id'], 1 / n))

    links=rdd.map(lambda x : (x['id'], x['neighbors']))

    for i in range(iteration):
        contribs= links.join(ranks).flatMap(lambda x: [(i, [1 / len(x[1][0]) * x[1][1]]) for i in x[1][0]]).reduceByKey(lambda x,y : x + y)
        ranks=contribs.mapValues(lambda x: sum(x)).mapValues(lambda x: x * d + (1-d)).coalesce(sc.defaultParallelism)

        #force spark to eval to not get stackoverflow
        if i % 10 == 0:
            ranks.collect()

    resultat=ranks.join(urls).sortBy(lambda x : x[1],ascending=False).map(lambda x : (x[1][1] , x[1][0]))

    return resultat.collect()


def calculProba(data):
    _,(neighbors,rank) = data
    nb_neighbors = len(neighbors)
    for neighbor in neighbors:
        yield neighbor, rank / nb_neighbors

def somme(a,b):
    return a+b

