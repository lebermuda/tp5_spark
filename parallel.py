#Page Rank sequential version

#https://cocalc.com/share/public_paths/960fae18301f8e8cb29472dc3ff5d0acf5659ec8/data-analysis%2Fspark-pagerank.ipynb


import json
import numpy as np

import pyspark
from pyspark import SparkContext


def parallel_pageRange(filename,iteration,d):

    sc = SparkContext("local", "First App")
    my_RDD_strings = sc.textFile("data/" + filename)
    # type(my_RDD_strings) = <class 'pyspark.rdd.RDD'>
    my_RDD_dictionaries = my_RDD_strings.map(json.loads)
    # type(my_RDD_dictionaries) = <class 'pyspark.rdd.PipelinedRDD'>
    rdd=sc.parallelize(my_RDD_dictionaries.collect()[0])
    # for element in rdd.collect() :
    #     print(element)

    ranks = rdd.map(lambda x : (x['id'], 1.))
    # ranks = sc.parallelize(link_data.keys()).map(lambda x : (x, 1.))
    
    urls = rdd.map(lambda x : (x['id'], x['url']))

    links=rdd.map(lambda x : (x['id'], x['neighbors']))
    # links = sc.parallelize(link_data.items()).cache()


    n=len(ranks.collect())
    for i in range(iteration):
        # compute contributions of each node where it links to
        contribs = links.join(ranks).flatMap(calculProba)

        # use a full outer join to make sure, that not well connected nodes aren't dropped
        contribs = links.fullOuterJoin(contribs).mapValues(lambda x : x[1] or 0.0)

        # Sum up all contributions per link
        ranks = contribs.reduceByKey(somme)

        # Re-calculate URL ranks
        ranks = ranks.mapValues(lambda rank: rank * d + (1-d))
        
        # Reduce partition
        ranks = ranks.coalesce(sc.defaultParallelism)

        # Force evaluation
        count = ranks.count()

    # Collects all URL ranks
    resultat=ranks.join(urls).sortBy(lambda x : x[1],ascending=False).map(lambda x : (x[1][1] , x[1][0]/n))

    return resultat.collect()


def calculProba(data):
    """
    This function takes elements from the joined dataset above and
    computes the contribution to each outgoing link based on the
    current rank.
    """
    _,(neighbors,rank) = data
    nb_neighbors = len(neighbors)
    for neighbor in neighbors:
        yield neighbor, rank / nb_neighbors

def somme(a,b):
    return a+b

# filename="python.org.json"
# d=0.85 #damping_factor
# iteration=50
# res_par=parallel_pageRange(filename,iteration,d)
# print(res_par[:3])



def recherchePi(NUM_SAMPLE):
    sc = SparkContext("local", "First App")

    trials = sc.range(0,NUM_SAMPLE)
    # type(trials) =<class 'pyspark.rdd.PipelinedRDD'>
    in_circle = trials.map(sample)
    pi = in_circle.reduce(reduce_fonc) * 4 /(NUM_SAMPLE)

    print("The constant Pi can be approximated by:", pi)
    return pi

def sample(p):
    x, y = np.random.rand(), np.random.rand()
    if (x**2 + y**2 < 1) :
        return 1     
    else :
        return 0

def reduce_fonc(a,b):
    return a + b 



