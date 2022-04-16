#Page Rank sequential version
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
    rdd2=rdd.flatMap(proba_link)
    for element in rdd2.collect() :
        print(element)


    return 0

def proba_link(data):
    # transform data=('id','url','size',[neighbors]) into multiple ('id', 'neighbors','nombre de voisin')
    print(data)
    k=len(data["neighbors"])
    rep=[]
    for neighbor in data["neighbors"] :
        rep.append( {   'id' : data['id'],
                        'neighbor' : neighbor,
                        'proba' : 1/k })
    return rep

filename="python.org.json"
d=0.85 #damping_factor
iteration=100

parallel_pageRange(filename,iteration,d)



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



