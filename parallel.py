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
    //SOUTIRER L de rdd2

    n=len(rdd.collect())
    v_rdd=rdd.map(lambda x : {"id":x["id"],"url" : x['url'],"rank":1/n })
    
    # POUR LE TEST
    data=lire_data(filename)
    L = initialize_L(data)

    k=0
    while (k< 5) :#iteration) :
        v=[element['rank'] for element in v_rdd.collect()]

        v_rdd=v_rdd.map(lambda x : {"id":x["id"],"url" : x['url'],"rank":pagerank(v,L[x["id"]],d,n)}) #v_rdd[i]_(i+1)=d*(L[i][:]*v_rdd_(i))+(1-d)/n
        k+=1

    final_rdd=v_rdd.sortBy(lambda x : x["rank"],ascending=False).map(lambda x : (x['url'],x["rank"]))

    print(final_rdd.collect()[:3])

    return 0


def pagerank(v,Li,d,n):
    produit_vect=0
    for i in range(n):
        produit_vect+=Li[i]*v[i]

    return d*produit_vect+(1-d)/n

def proba_link(data):
    # transform data=('id','url','size',[neighbors]) into multiple ('id', 'neighbors','nombre de voisin')
    print(data)
    k=len(data["neighbors"])
    rep=[]
    for neighbor in data["neighbors"] :
        rep.append( {   'id' : data['id'],
                        # 'url' : data['url'],
                        'neighbor' : neighbor,
                        'proba' : 1/k })
    return rep


def lire_data(filename):
    with open("data/" + filename) as json_data:
        data_dict = json.load(json_data)
    return(data_dict)

def initialize_L(data):
    n=len(data)
    L=[[ 0 for j in range(n)] for i in range(n)]
    for i in range(n):
        k_connection=len(data[i]["neighbors"])
        for j in range(n):
            if j in data[i]["neighbors"] :
                L[j][i]=1/k_connection
    return L


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



