#Page Rank sequential version
import json
import numpy as np

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

def sequential_pageRank(filename,iteration,d):
    data=lire_data(filename)

    n = len(data)
    L = initialize_L(data)
    r = [1 / n for i in range(n)]

    k=0
    while (k<iteration) :
        r=np.dot(L,r)*d+(1-d)
        k+=1

    r2 = {data[i]["url"]: r[i] for i in range(n)}

    # Trier le résultat
    sortedDict = sorted(r2.items(), key=lambda x: x[1], reverse=True)

    return sortedDict