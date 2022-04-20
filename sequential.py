#Page Rank sequential version
import json
import numpy as np

def lire_data(filename):
    with open("data/" + filename) as json_data:
        data_dict = json.load(json_data)
    return(data_dict)

#Define matrix of probability (Probability to go from one page to another page)
def initialize_L(data):
    n=len(data)
    L=[[ 0 for j in range(n)] for i in range(n)]
    for i in range(n):
        k_connection=len(data[i]["neighbors"])
        for j in range(n):
            if j in data[i]["neighbors"] :
                L[j][i]=1/k_connection
    return L

#iteration: Iteration before stoping, must be high enough for convergence
#d: damping factor to reduce the impact of dead end
def sequential_pageRank(filename,iteration,d):
    data=lire_data(filename)

    n = len(data)
    L = initialize_L(data)

    #initialize probability to uniform distribution
    probability = [1 / n for i in range(n)]

    for i in range(iteration):
        probability=np.dot(L,probability)*d+(1-d)

    #Associate url with their respective probability
    r2 = {data[i]["url"]: probability[i] for i in range(n)}

    # Sort results
    sortedDict = sorted(r2.items(), key=lambda x: x[1], reverse=True)

    return sortedDict