#Page Rank sequential version
import json
import numpy as np

import pyspark
from pyspark import SparkContext


def parallel_pageRange(filename):
    sc = SparkContext("local", "First App")
    my_RDD_strings = sc.textFile("data/" + filename)
    my_RDD_dictionaries = my_RDD_strings.map(json.loads)

    print(my_RDD_dictionaries)

    return 0
