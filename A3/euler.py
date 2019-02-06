import sys
import random
import operator
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("euler")
sc = SparkContext(conf=conf)

sample_input = float(sys.argv[1])
def frange(start, stop=None, step=None):
    #Use float number in range() function
    # if stop and step argument is null set start=0.0 and step = 1.0
    if stop == None:
        stop = start + 0.0
        start = 0.0
    if step == None:
        step = 1.0
    while True:
        if step > 0 and start >= stop:
            break
        elif step < 0 and start <= stop:
            break
        yield ("%g" % start) # return float number
        start = start + step

def counter(iteration):
    count = 0
    for i in frange(iteration):
        sum = 0.0
        while (sum < 1):
            sum += random.random()
            count += 1
    return count


def main():
    partition_slices = 150
    iteration_count = sample_input/partition_slices

    list_store = []
    for i in range(partition_slices):
        list_store.append(iteration_count)

    create_rdd = sc.parallelize(list_store, partition_slices)
    count_rdd = create_rdd.map(counter)
    claculate = count_rdd.reduce(operator.add)
    result = float(claculate)/sample_input
    print("\n\n\n")
    print (result)

if __name__ == "__main__":
    main()
