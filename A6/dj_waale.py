import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
sc = spark.sparkContext
def min_distance(x,y):
	(source1, dist1)=x
	(source2, dist2)=y
	if dist1 <= dist2:
		return (source1, dist1)

	else:
		return (source2, dist2)

def funct2(a):
	a1=(int)(a[0])
	b1=(int)(a[1])
	return (a1,b1)

def fetch_edges(line):
    line = line.split(':')
    length = len(line)
    elt1 = int(line[0])
    if (length==1):
        return None

    if (length>1):
        str2 = line[1]
        str2 = str2.split(' ')
        str2 = str2[1:]
        length2 = len(str2)
        list_i=[]
        for i in range(length2):
            list_i.append((elt1,int(str2[i])))
        return list_i

def tup(line,i):
	x=line[0]
	y=line[1][0]
	z=line[1][1]
	if z==i:
		return True

def change(line):
	node=line[0]
	dest=line[1][0]
	src=line[1][1][0]
	dist=line[1][1][1]
	return (dest,(node,dist+1))

def main():

	inputs = sys.argv[1]
	output = sys.argv[2]
	sourceNode = int(sys.argv[3])
	destinationNode = int(sys.argv[4])
	text = sc.textFile(inputs)
	edges = text.map(fetch_edges)
	edges = edges.filter(bool)
	edges=edges.flatMap(lambda x: x)
	edges = edges.map(funct2)
	edges.coalesce(1).saveAsTextFile(output)

	knownPath = sc.parallelize([(1, ('',0))])
	for i in range(6):
		joinEdgesWithKnownPath = edges.join(knownPath.filter(tup(knownPath,i))
		intermediatePath = joinEdgesWithKnownPath.map(change)
		newpath = knownPath.union(intermediatePath).reduceByKey(min_distance)
		print (newpath.collect())
		newpath.coalesce(1).saveAsTextFile(output + '/iter-' + str(i))
'''
	inputForTheProgram = sc.textFile(inputs)
	splittedInput = inputForTheProgram.map(lambda input : input.split(':')).map(lambda (node, edges) : (int(node), map(int, edges.split())))
	edges = splittedInput.flatMapValues(lambda value : value)
	finalPath = [int(destinationNode)]

	while destinationNode!= sourceNode and destinationNode != '':
		print destinationNode
		lookUpValue = knownPath.lookup(destinationNode)
		print lookUpValue


		if lookUpValue[0][0] =='':
			break

		else:
			destinationNode = lookUpValue[0][0]
			finalPath.append(destinationNode)


	print finalPath
	finalPath = finalPath[::-1]
	finalPath = sc.parallelize(finalPath)
	finalPath.saveAsTextFile(output + '/path')
'''
if __name__ == "__main__":
	main()
