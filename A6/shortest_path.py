'''                 BIG DATA ASSIGNMENt-6
                    CMPT-732
                    SHRAY KHANNA
                    301367221
'''
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('shortest path').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
sc = spark.sparkContext

def rdd_convert(line):
	x=line[0]
	y=line[1]
	z=line[2]
	return (x,(y,z))

# add more functions as necessary
def get_graphedges(line):
    list1 = line.split(':')
    if list1[1] == '':
        return None
    list2 = list1[1].split(' ')
    list2 = filter(None, list2)
    results = []
    s = list1[0]
    for d in list2:
        results.append((s, d))
    return results

schema = StructType([
StructField('node', StringType(), False),
StructField('source', StringType(), False),
StructField('distance', IntegerType(), False),
])

def main(inputs, output,source_node,dest_node):
	textinput = sc.textFile(inputs)
	edges_rdd = textinput.map(get_graphedges).filter(lambda x: x is not None).flatMap(lambda x: x).coalesce(1)
	edges = edges_rdd.toDF(['source', 'destination']).cache()
	edges.registerTempTable('Source_Table')
	
	Create_header = Row('node', 'source', 'distance')
	initial_node = source_node
	initial_row = Create_header(initial_node, initial_node, 0)
	
	known_paths = spark.createDataFrame([initial_row], schema=schema)
	part_knownpaths = known_paths
	for i in range(6):
		part_knownpaths.registerTempTable('Part_known')
		newpaths = spark.sql("""
			SELECT destination AS node, t1.source AS source, (distance+1) AS distance FROM
			Source_Table t1
			JOIN
			Part_known t2
			ON (t1.source = t2.node)
			""")
		newpaths.registerTempTable('NewPathTable')
		known_paths.registerTempTable('KnowPathTable')
		duplicate_df = spark.sql("""
			SELECT t1.node AS node, t1.source as source, t1.distance as distance FROM
			NewPathTable t1
			JOIN
			KnowPathTable t2
			ON (t1.node = t2.node)
			""")
		if duplicate_df.count() != 0:
			newpaths = newpaths.subtract(duplicate_df)

		part_knownpaths = newpaths
		known_paths = known_paths.unionAll(newpaths)
		known_paths.write.save(output + '/iter' + str(i), format='json')
	
	knownpaths_rdd = known_paths.rdd
	knownpaths_map = knownpaths_rdd.map(rdd_convert).cache()
	paths = []
	paths.append(dest_node)
	dest = knownpaths_map.lookup(dest_node)
	for j in range(6):
		if not dest:
			paths = ['invalid destination']
			break
		parent = dest[0][0]
		paths.append(parent)
		if parent == source_node:
			break
		dest = knownpaths_map.lookup(parent)
	paths = reversed(paths)
	outdata = sc.parallelize(paths)
	outdata.coalesce(1).saveAsTextFile(output + '/path')

    # main logic starts here

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    source_node = sys.argv[3]
    dest_node = sys.argv[4]

    main(inputs, output,source_node,dest_node)