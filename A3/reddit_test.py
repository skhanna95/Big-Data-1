from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
# add more functions as necessary


#def main(inputs, output):
	#text = sc.textFile(inputs)
	#x = text.map(json.loads)
inputs1 = sys.argv[1]

output = sys.argv[2]

conf = SparkConf().setAppName("reddit averages")
sc = SparkContext(conf=conf)

text = sc.textFile(inputs1)

def parse_str(line):
    str = json.loads(line)
    key = str.get("subreddit")
    count = 1
    score = str.get("score")
    dct = {"key": key, "pair": (count, score)}
    #return dct
	return x.map(dct0)

def add_pairs(x,y):
    (a,b)=x
    (c,d)=y
    return (a+c,b+d)

def score_pair(abc):
	x=abc[0]
	#(b,c)=bc
	#x=a
	#div=float(b/c)
	div=float(abc[1][0]/abc[1][1])
	return (x,div)

#dcts = text.map(lambda line: parse_str(line))
dcts = text.map(parse_str)
#lines = dcts.map(lambda dct: (dct.get("key"), dct.get("pair")))
dcts.saveAsTextFile(output)
#reduced_lines = lines.reduceByKey(add_pairs).cache()
#reduced_lines.saveAsTextFile(output)
#averaged_lines = reduced_lines.map(score_pair)
#averaged_lines.saveAsTextFile(output)
#json_lines = averaged_lines.map(lambda line: json.dumps(line))
#json_lines.saveAsTextFile(output)
		
'''if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
'''