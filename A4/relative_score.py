from pyspark import SparkConf, SparkContext
import json
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def add_tuples(t1, t2):
    return tuple(sum(p) for p in zip(t1, t2))

def score_pair(abc):
	x=abc[0]
	div=float(abc[1][0]/abc[1][1])
	return (x,div)
def key_value(text):
    key = text.get("subreddit")  #subreddit as key
    val1 = text.get("score")     #count, score_sum pair as value
    count=1
    return(key,(val1,count))
def final_calc(abc):
    x=abc[0]
    y=abc[1][0].get('score')
    z=abc[1][1]
    div= (y/z)
    a=abc[1][0].get('author')
    return (x,div,a)

def main(inputs, output):
    text = sc.textFile(inputs)
    comments = text.map(json.loads).cache()
    commentbysub = comments.map(lambda c: (c['subreddit'], c))
    #commentbysub.saveAsTextFile(output)
    c_score = comments.map(key_value)
    c_scoresum = c_score.reduceByKey(add_tuples)
    #c_scoresum.saveAsTextFile(output +'/1')
    c_avgscore = c_scoresum.map(score_pair)
    #c_avgscore.saveAsTextFile(output+'/1')
    c_avgscore=c_avgscore.filter(lambda n:n[1]>0)
    reddits = commentbysub.join(c_avgscore).cache()
    #reddits.saveAsTextFile(output+'/2')
    outdata=reddits.map(final_calc)
    outdata=outdata.filter(lambda n:n[1]>0)
    outdata=outdata.sortBy(lambda x: x[1], ascending = False)
    outdata.saveAsTextFile(output)
	
if __name__ == '__main__':
    conf = SparkConf().setAppName('relative score')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)