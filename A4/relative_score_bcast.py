from pyspark import SparkConf, SparkContext
import json
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def add_pairs(x,y):
    (a,b) = x
    (c,d) = y
    return (a+c,b+d)

def key_value_pair(text):
    return (text['subreddit'],(1,text['score']))

def score_pair(abc):
	x=abc[0]
	div=float(abc[1][0]/abc[1][1])
	return (x,div)

def final_calc(abc):
    x=abc[0]
    y=abc[1][0].get('score')
    z=abc[1][1]
    div= (y/z)
    a=abc[1][0].get('author')
    return (x,div,a)

def output_format(kv):
    k, v = kv
    return '[%f %s]' % (k, v)

def score_comm(comment, b_cast):
    return (comment[1]['score']/b_cast.value[comment[0]], comment[1]['author'])

def main(inputs, output):
    text = sc.textFile(inputs)
    inp_comment=text.map(json.loads)
    commentbysub = inp_comment.map(lambda c: (c['subreddit'], c))
    #commentbysub.saveAsTextFile(output+'/1')
    
    key_value_RDD = inp_comment.map(key_value_pair)
    subredd_count_score = key_value_RDD.reduceByKey(add_pairs)
    average = subredd_count_score.map(score_pair) 
    brd_var = sc.broadcast(dict(average.collect()))
    average_positive = average.filter(lambda n: n[1]>0)

    relative_score_rdd = commentbysub.map(lambda x: score_comm(x,brd_var))
    #relative_score_rdd.saveAsTextFile(output + '/nn')
    
    outdata = relative_score_rdd.sortBy(lambda s:s[0], ascending = False).map(output_format)
    #outdata.saveAsTextFile(output)
    outdata = outdata.map(lambda d: json.dumps(d))
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('relative score bcast')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)