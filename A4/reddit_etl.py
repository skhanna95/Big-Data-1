
from pyspark import SparkConf, SparkContext
import sys
import json                       #Added
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from operator import is_not
from functools import partial
# add more functions as necessary

def add_pairs(x,y):
    (a,b) = x
    (c,d) = y
    return (a+c,b+d)

def key_value(text):
    key = text.get("subreddit")  #subreddit as key
    val1 = text.get("score")     #count, score_sum pair as value
    count=1
    return(key,(val1,count))

def avg_score(xyz):
    x,(y,z) = xyz
    a_score = float(y/z)
    return (x,a_score)

def split(text):
    subreddit = text.get("subreddit")  #subreddit as key
    score = text.get("score")     #count, score_sum pair as value
    author=text.get("author")
    transform = {"author": author, "subreddit": subreddit,"score":score}
    return transform
def search(text):
    s=text.get("subreddit")
    score = text.get("score")     #count, score_sum pair as value
    author=text.get("author")
    if 'e' in s:
            transform = {"author": author, "subreddit": s,"score":score}
            return transform
            
def main(inputs, output):
    text = sc.textFile(inputs)
    outdata = text.map(json.loads)
    #outdata.saveAsTextFile(output)
    new=outdata.map(search)
    new = new.filter(partial(is_not, None))
    #new.saveAsTextFile(output)
    new_out=new.map(key_value)
    #new_out.saveAsTextFile(output)
    f_out=new_out.reduceByKey(add_pairs).cache()
    f2_out=f_out.map(avg_score)
    #f2_out.saveAsTextFile(output+'/3')
    
    f3_out=f2_out.filter(lambda n:n[1]>0)
    f3_out.map(json.dumps).saveAsTextFile(output + '/positive')
    f4_out=f2_out.filter(lambda n:n[1]<=0)
    f4_out.map(json.dumps).saveAsTextFile(output + '/negative')
    
    #f3_out.saveAsTextFile(output)
if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average code')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)






#-------------------------------------------------------------------------------------------------
