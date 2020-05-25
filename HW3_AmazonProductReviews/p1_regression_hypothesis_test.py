
import numpy as np
import json
import re
import sys


from pyspark import SparkContext
sc = SparkContext()

def filterfields(data):
  try:
    if data['reviewText']:
      return [data['overall'],data['reviewerID'],data['asin'],data['reviewText'].lower(),data['verified']]
  except KeyError:
    pass

def regexMatch(data,pattern):
  splittedlst=re.findall(pattern,data[3])
  return splittedlst

def regexMatch2(data,pattern):
  splittedlst=re.findall(pattern,data[3])
  return (data[0],data[4],splittedlst)

def helpermethod(data):
  #add up count of each word in each list i.e. review=>data is a list
  word_dict=dict()
  lst=[]
  for item in data[2]:
    word_dict[item]=word_dict.get(item,0)+1
  for key in word_dict:
    lst.append((key,word_dict[key])) #add word,total count in this review,total words in this review
  return (data[0],data[1],len(data[2]),lst)

def combineFreqWords(data,cache):
  verified =0
  if data[1] is True:verified=1
  lst=[]
  # for word,count in data[3]: #if reviews where frequent words are not present are to be ignored i.e.zeros to be ignored
  #   if word in cache.value:
  #     lst.append((word,count))
  review_worddict=dict()
  for w,c in data[3]:
    review_worddict[w]=c
  for freqWord in cache.value:
    isPresentInreview=0
    if freqWord in review_worddict:
      lst.append((freqWord,review_worddict[freqWord]))
    else:
      lst.append((freqWord,0)) 
  ret_lst=[]
  for freqWord,count in lst:#COUNT is total number of times the 1000 list word is in this review
    if data[2]!=0:
      ret_lst.append((freqWord,(data[0],verified,count/data[2])))# (word,(rating,verified,count/total words in this review))
  return ret_lst

def standardize(data):
  #data:(word,[(rating1,verified1,relativefreq1),(rating2,verified2,relativefreq2),(..)])
  means=np.mean(data[1],axis=0)
  sd=np.std(data[1],axis=0)
  lst=[]
  for r,v,f in data[1]:
    lst.append(( (r-means[0])/sd[0],(v-means[1])/sd[1],(f-means[2])/sd[2] ))
  return ((data[0],lst))

def singleLinear(data):
  #data:(word,[(rating1,verified1,relativefreq1),(rating2,verified2,relativefreq2),(..)])
  Y=[]
  X=[]
  for r,v,f in data[1]:
    Y.append([r])
    X.append([f])
  a=np.linalg.inv(np.dot(np.transpose(X),X))
  b=np.dot(np.transpose(X),Y)
  beta=np.dot(a,b)
  beta=beta[0][0]
  return (data[0],(beta),data[1])
  # return (data[0],list(c),data[1]) #(word,[beta],[(r,v,f),()..])

def MultiLinear(data):#standardized input data
  #data:(word,[(rating1,verified1,relativefreq1),(rating2,verified2,relativefreq2),(..)])
  Y=[]
  X=[]
  for r,v,f in data[1]:
    Y.append([r])
    X.append([1,f,v])
  a=np.linalg.inv(np.dot(np.transpose(X),X))
  b=np.dot(np.transpose(X),Y)
  c=np.dot(a,b)
  beta=(c[0][0],c[1][0],c[2][0])#beta for b_0,fre,verified
  return (data[0],beta,data[1])
  # return Y

import scipy.stats as stats
def t_value(data,isLinear):
  #(word,(beta1-freq,beta2-verified),[(r,v,f)]) for mutli and single beta of mlinear
  betas=[]
  beta_freq=0
  if isLinear:
    M=1
    beta_freq=data[1]
  else:
    M=2
    betas.append([data[1][0]]) #beta_0
    betas.append([data[1][1]]) #beta fre
    betas.append([data[1][2]]) #beta verfied
    beta_freq=data[1][1]

  X,Y=[],[]
  N=len(data[2])#number of observations /reviews for each word->will be same for all as zero is included
  X_j=[]
  for r,v,f in data[2]:
    Y.append([r])    
    if isLinear:
      X.append([f])
      X_j.append([f])
    else:
      X.append([1,f,v])
      X_j.append([f])
  rss=0.0
  if isLinear:
    rss=np.sum(np.square(np.subtract(Y,np.multiply(beta_freq,X))))
  else:
    rss=np.sum(np.square(np.subtract(Y,np.dot(X,betas))))
  df=N-(M+1)
  s_squared=np.divide(rss,df)
  X_mean=0
  if isLinear:
    X_mean=np.mean(X)
  else:
    X_mean=np.mean(X_j)
  denom=np.sum(np.square(np.subtract(X_j,X_mean)))
  SE=np.sqrt(np.divide(s_squared,denom))
  t_val=np.divide(beta_freq,SE)
  p_val=(stats.t.sf(np.abs(t_val),df)*2)*1000
  return (data[0],beta_freq,p_val)

def main():
  path=sys.argv[1]
  ipfile=sc.textFile(path)
  data=ipfile.map(json.loads)
  pattern='((?:[\.,!?;"])|(?:(?:\#|\@)?[A-Za-z0-9_\-]+(?:\'[a-z]{1,3})?))'
  filteredrdd=data.map(filterfields)\
                  .filter(lambda data:data!=None)
  filteredrdd.persist()#rdd to use for relative frequency 
  topwords=filteredrdd.flatMap(lambda x:regexMatch(x,pattern)).map(lambda word:(word,1)).reduceByKey(lambda a,b:a+b).sortBy(lambda x:-x[1]).take(1000)
  cache=sc.parallelize(topwords).collectAsMap() #top words as rdd
  broadcastDict=sc.broadcast(cache)

  rdd=filteredrdd.map(lambda x:regexMatch2(x,pattern)).map(helpermethod)\
                .map(lambda reviewRec:combineFreqWords(reviewRec,broadcastDict))\
                .flatMap(lambda x:x).groupByKey().mapValues(list)
  rdd.persist()
  standardizedRdd=rdd.map(standardize)
  standardizedRdd.persist() #definitely keep this persist

  linearrdd=standardizedRdd.map(singleLinear)
  linearrdd.persist()

  multilinearrdd=standardizedRdd.map(MultiLinear)
  multilinearrdd.persist()

  linear_tvalues=linearrdd.map(lambda x:t_value(x,True))
  linear_tvalues.persist()
  mlinear_tvalues=multilinearrdd.map(lambda x:t_value(x,False))
  mlinear_tvalues.persist()

  lin_topPos=linear_tvalues.takeOrdered(20,key=lambda x:-x[1]) #top 20  positive correlations linear
  lin_topNeg=linear_tvalues.takeOrdered(20,key=lambda x:x[1])  ##top 20  negaitve correlations  linear 

  mlin_topPos=mlinear_tvalues.takeOrdered(20,key=lambda x:-x[1]) #top 20  positive correlations multi linear 
  mlin_topNeg=mlinear_tvalues.takeOrdered(20,key=lambda x:x[1])  ##top 20  negaitve correlations  multi linear 

  final_output=[lin_topPos,lin_topNeg,mlin_topPos,mlin_topNeg]
  return final_output

if __name__ == "__main__":
    op=main()
    for item in op:
      print(item)
    sc.stop()

