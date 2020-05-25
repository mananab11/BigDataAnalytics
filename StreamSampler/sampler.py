##########################################################################
## Simulator.py  v 0.1
##
## Implements two versions of a multi-level sampler:
##
## 1) Traditional 3 step process
## 2) Streaming process using hashing
##
##
## Original Code written by H. Andrew Schwartz
## for SBU's Big Data Analytics Course 
## Spring 2020
##
## Student Name: KEY
## Student ID: 

##Data Science Imports: 
import numpy as np
import mmh3
from random import random

from datetime import datetime

##IO, Process Imports: 
import sys
from pprint import pprint


##########################################################################
##########################################################################
# Task 1.A Typical non-streaming multi-level sampler

def typicalSampler(filename, percent = .01, sample_col = 0):
    # Implements the standard non-streaming sampling method
    # Step 1: read file to pull out unique user_ids from file
    # Step 2: subset to random  1% of user_ids
    # Step 3: read file again to pull out records from the 1% user_id and compute mean withdrawn

    mean, standard_deviation = 0.0, 0.0
    file_obj=filename
    user_id=[] #stores unique user_ids
    user_id_subset=[] #sampleid
    sampledid_values=[]
    #Step 1
    for line in file_obj:
        userid=line.split(',')[sample_col]
        if userid not in user_id:
            user_id.append(line.split(',')[sample_col])
    #Step 2
    sample_size=int(len(set(user_id))*percent)
    user_id_subset=list(np.random.choice(a = user_id, size = sample_size, replace = False))
    #Step3
    file_obj.seek(0)
    for line in file_obj:
        if(line.split(',')[sample_col] in user_id_subset):
            sampledid_values.append(float(line.split(',')[3].strip()))
    valueAs_arr=np.array(sampledid_values) 
    mean=np.mean(np.array(valueAs_arr),axis=0)
    standard_deviation=np.std(valueAs_arr,axis=0)
    return mean, standard_deviation


##########################################################################
##########################################################################
# Task 1.B Streaming multi-level sampler

def streamSampler(stream, percent = .01, sample_col = 0):
    # Implements the standard streaming sampling method:
    #   stream -- iosteam object (i.e. an open file for reading)
    #   percent -- percent of sample to keep
    #   sample_col -- column number to sample over
    #
    # Rules:
    #   1) No saving rows, or user_ids outside the scope of the while loop.
    #   2) No other loops besides the while listed. 
    
    mean, standard_deviation = 0.0, 0.0
    # i=0
    bucket_tokeep=1
    buckets=int(1/percent)
    prev_mean=0
    new_mean=0
    old_variance=0
    new_variance=0
    std=0.0
    count=0


    for line in stream:
        ##<<COMPLETE>>
        user_id=line.split(',')[sample_col]
        bucketfor_record=hash(user_id,buckets)
        if(bucketfor_record==bucket_tokeep): #
            user_transaction_val=float(line.split(',')[3])
            count+=1
            new_mean=prev_mean+((user_transaction_val - prev_mean)/count)
            new_variance=old_variance+((user_transaction_val-prev_mean)*(user_transaction_val-new_mean))
            std=np.sqrt((new_variance)/(count))
            prev_mean=new_mean
            old_variance=new_variance

    mean=new_mean
    standard_deviation=std
    ##<<COMPLETE>>

    return mean, standard_deviation

#hash function implementation
def hash(user_id,buckets):
    return mmh3.hash(user_id)%buckets



##########################################################################
##########################################################################
# Task 1.C Timing

files=['transactions_small.csv', 'transactions_medium.csv', 'transactions_large.csv']
# files=['transactions_small.csv','transactions_medium.csv']
percents=[.02, .005]

if __name__ == "__main__": 

    ##<<COMPLETE: EDIT AND ADD TO IT>>
    for perc in percents:
        print("\nPercentage: %.4f\n==================" % perc)
        for f in files:
            print("\nFile: ", f)
            fstream = open(f, "r")
            startTime=datetime.now()
            print("  Typical Sampler: ", typicalSampler(fstream, perc, 2))
            # print('Time elasped: for file: ',f,"->", datetime.now() - startTime,'\n')
            print('\nTime elasped: for file: ',f,"->", ((datetime.now() - startTime).total_seconds()*1000 ),'\n')
            fstream.close()
            fstream = open(f, "r")
            startTime=datetime.now()
            print("  Stream Sampler:  ", streamSampler(fstream, perc, 2))            
            # print('\nTime elasped: for file: ',f,"->", datetime.now() - startTime,'\n')
            print('\nTime elasped: for file: ',f,"->", ((datetime.now() - startTime).total_seconds()*1000 ),'\n')

          


