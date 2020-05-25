import numpy as np
import json
import re
import sys


from pyspark import SparkContext
sc = SparkContext()


########################Second question##################################3


def reorder(data):
  #data:(productid,[(user,rating),()])
  lst=[]
  for user,rating in data[1]:
    lst.append((user,(data[0],rating))) #(user,product,rating)
  return lst

def meanCenterR(data): #mean center ratings
#data:[(productid,[(user,latest_rating),()]),()]
  # data_dict=dict(data[1]) 
  # lstofvals=list(data_dict.values()) #actual_ratings
  # lstofkeys=list(data_dict.keys()) #username4
  # print("started meanCenterR")
  lstofvals,lstofkeys=[],[]
  for user,act_rating in data[1]:
    lstofkeys.append(user)
    lstofvals.append(act_rating)
  meancentered_ratings=(np.subtract(lstofvals,np.mean(lstofvals)))
  return ( data[0],list(zip(lstofkeys,meancentered_ratings,lstofvals ) ) )

def buildUtility(data,reviewerid_brd):#will work across nodes
  #[(productid,[(user,mean_centeredrating,actual rating),()]),()]
  productid=data[0]
  lst=[]
  local_dict=dict()
  actual_rating_lst=[]
  for user,rating,actual_rating in data[1]:
    local_dict[user]=(rating,actual_rating)
  for util_user in reviewerid_brd.value:
    if util_user in local_dict:
      # if user in reviewerid_brd.value:
        # lst.append((util_user,local_dict[util_user]))
        lst.append((local_dict[util_user][0]))
        actual_rating_lst.append(local_dict[util_user][1])
    else:
        # lst.append((util_user,0))
        lst.append((0))
        actual_rating_lst.append(0)
  return (productid,lst,actual_rating_lst)

def filter_intersection(data,input_item_br):
  #data: (product,[meancentered_ratings,...141 users],[actual_rating all 141 users])
  ratings_item=np.array(input_item_br.value[0][2]) #actual rating for the input product
  ratings_target=np.array(data[2])   #actual rating for each product coming in 
  if (( (ratings_target >0) & (ratings_item >0) ).sum()) >=2:
    return True

def correlation(data,targetItem):
  #data: (product,[meancentered_ratings,...142 users],[actual_rating all 142 users])
  #targetItems :list of items to query for from matrix
  #take each item and find correlation with every other record present on the current data node
  item1=data[1] #mean centered ratings for 142 users -columns
  item2=targetItem.value[0][1]  #mean centered ratings for 142 users-columns from the to be queried item
  numerator=np.dot(item1,item2)
  denominator=np.multiply(np.sqrt(np.sum(np.square(item1))),np.sqrt(np.sum(np.square(item2))))
  return (targetItem.value[0][0],data[0],(numerator/denominator))

#filter util matrix using similar_neigborhoods found in above step:input is input item variable,neighbors similarity variable
def filterUtilOnNeighbors(data,input_br,similar_neigborsBrd):
  #data: (product,[meancentered_ratings,...141 users],[actual_rating all 141 users])
  current_item=input_br.value[0][0]
  if (current_item,data[0]) in similar_neigborsBrd.value:
    return True

def reorderFilter3(data,similar_neigborsBrd):
  #data:(user,[(product,rating),()])
  neighborlst,lst_ret=[],[]
  user=data[0]
  for item,neighboritem in similar_neigborsBrd.value:
    neighborlst.append(neighboritem)
  for item,rating in data[1]:
    if item in neighborlst:
      #if user has rated items in neigbors
      lst_ret.append((user,(item,rating))) #(user,product,rating)
    else:
      #if user hasn't rated any item in neigbors
      lst_ret.append((user,('noitem','norating')))
  return lst_ret

def countItemsRatedByuser(data):
  #data : (user,[(item,rating)])
  lst=[]
  count=0
  for item,rating in data[1]:
    if rating!='norating' and rating>0:
      count=count+1
  if count>=2:
    lst.append(True) #is this user (column ) to be considered for prediction (column)
  else:
    lst.append(False)
  return lst

def reformat(data,reviewerid_brd):
  #data: (product,[meancentered_ratings,...141 users],[actual_rating all 141 users])
  itemname=data[0]
  rating_lst=data[2] #actual  rating
  user_lst=reviewerid_brd.value #list of all 141 users
  lst=[]
  for index in range(0,len(rating_lst)):
    lst.append((user_lst[index],(itemname,rating_lst[index]))) #(user,(item,actual rating))
  return lst

def predict(data,similaritydata,target_item,users_named,user_index):
  current_user=users_named.value[user_index] #can be taken from data[0] also will be same
  #data(user,[(item,actual rating)])
  target_itemname=target_item.value[0][0] #target product name 
  rating_lst=target_item.value[0][2] #actual ratings for target  
  item_rating=dict(data[1]) #dict of item:rating for the current user
  # denominator=sum(dict(similaritydata.value).values())
  denominator=0
  numerator=0
  for (current_item,neigbor_item) in similaritydata.value:
    if item_rating[neigbor_item]!=0:
      denominator+=similaritydata.value[(current_item,neigbor_item)]
    if current_item==target_itemname:
      simval=similaritydata.value[(current_item,neigbor_item)]
      rating_x_j= item_rating[neigbor_item]#rating for neigbor_item from data for current_user 
      numerator+=simval*rating_x_j
  rating_x_i=numerator/(denominator)  
  return (current_user,rating_x_i)

def main():
  path=sys.argv[1]
  items = re.findall(r"(?<![\"=\w])(?:[^\W_]+)(?![\"=\w])", sys.argv[2])
  ipfile=sc.textFile(path)
  recdata=ipfile.map(json.loads)
  inputlst=items
  # inputlst=['B00EZPXYP4'] #input lists
  op_lst=[]
  filter2=recdata.map(lambda x:((x['reviewerID'],x['asin']),(x['overall'],x['unixReviewTime'])))\
                .combineByKey(lambda v:[v],lambda c,v:c+[v],lambda c1,c2:c1+c2)\
                .map(lambda x:(x[0][0],x[0][1],x[1][0][0]))\
                .map(lambda x:(x[1],(x[0],x[2])))\
                .combineByKey(lambda v:[v],lambda c,v:c+[v],lambda c1,c2:c1+c2)\
                .filter(lambda x:len(x[1])>24)
  
  filter2.persist()
  filter3=filter2.flatMap(reorder)\
                .combineByKey(lambda v:[v],lambda c,v:c+[v],lambda c1,c2:c1+c2)\
                .filter(lambda x:len(x[1])>4)
  
  filter3.persist()
  user_ids=filter3.map(lambda x:x[0]).collect()
  reviewerid_brd=sc.broadcast(user_ids) #small set of user ids
  utilMatrixRecords=filter2.map(meanCenterR)\
                        .map(lambda x:buildUtility(x,reviewerid_brd))
  utilMatrixRecords.persist()

  # ###iteration query for each product
  for item in inputlst:

    # queryrecords=utilMatrixRecords.filter(lambda x:x[0] in inputlst).take(len(inputlst))#
    queryrecords=utilMatrixRecords.filter(lambda x:x[0] ==item).take(1)#
    input_item_br=sc.broadcast(queryrecords)#

    filtered_utilMatrix=utilMatrixRecords.filter(lambda x:filter_intersection(x,input_item_br)) #will have self also in the final list#
    filtered_utilMatrix.persist() #persist it#
    

    neighbors=filtered_utilMatrix.map(lambda x:correlation(x,input_item_br)) #input product,targetproduct,similarity#

    similar_neigbors=neighbors.filter(lambda x:x[2]>0).sortBy(lambda x:-x[2]).map(lambda x:((x[0],x[1]),x[2])).take(50)#
    similar_neigborsBrd=sc.broadcast(dict(similar_neigbors))#

    #data: (product,[meancentered_ratings,...141 users],[actual_rating all 141 users])
    utilMatrixRows=filtered_utilMatrix.filter(lambda x:filterUtilOnNeighbors(x,input_item_br,similar_neigborsBrd)) #

    # #(user,[(all items here will be in util matrix but not all items from util will be here,rating)]
    groupedByUsers=filter3.flatMap(lambda x:reorderFilter3(x,similar_neigborsBrd))\
                          .combineByKey(lambda v:[v],lambda c,v:c+[v],lambda c1,c2:c1+c2)#***
    groupedByUsers.persist()#****

    indexesRated_flag=groupedByUsers.flatMap(countItemsRatedByuser).collect() #indexes(user columns) where rating is done #***
    indexesRated_brd=sc.broadcast(indexesRated_flag) #check if broadcast is really required ,isn't used anywhere else except below cell #***
    # np.array(indexesRated_flag) #columns where prediction is to be done
    user_flags=indexesRated_brd.value #**
    users_topredict=np.where(user_flags)[0] #users/columns where target item ating needs to be predicted if missing #***
    users_toPredict_dict=dict()
    for index in users_topredict:
      users_toPredict_dict[index]=0

    reformattedutilMatrix=utilMatrixRows.flatMap(lambda x:reformat(x,reviewerid_brd)).combineByKey(lambda v:[v],lambda c,v:c+[v],lambda c1,c2:c1+c2) #
    reformattedutilMatrix.persist() #


    # users_toPredict #lis with indexes where to predict
    target_itemname=input_item_br.value[0][0] #target preidtcion row
    rating_lst=input_item_br.value[0][2] #actual rating target preidiction row
    reviewerid_brd.value #list of all 142 users
    lst=[] #list of users for which predicted
    lst2=[] #list for columns already having ratings
    for user_index in range(0,len(rating_lst)):
      if input_item_br.value[0][2][user_index]==0 and user_index in users_toPredict_dict:
        values=reformattedutilMatrix.filter(lambda x:x[0]==reviewerid_brd.value[user_index]).map(lambda x:predict(x,similar_neigborsBrd,input_item_br,reviewerid_brd,user_index)).collect()
        lst.extend(values)
      # else:
      #   lst2.append((reviewerid_brd.value[user_index],input_item_br.value[0][2][user_index]))

    op_lst.append((item,lst+lst2))
  return op_lst

if __name__ == "__main__":
    op=main()
    for item in op:
      print(item)
    sc.stop()