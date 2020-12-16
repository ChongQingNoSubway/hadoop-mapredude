author: wenhui zhu


#Descriptions of Map and Reduce functions

##Map:
according to the joinColumn to category data, the key will be the value of join_column and the value will be the whole column data


##Reduce:
the <key , value> is passed as input to the reducer.
reducer will combines the tuples from both list corresponding to that value
when the value is equal ,and then the tmplist will be an intermediate value to store the result of comparison,
Finally Optimize the tmplist data and write it to the output.
          