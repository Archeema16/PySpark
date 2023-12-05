# Databricks notebook source
# MAGIC %python
# MAGIC x = [2,9,8,6,5]
# MAGIC x1 = [i*i for i in x]
# MAGIC x2 = [i**2 for i in x if i%2==0]
# MAGIC print(x1)
# MAGIC print(x2)

# COMMAND ----------

print(sum(x))
print(list(reversed(x2)))
a=2
b=3
def somefunc(a,b,myfunc):
    return myfunc(a,b)

print(somefunc(a,b,lambda x,y : x+y))
#Or do it short
add = lambda x,y:x+y
print(add(2,5))

# COMMAND ----------

subtract = lambda x : x-2
var_list = [2,5,7]
iterable = map(subtract,var_list)
new_list = list(iterable)
print(new_list)

#Now use reduce when you need to accumulate or reduce a list of values into a single result based on a specific operation
#Reduce function should have 2 parameters and return 1 result
from functools import reduce
print(reduce(lambda x,y:x+y,var_list)) #Single line of code and it does the operation of whole list into 1 value



# COMMAND ----------

my_Map = {"Alice":10,"Bob":21,13:"Shafqat"}
print(my_Map["Alice"])
print(my_Map[13])

var_list = [2,5,7]
print(sorted (var_list))
print(sorted (var_list,reverse=True))
print(sorted ([("Alice",4),("Charlie",2),("Bob",3)],key=lambda x: x[1]))
print(sorted ([("Alice",4),("Charlie",2),("Bob",3)],key=lambda x: x[1],reverse=True))

# COMMAND ----------

from itertools import groupby  # itertools.groupby requires the list to be sorted
{
    r: len(s) 
    for r, s in {
        p: list(v) 
        for p, v in groupby(
            sorted(
                list(map(
                    lambda x: (x, 1),
                    "sheena is a punk rocker she is a punk punk".split(" ")
                )),
                key=lambda x: x[0]
            ), 
            lambda x: x[0]
        )
    }.items()
}

# COMMAND ----------

from functools import reduce
{
    r: reduce(
        lambda x, y: x + y, 
        list(map(lambda x: x[1], s))
    )
    for r, s in {
        p: list(v) 
        for p, v in groupby(
            sorted(
                list(map(
                    lambda x: (x, 1), 
                    "sheena is a punk rocker she is a punk punk".split(" ")
                )),
                key=lambda x: x[0]
            ),
            lambda x: x[0]
        )
    }.items()
}

# COMMAND ----------

#Reading 
flightData = spark.read().option("inferSchema","true").option("header","true").csv("")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


