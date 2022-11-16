#!/usr/bin/env python
# coding: utf-8

# In[1]:


#first install the package
get_ipython().system('pip install dnspython')
#m  shows the information
get_ipython().system('-m pip install pymongo[srv]')


# In[2]:


import pymongo
import json
from pymongo import MongoClient
import pandas as pd


# In[3]:


client =pymongo.MongoClient('mongodb://127.0.0.1:27017/')


# In[4]:


db=client.Studentsdetails


# In[5]:


records = db.students_collection


# In[6]:


f = open("C:/Users/DELL/Downloads/Studentsdetails.json")


# In[7]:


a = json.load(f)


# In[8]:


a = records


# In[9]:


p = records.count_documents({})
db = client.get_database('students')
records = db.students_collection


# In[10]:


#1) Find the student name who scored maximum scores in all (exam, quiz and homework)?


# In[11]:


agg = records.aggregate([
    {"$unwind":"$scores"},
    {"$group":
     {
         "_id":"$_id",
        "name":{"$first":"$name"}
      ,
     "Total":{"$sum":"$scores.score"},
      }
     },
      {"$sort":{"Total":-1}},
     {"$limit":1}
    
])
for i in agg:
      print(i)


# In[12]:


#2) Find students who scored below average in the exam and pass mark is 40%?


# In[13]:


agg1 = records.aggregate([
    {"$unwind":"$scores"},
    {"$match":{'scores.type':'exam',"scores.score":{"$gt":40,"$lt":60}}
    }
      
])
for i in agg1:
      print(i)


# In[14]:


#3) Find students who scored below pass mark and assigned them as fail, and above pass mark as pass in all the categories.


# In[15]:


x = records.aggregate(
[ {"$set": 
   {"scores": 
     {"$arrayToObject": 
       [{"$map": 
           {"input": "$scores",
            "as": "s",
            "in": {"k": "$$s.type", "v": "$$s.score"}}}]}}},
 {"$project":
  {
     "_id":1,
     "name":1,
     "result":{
            "$cond":
                    {"if": {"$and" : [{"$gte": ["$scores.exam", 40]}, {"$gte": ["$scores.quiz", 40]}, {"$gte": [ "$scores.homework", 40]}]
                            },
                    "then" :"pass",
                    "else":"fail"
                    }
               }
  }
}
  ])
for i in x:
    print(i)


# In[16]:


#4) Find the total and average of the exam, quiz and homework and store them in a separate collection.


# In[32]:


avgtotal = db.avgtotal_collection
avgtt = []


for i in records.aggregate([
    {"$unwind":"$scores"},
    {"$group":
     {
         "_id":"$_id",
        "name":{"$first":"$name"}
      ,
     "Total":{"$sum":"$scores.score"},
      "Average":{"$avg":"$scores.score"}
      }
     },
     {"$sort":{"_id":1}}
     
    ]):
    avgtt.append(i)
    avgtotal.insert_many(avgtt)
    print(i)


# In[18]:


#5) Create a new collection which consists of students who scored below average and above 40% in all the categories


# In[34]:


passavge = db.belowavg_collection

for i in records.aggregate(
[{"$match": 
   {"$expr": 
     {"$and": 
       [{"$gt": [{"$min": "$scores.score"}, 40]},
         {"$lt": [{"$max": "$scores.score"}, 70]}
        ]
      }
    }
  }]):
    
    passavg = []  
    passavg.append(i)
    print(i)
passavge.insert_many(passavg)


# In[ ]:





# In[20]:


#6) Create a new collection which consists of students who scored below the fail mark in all the categories.


# In[21]:


fail = db.allfail_collection
agg = records.aggregate(
[{"$match": 
   {"$expr": 
         {"$lt": [{"$max": "$scores.score"}, 40]
          }
        
      }
    }
  ])

failed = []

for i in agg:
    failed.append(i)
    print(i)

fail.insert_many(failed)


# In[22]:


#7) Create a new collection which consists of students who scored above pass mark in all the categories.


# In[23]:


pas = db.allpass_collection
agg = records.aggregate(
[{"$match": 
   {"$expr": 
         {"$gt": [{"$min": "$scores.score"}, 40]
          }
        
      }
    }
  ])

passed = []

for i in agg:
    passed.append(i)
    print(i)
pas.insert_many(passed)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




