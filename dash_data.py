
from connections import cloudM_R,mongoR_I,elastic_update,sql_update,sqlread,mongocloud
import pandas as pd
from pymongo import MongoClient
from config import cloudM,cloudMpassword,sqluser,sqlpass,servername
cloudMClnt=MongoClient()
cloudclient=MongoClient("mongodb+srv://"+ cloudM + ":"
                       + cloudMpassword + "@cluster0-omshy.mongodb.net/test?retryWrites=true&w=majority")

db=cloudclient['Aircraft']
colmodelscloud=db['models']


df,df2,df3 = cloudM_R()

def collection_summary():
    
     aggpipeline = [{ "$group": {"_id":"$AIRLINE","total": { "$sum": "$PRICE" },"myCount": { "$sum": 1 }}}]
     cursor1=colmodelscloud.aggregate(aggpipeline)
     netcount_costdf = pd.DataFrame(cursor1)
     netcount_costdf=netcount_costdf.rename(columns={'_id':'Airline'})
     netcount_costdf=netcount_costdf.sort_values(['myCount'],ascending = False)
     aggpipeline =[{ "$group": {"_id": 
                               {"Airline":"$AIRLINE",
                                "Size":"$SIZE"},
                                "total": { "$sum": "$PRICE" },"myCount": { "$sum": 1 }}}]
     cursor2=colmodelscloud.aggregate(aggpipeline)
     netcount_spl_costdf = pd.DataFrame(cursor2)
     netcount_spl_costdf=netcount_spl_costdf.rename(columns={'_id':'Airline'})
     netcount_spl_costdf=netcount_spl_costdf.sort_values(['myCount'],ascending = False)
     testdf=netcount_spl_costdf.Airline.dropna().apply(pd.Series)

     netcount_spl_costdf['Airline1']=testdf['Airline']
     netcount_spl_costdf['Size']=testdf['Size']
     netcount_spl_costdf.drop('Airline',axis='columns', inplace=True)
     netcount_spl_costdf.rename(columns={'Airline1':'Airline'}, inplace = True)
     return netcount_costdf,netcount_spl_costdf