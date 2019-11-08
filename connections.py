import sys
import pandas as pd
import pyodbc as pyodbc
from sqlalchemy.dialects.mssql import pymssql
import sqlalchemy
import pymssql
import pymongo
import csv
import json
from config import cloudM,cloudMpassword,sqluser,sqlpass
from pymongo import MongoClient
from flask import Flask, jsonify, render_template
from elasticsearch import Elasticsearch

#local mongo install
client = MongoClient()
client = MongoClient('localhost', 27017)

#cloud mongo connect
cloudMClnt=MongoClient()
cloudMClnt=MongoClient("mongodb+srv://"+ cloudM + ":"+ cloudMpassword + "@cluster0-omshy.mongodb.net/test?retryWrites=true&w=majority")


es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
es

from sqlalchemy import create_engine, MetaData, Table, select
connection = pymssql.connect(host='yoga900',user=sqluser, password=sqlpass,database='Aircraft')

# read cloud Mongo Data and return dataframes
def cloudM_R():
    db=cloudMClnt['Aircraft']
    colmodels=db['models']
    colmodels2=db['models2']
    colmodels3=db['modelsold']
    colmodels4=db['solddetails']
    
    modelsdf = pd.DataFrame(list(colmodels.find()))
    modelsolddf = pd.DataFrame(list(colmodels3.find()))
    solddetailsdf = pd.DataFrame(list(colmodels4.find()))
    #modelsdf = pd.DataFrame(list(colmodels.find()))
    return modelsdf,modelsolddf,solddetailsdf


#insert data into local mongo
def mongoR_I(exportdf,msdf,soldf):
    db=client['Aircraft']
    colmodels=db['models']
    colmodels2=db['models2']
    colmodelsales=db['modelsold']
    colsales=db['sales']
    cursor = colmodels.find() 
#for record in cursor: 
    colmodels.drop()
    
    colmodels2.drop()
    colmodelsales.drop()
    colsales.drop()
    #records = json.loads(exportdf.to_json(orient='records'))
    #records = json.loads(exportdf.T.to_json()).values()
    #db.models.insert_many(records)
    #db.insert_many(df.to_dict('records'))
    db.models.insert_many(exportdf.to_dict('records'))
    db.models2.insert_many(exportdf.to_dict('records'))
    db.modelsold.insert_many(msdf.to_dict('records'))
    db.sales.insert_many(soldf.to_dict('records'))



###Update local elastic data cache
def elastic_update(exportdf,msdf,solddf):
    es.indices.delete(index='aircraft')
    es.indices.delete(index='aircraft_sales')
    es.indices.delete(index='solddetails')
    es.indices.create(index='aircraft', ignore=400)
    es.indices.create(index='aircraft_sales', ignore=400)
    es.indices.create(index='solddetails', ignore=400)
    INDEX="aircraft"
    TYPE= "models"
    from bson import json_util

    def rec_to_actions(export1df):
        #export1df["DATEOFORDER"]=export1df['DATEOFORDER'].dt.strftime('%Y-%m-%d')
        import json
        tmp = export1df.to_json(orient = "records")
        print(tmp)
    # Load each record into json format before bulk
        df_json= json.loads(tmp)
        for record in df_json: #export1df.to_dict(orient="records"):
            #print(record)
            yield ('{ "index" : { "_index" : "%s", "_type" : "%s" }}'% (INDEX, TYPE))
            yield (json.dumps(record, default=str))

    #from elasticsearch import Elasticsearch
    #e = Elasticsearch() # no args, connect to localhost:9200
    if not es.indices.exists(INDEX):
        raise RuntimeError('index does not exists, use `curl -X PUT "localhost:9200/%s"` and try again'%INDEX)

    r = es.bulk(rec_to_actions(exportdf)) # return a dict
    INDEX="aircraft_sales"
    TYPE= "sold"
    r1= es.bulk(rec_to_actions(msdf)) # return a dict
    INDEX="solddetails"
    TYPE= "solddet"
    r1= es.bulk(rec_to_actions(solddf)) # return a dict
    print(not r["errors"])


cloudmodelsdf,cloudsoldmodelsdf,cloudsolddetails = cloudM_R()
del cloudmodelsdf['_id']
del cloudsoldmodelsdf['_id']
del cloudsolddetails['_id']
