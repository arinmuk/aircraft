import sys
import pandas as pd
import pyodbc as pyodbc
from sqlalchemy.dialects.mssql import pymssql
from sqlalchemy import create_engine, MetaData, Table, select
import sqlalchemy
import pymssql
import pymongo
import csv
import json
from config import cloudM,cloudMpassword,sqluser,sqlpass,servername
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
connection = pymssql.connect(host='zbook',user=sqluser, password=sqlpass,database='Aircraft')

def sqlread():
    conn = pymssql.connect(server=servername, user=sqluser, password=sqlpass, database='aircraft') 
    cursor = conn.cursor()
    qry='SELECT * from aircraft'
    salesmasterqry='select ID, MODEL_NO, DIMAID, WID, AIRLINE, AIRCRAFT_TYPE, REGISTRATION, DESCRIPTION, SIZE, PRICE, SHIPPING, TAX, COMPANY, ORDEREDFROM, DATEOFORDER,  HangarClub,  PictureID from aircraftsold'
    salesqry='select s.*,a.price,a.shipping,a.tax from solddetails s inner join aircraftsold a on s.aircraftid=a.id '
    cursor.execute('SELECT * from aircraft')
    cursor.execute(salesqry)
    row = cursor.fetchone()  
    #while row:  
            #print(row)
            #row = cursor.fetchone()  
    sqldf=pd.read_sql(qry,conn)
    solddf=pd.read_sql(salesqry,conn)
    modelsolddf=pd.read_sql(salesmasterqry,conn)
    
    
    solddf["profit_loss"]=solddf["NetRecd"]-(solddf["price"]+solddf["shipping"]+solddf["tax"])
    solddf["Sale_Date"]=pd.to_datetime(solddf["SaleDate"])
    solddf["month"]=solddf['SaleDate'].dt.month
    solddf["year"]=solddf['SaleDate'].dt.year
    
    #solddf.set_index("SaleDate",inplace=True)
    
    sqldf.columns
    calcdf = sqldf.drop(['DIMAID', 'WID','DESCRIPTION', 'PICTURE', 'Picture2',
       'Picture3', 'Rare', 'HangarClub', 'MarketValue', 'PictureID'],axis =1)
    exportdf=sqldf.drop(['PICTURE', 'Picture2',
       'Picture3', 'Rare', 'MarketValue'], axis =1)

    return exportdf,solddf,modelsolddf

def mongocloud(exportdf,slddf,mssolddf):
    db=cloudMClnt['Aircraft']
    colmodelscloud=db['models']
    colmodels2cloud=db['models2']
    colsale2cloud=db['solddetails']
    colmssoldcloud=db['modelsold']
    cursor = colmodelscloud.find() 
#for record in cursor: 
    colmodelscloud.drop()
    colsale2cloud.drop()
    colmodels2cloud.drop()
    colmssoldcloud.drop()
    #records = json.loads(exportdf.to_json(orient='records'))
    #records = json.loads(exportdf.T.to_json()).values()
    #db.models.insert_many(records)
    #db.insert_many(df.to_dict('records'))
    db.models.insert_many(exportdf.to_dict('records'))
    db.models2.insert_many(exportdf.to_dict('records'))
    db.solddetails.insert_many(slddf.to_dict('records'))
    db.modelsold.insert_many(mssolddf.to_dict('records'))

# read cloud Mongo Data and return dataframes
def cloudM_R():
    db=cloudMClnt['Aircraft']
    colmodels=db['models']
    colmodels2=db['models2']
    colmodels3=db['modelsold']
    colmodels4=db['solddetails']
    
    modelsdf = pd.DataFrame(list(colmodels.find().sort([('ID', 1)])))
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


#cloudmodelsdf,cloudsoldmodelsdf,cloudsolddetails = cloudM_R()
#del cloudmodelsdf['_id']
#del cloudsoldmodelsdf['_id']
#del cloudsolddetails['_id']

def sql_update(mongodata,mongosold,mongosales):
    #mongodata,mongosold,mongosales=cloudM_R()
    mongodata.head()
    mongodata.fillna('')
    mongodata['DIMAID'].fillna('',inplace=True)
    mongodata['REGISTRATION'].fillna('',inplace=True)
    mongodata['SHIPPING'].fillna(0,inplace=True)
    mongodata['PRICE'].fillna(0,inplace=True)
    mongodata['PictureID'].fillna('',inplace=True)
    mongosold.fillna('')
    mongosold['DIMAID'].fillna('',inplace=True)
    mongosold['REGISTRATION'].fillna('',inplace=True)
    mongosold['SHIPPING'].fillna(0,inplace=True)
    mongosold['PRICE'].fillna(0,inplace=True)
    mongosold['PictureID'].fillna('',inplace=True)
    connection = pymssql.connect(host='zbook',
                             user=sqluser,
                             password=sqlpass,
                             database='Aircraft')
    # Create a new record

    #sql = "INSERT INTO dbo.aircraft (AIRCRAFT_TYPE, AIRLINE, COMPANY, DATEOFORDER, DESCRIPTION, DIMAID,HangarClub,ID,MODEL_NO,ORDEREDFROM,PRICE,PictureID,REGISTRATION,SHIPPING,SIZE,TAX,WID) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    sql = "INSERT INTO dbo.aircraft (ID,AIRCRAFT_TYPE, AIRLINE, COMPANY, DATEOFORDER, DESCRIPTION,HangarClub,MODEL_NO,ORDEREDFROM,PRICE,PictureID,REGISTRATION,SHIPPING,DIMAID,SIZE,TAX,WID) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s)"
    cursor=connection.cursor()
    cursor.execute("delete from aircraft")
    # Execute the query
    for idx,data in mongodata.iterrows():
        cursor.execute(sql, (data["ID"],data["AIRCRAFT_TYPE"],data["AIRLINE"],data["COMPANY"],data["DATEOFORDER"],data["DESCRIPTION"],data["HangarClub"],data["MODEL_NO"],data["ORDEREDFROM"],data["PRICE"],data["PictureID"],data["REGISTRATION"],data["SHIPPING"],data["DIMAID"],data["SIZE"],data["TAX"],data["WID"]))

    # the connection is not autocommited by default. So we must commit to save our changes.
    connection.commit()

    sql = "INSERT INTO dbo.aircraftsold (ID,AIRCRAFT_TYPE, AIRLINE, COMPANY, DATEOFORDER, DESCRIPTION,HangarClub,MODEL_NO,ORDEREDFROM,PRICE,PictureID,REGISTRATION,SHIPPING,DIMAID,SIZE,TAX,WID) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s)"
    cursor=connection.cursor()
    cursor.execute("delete from aircraftsold")
    # Execute the query
    for idx,data in mongosold.iterrows():
        cursor.execute(sql, (data["ID"],data["AIRCRAFT_TYPE"],data["AIRLINE"],data["COMPANY"],data["DATEOFORDER"],data["DESCRIPTION"],data["HangarClub"],data["MODEL_NO"],data["ORDEREDFROM"],data["PRICE"],data["PictureID"],data["REGISTRATION"],data["SHIPPING"],data["DIMAID"],data["SIZE"],data["TAX"],data["WID"]))

    # the connection is not autocommited by default. So we must commit to save our changes.
    connection.commit()
    #

    #sql = " insert into dbo.SoldDetails(AircraftID,[Listing price],[Net Recd],SaleDate,ListingFee,EbayFee,PaypalFee,Shipping,Insurance,Buyer,NetRecd) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    sql = " insert into dbo.SoldDetails(ID,AircraftID,[Listing price],[Net Recd],SaleDate,ListingFee,EbayFee,PaypalFee,Shipping,Insurance,Buyer,NetRecd) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    print(sql)
    cursor=connection.cursor()
    cursor.execute("delete from SoldDetails")
    for idx,data in mongosales.iterrows():
        cursor.execute(sql, (data["ID"],data["AircraftID"],data["Listing price"],data["Net Recd"],data["SaleDate"],data["ListingFee"],data["EbayFee"],data["PaypalFee"],data["Shipping"],data["Insurance"],data["Buyer"],data["NetRecd"]))

    # the connection is not autocommited by default. So we must commit to save our changes.
    connection.commit()

