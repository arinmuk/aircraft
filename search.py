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
connection = pymssql.connect(host=servername,user=sqluser, password=sqlpass,database='Aircraft')

# read cloud Mongo Data and return dataframes


def SearchAirline_cloudM_R(airlinename):
    db=cloudMClnt['Aircraft']
    colmodels=db['models']
    airline=airlinename
    
    
    modelsdf = pd.DataFrame(list(colmodels.find({'AIRLINE':airline}).sort([('ID', 1)])))
    
    return modelsdf
def SearchRegistration_cloudM_R(registration):
    db=cloudMClnt['Aircraft']
    colmodels=db['models']
    reg=registration
    
    
    modelsdf = pd.DataFrame(list(colmodels.find({'REGISTRATION':reg}).sort([('ID', 1)])))
    
    return modelsdf

def DistinctAirline_cloudM_R():
    db=cloudMClnt['Aircraft']
    colmodels=db['models']
    
    
    distinctmodelsdf = pd.DataFrame(list(colmodels.distinct('AIRLINE')))
    
    return distinctmodelsdf

def DistinctRegistration_cloudM_R():
    db=cloudMClnt['Aircraft']
    colmodels=db['models']
    
    
    distinctmodelsdf = pd.DataFrame(list(colmodels.distinct('REGISTRATION')))
    
    return distinctmodelsdf

def dataanimation():
    db=cloudMClnt['Aircraft']
    colmodelscloud=db['models']
    modelscoldf = pd.DataFrame(list(colmodelscloud.find().sort([('ID', 1)])))
    modelscoldf["month"]=modelscoldf["DATEOFORDER"].dt.month
    modelscoldf["year"]=modelscoldf["DATEOFORDER"].dt.year
    modelscoldf= modelscoldf.drop(['_id','MODEL_NO','DIMAID','WID','AIRCRAFT_TYPE','REGISTRATION','DESCRIPTION','SIZE','PRICE','SHIPPING','TAX','COMPANY','ORDEREDFROM','DATEOFORDER','PictureID','HangarClub'],axis=1)
    modelscolgrpdf=modelscoldf.groupby(['year','AIRLINE'],as_index=False).count().rename(columns={'ID':'ModelCount'})
    return modelscolgrpdf

