from flask import Flask,render_template,jsonify,request
import json
from pymongo import MongoClient 
from connections import cloudM_R,mongoR_I,elastic_update,sql_update,sqlread,mongocloud
from search import DistinctAirline_cloudM_R,SearchAirline_cloudM_R,DistinctRegistration_cloudM_R,SearchRegistration_cloudM_R
from flask_cors import CORS, cross_origin
#client = MongoClient()
#client = MongoClient('localhost', 27017)
#db=client['Aircraft']
#colmodels=db['models']
#colmodels2=db['models2']
#cursor = colmodels.find()


app=Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
CORS(app, support_credentials=True)

def mongo_coll_read():
    cloudmodelsdf,cloudsoldmodelsdf,cloudsolddetails = cloudM_R()
    cloudmodelsdf.fillna('')
    cloudmodelsdf['DIMAID'].fillna('',inplace=True)
    cloudmodelsdf['REGISTRATION'].fillna('',inplace=True)
    cloudmodelsdf['SHIPPING'].fillna(0,inplace=True)
    cloudmodelsdf['PRICE'].fillna(0,inplace=True)
    cloudmodelsdf['PictureID'].fillna('',inplace=True)
    cloudmodelsdf.fillna('')
    cloudmodelsdf['DIMAID'].fillna('',inplace=True)
    cloudmodelsdf['REGISTRATION'].fillna('',inplace=True)
    cloudmodelsdf['SHIPPING'].fillna(0,inplace=True)
    cloudmodelsdf['PRICE'].fillna(0,inplace=True)
    cloudmodelsdf['PictureID'].fillna('',inplace=True)
     #print(cursor)
    return cloudmodelsdf,cloudsoldmodelsdf,cloudsolddetails#'Home21 -read'




@app.route("/")
def home():
    return render_template('home.html')

#/Load_Cloud_Data
@app.route("/Load_Cloud_Data")
@cross_origin(supports_credentials=True)
def loadcloud():
    sqldf,solddf,modelsolddf = sqlread()
    mongocloud(sqldf,solddf,modelsolddf)
    return render_template('/home.html')

@app.route("/readAircraft")
@cross_origin(supports_credentials=True)
def read():
    res,res1,res2=mongo_coll_read()
    del res['_id']
    del res1['_id']
    del res2['_id']
    res_fix = res[["ID", "MODEL_NO","DIMAID","WID","AIRLINE", "AIRCRAFT_TYPE","REGISTRATION",  "DESCRIPTION",  "SIZE", "PRICE",  "SHIPPING", "TAX",  "COMPANY", "DATEOFORDER",  "ORDEREDFROM", "PictureID",  "HangarClub"]]
    #res_fix=res_fix.sort_values("ID",inplace=True)
    return jsonify(res_fix.to_dict('records'))

@app.route("/readSales")
@cross_origin(supports_credentials=True)
def read_summarize():
    res,res1,res2=mongo_coll_read()
    del res['_id']
    del res1['_id']
    del res2['_id']
    res2= res2.drop(['ID','AircraftID','Buyer','SaleDate'],axis=1)
    solddf_grp1=res2.groupby(['year','month'],\
        as_index=False).agg({'Listing price':"sum",'Net Recd':"sum",
                            'ListingFee':"sum",
                            'EbayFee':"sum",
                            'PaypalFee':"sum",
                            'Shipping':"sum",
                            'Insurance':"sum",
                            'NetRecd':"sum",
                            'price':"sum",
                            'shipping':"sum",
                            'tax':"sum",
                            'profit_loss':"sum"},
                            )
    
    return jsonify(solddf_grp1.to_dict('records'))

@app.route("/about")
def about():
    return render_template ('about.html')

@app.route("/salesgraphs")
def salesgraphs():
    return render_template ('index1.html')

@app.route("/summarizecnt")
@cross_origin(supports_credentials=True)
def sum_model_cnt():
        aircraftdf,res1,res2=mongo_coll_read()
        
        
        air_grp = aircraftdf.groupby(['AIRLINE']).ID.count()
        airgrp=aircraftdf.groupby(['AIRLINE'],as_index=False).agg({"ID":"count"}).rename(columns={'ID':'Count'})
        airgrp=airgrp.sort_values(['Count'],ascending=False)
        top10airgrp=airgrp.head(30)
        return jsonify(top10airgrp.to_dict('records'))
    


@app.route("/airlineDash")
def dashgraphs():

    
    
    
    return render_template ('airlinedashboard.html')





@app.route("/MsearchModels")
def searchModels():
    return render_template('/MsearchModels.html')
@app.route("/EsearchModels")
def EsearchModels():
    return render_template('/EsearchModels.html')

@app.route("/Refresh_Data")
def DataRefresh():
    exportdf,msdf,soldf=mongo_coll_read()
    del exportdf['_id']
    del msdf['_id']
    del soldf['_id']
    mongoR_I(exportdf,msdf,soldf)
    elastic_update(exportdf,msdf,soldf)
    sql_update(exportdf,msdf,soldf)
    return render_template('/home.html')



#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++search code
def readdata():
    alldatadf,allsolddetdf,allsoldtrans=cloudM_R()
    return alldatadf


@app.route("/send", methods=["GET", "POST"])
def sendairline():
    
    print(request.method)
    
    if request.method == "POST":
          nickname = request.form["airline"]
          print("apple")
          print(nickname) 
          airecords_df=SearchAirline_cloudM_R(nickname)
      #    res_fix = airecords_df[["ID", "MODEL_NO","DIMAID","WID","AIRLINE", "AIRCRAFT_TYPE","REGISTRATION",  "DESCRIPTION",  "SIZE", "PRICE",  "SHIPPING", "TAX",  "COMPANY", "DATEOFORDER",  "ORDEREDFROM", "PictureID",  "HangarClub"]]
      #  return jsonify(res_fix.to_dict('records'))
          columnslst=airecords_df.columns
          print(columnslst)
          if columnslst[0]=="_id":
                    del airecords_df['_id']
          
          filterdata_dict=airecords_df.to_dict('records')
          UAirdf=DistinctAirline_cloudM_R()
          UAirdf.rename(columns={0:"Airline"},inplace=True)
          data_dict = UAirdf.to_dict('records')
          print(data_dict)
          labelval=nickname
    return render_template("formsearch.html",data = data_dict,alldata=filterdata_dict,airlinename=labelval)

@app.route("/uniqueAirlines")
def retrieveairline():
    
    UAirdf=DistinctAirline_cloudM_R()
    UAirdf.rename(columns={0:"Airline"},inplace=True)
    tempdata=jsonify(UAirdf.to_dict('records'))
    alldatadf=readdata()
    del alldatadf['_id']
    #distinctAirlinedf.head()
    #data_dict=distinctAirlinedf.to_dict('records')
    data_dict = UAirdf.to_dict('records')
    alldata_dict=alldatadf.to_dict('records')
    return render_template("formsearch.html", data = data_dict, alldata=alldata_dict)




#++++++++++++++++++++++++++

@app.route("/sendReg", methods=["GET", "POST"])
def sendreg():
    
    print(request.method)
    
    if request.method == "POST":
          nickname = request.form["registration"]
          print("apple")
          print(nickname) 
          airecords_df=SearchRegistration_cloudM_R(nickname)
      #    res_fix = airecords_df[["ID", "MODEL_NO","DIMAID","WID","AIRLINE", "AIRCRAFT_TYPE","REGISTRATION",  "DESCRIPTION",  "SIZE", "PRICE",  "SHIPPING", "TAX",  "COMPANY", "DATEOFORDER",  "ORDEREDFROM", "PictureID",  "HangarClub"]]
      #  return jsonify(res_fix.to_dict('records'))
          columnslst=airecords_df.columns
          print(columnslst)
          if columnslst[0]=="_id":
                    del airecords_df['_id']
          
          filterdata_dict=airecords_df.to_dict('records')
          UAirdf=DistinctRegistration_cloudM_R()
          UAirdf.rename(columns={0:"Registration"},inplace=True)
          data_dict = UAirdf.to_dict('records')
          print(data_dict)
          labelval=nickname
    return render_template("frmsearchreg.html",data = data_dict,alldata=filterdata_dict,airlinename=labelval)

@app.route("/uniqueReg")
def retrieve_reg():
    
    UAirdf=DistinctRegistration_cloudM_R()
    UAirdf.rename(columns={0:"Registration"},inplace=True)
    tempdata=jsonify(UAirdf.to_dict('records'))
    alldatadf=readdata()
    del alldatadf['_id']
    #distinctAirlinedf.head()
    #data_dict=distinctAirlinedf.to_dict('records')
    data_dict = UAirdf.to_dict('records')
    alldata_dict=alldatadf.to_dict('records')
    return render_template("frmsearchreg.html", data = data_dict, alldata=alldata_dict)








#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

if __name__=='__main__':
    app.run(debug=True)