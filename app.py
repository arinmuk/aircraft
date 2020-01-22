from flask import Flask,render_template,jsonify
import json
from pymongo import MongoClient 
from connections import cloudM_R,mongoR_I,elastic_update,sql_update
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

@app.route("/MsearchModels")
def searchModels():
    return render_template('/MsearchModels.html')

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


if __name__=='__main__':
    app.run(debug=True)