import os

import pandas as pd
import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy import func,inspect,table,column
from flask import Flask, jsonify, render_template
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///db/bellybutton.sqlite?check_same_thread=False")
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)
Base.classes.keys()
inspector = inspect(engine)
inspector.get_table_names()
#otu = Base.classes.otu
samples_t = Base.classes.samples
samples_m=Base.classes.sample_metadata
session = Session(engine)
conn = engine.connect()
session = Session(bind=conn)


@app.route("/")
def index():
    """Return the homepage."""
    return render_template("index.html")


@app.route("/names")
def names():
    """Return a list of sample names."""
    results = session.query(samples_m.sample).all()

    
    df=pd.DataFrame(results)

    lst=df["sample"].tolist()
    names_sample=[item for item in lst]

    # Use Pandas to perform the sql query
  

    # Return a list of the column names (sample names)
    return jsonify(names_sample)

@app.route("/metadata/<sample>")
def sample_metadata(sample):
    """Return the MetaData for a given sample."""
    print(sample)
    sel=[]
    srch_sample = sample
    #sel = [
     #   samples_m.SAMPLEID,
      #  samples_m.ETHNICITY,
       # samples_m.GENDER,
        #samples_m.AGE,
        #samples_m.LOCATION,
        #samples_m.BBTYPE,
        #samples_m.WFREQ
    #]

    #results = db.session.query(*sel).filter(samples_m.sample == srch_sample).all()
    results = session.query(samples_m.sample,samples_m.ETHNICITY,samples_m.GENDER, samples_m.AGE,samples_m.LOCATION,samples_m.BBTYPE,samples_m.WFREQ).\
        filter(samples_m.sample == srch_sample).all()
    # Create a dictionary entry for each row of metadata information
    samples_m1 = {}
    for result in results:
        samples_m1["sample"] = result[0]
        samples_m1["ETHNICITY"] = result[1]
        samples_m1["GENDER"] = result[2]
        samples_m1["AGE"] = result[3]
        samples_m1["LOCATION"] = result[4]
        samples_m1["BBTYPE"] = result[5]
        samples_m1["WFREQ"] = result[6]

    print(samples_m1)
    return jsonify(samples_m1)


@app.route("/samples/<sample>")
def samples(sample):
    """Return `otu_ids`, `otu_labels`,and `sample_values`."""
    print(sample)
    stmt = session.query(samples_t).statement
    df = pd.read_sql_query(stmt, session.bind)
    df.head()
    # Filter the data based on the sample number and
    # only keep rows with values above 1
    sample_data = df.loc[df[sample] > 1, ["otu_id", "otu_label", sample]]
    # Format the data to send as json
    data = {
       "otu_ids": sample_data.otu_id.values.tolist(),
       "sample_values": sample_data[sample].values.tolist(),
       "otu_labels": sample_data.otu_label.tolist(),
    }
    return  jsonify(data)
@app.route("/wfreq/<sample>")
def sample_wfreq(sample):
    """Return the MetaData for a given sample."""
    print(sample)
    sel=[]
    srch_sample = sample
    results = session.query(samples_m.WFREQ).\
        filter(samples_m.sample == srch_sample).all()
    # Create a dictionary entry for each row of metadata information
    samples_m2 = {}
    for result in results:
        samples_m2["WFREQ"] = result[0]
        

    print(samples_m2)
    return jsonify(samples_m2)

if __name__ == "__main__":
    app.run()