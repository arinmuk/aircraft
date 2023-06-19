function buildMetadata(sample) {

  // @TODO: Complete the following function that builds the metadata panel
  urlstring="/dash_pane3/"+sample
  //console.log(urlstring)
  // Use `d3.json` to fetch the metadata for a sample
    // Use d3 to select the panel with id of `#sample-metadata`
    d3.json(urlstring).then(function(sample_m) {
      var objsample=sample_m
      //console.log(objsample)
      
      var htmlclear = d3.select(".panel-body")
      var test = htmlclear.html()
      
      //console.log(test)
      if (test !=''){
        d3.select(".panel-body").html("")
      }
      //console.log(htmlclear)
      Object.entries(objsample).forEach(([key,value])=>{
         Object.entries(value).forEach(([a,b])=>{
        var row = htmlclear.append("h5")
        row.text(a+" : "+b)
        //console.log(a)
        //console.log(b)
        //console.log("+++++++")

      }
 
      )
   
    
    })
    // Use `.html("") to clear any existing metadata

    // Use `Object.entries` to add each key and value pair to the panel
    // Hint: Inside the loop, you will need to use d3 to append new
    // tags for each key-value in the metadata.

    // BONUS: Build the Gauge Chart
    // buildGauge(data.WFREQ);
})}

function buildCharts(sample) {

  //
// "total": 9861.16, 
//"ModelCount": 280, 
//"Airline": "BRITISH AIRWAYS", 
//"Size": "1:400"
//
//
  //
  var workarr=[]
  var samp_arr=[]
  var toptenarr=[]
  // @TODO: Use `d3.json` to fetch the sample data for the plots
  urlstring="/dash_pane5/"+sample
  var parasample=sample
  var Size=[]   //Size  otuid
  var Airline=[]  //Airline otulabels
  var ModelCount=[]  // ModelCount svalues
  var otuidbub=[]    //otuidbub  Size
  var total=[] //total  svaluesbub
  var otulabelsbub=[]
  var totalcost = 0
  //var scalecost={}
  //console.log(urlstring)
    d3.json(urlstring).then((sample_m) => {
      console.log(sample_m)
      sample_m.sort((a, b) => (a.total > b.total) ? 1 : -1)
      Size=sample_m.map(element =>element.Size)
      Airline=sample_m.map(element =>element.Airline)
      total=sample_m.map(element =>Math.round(element.total))
      ModelCount=sample_m.map(element =>element.ModelCount)
      if (parasample =="All") {
        for (var i in total) {
          totalcost += total[i];
        }}
        else {

          for (var i in total) {
            totalcost += total[i];
          }

        }
        let scalecost = {"scale":sample,"TotalCost":totalcost}
        console.log(total)
        console.log(totalcost)
        console.log(scalecost)
      ///otuidbub=sample_m.map(element =>element.Size)
      //otulabelsbub=sample_m.map(element =>element.Airline)
    
      //var length=sample_m.sample_values.length
      var length=sample_m.length
      for (var j=0;j<length;j++)  {
            workarr=[]
            for (element in sample_m){
              //console.log(sample_m[element].total)
            workarr.push(sample_m[element].total)

          }
          //console.log(workarr)
     
         

    }
    samp_arr.push(workarr)
    //console.log(samp_arr)
    samp_arr.sort((a,b)=> b-a)
      
    console.log(samp_arr)
    toptenarr=samp_arr.slice(0,10)
    console.log(toptenarr)
  ///for (var x=0;x<10;x++){  
   /// otuid.push(toptenarr[x][0])
   /// otulabels.push(toptenarr[x][1])
   /// svalues.push(toptenarr[x][2])
////}
 //console.log("otuid:",otuid)
 //console.log("otulabels:",otulabels)
 //console.log("svalues:",svalues)

      // @TODO: Build a Bubble Chart using the sample data
      console.log(Airline)
      console.log(total)
      var airtop10= (Airline.slice(-20))
      var airtop10cost = (total.slice(-20))
      var airtopModcnt=(ModelCount.slice(-20))
      console.log(airtop10)
      console.log(airtop10cost)
      var trace1 = {
        x: airtop10,
        y: airtop10cost, //svaluesbub.map(element=>Math.round(element)/100),
        text: airtopModcnt.map(element=>'Tot_Models:'+element),//['A<br>size: 40', 'B<br>size: 60', 'C<br>size: 80', 'D<br>size: 100']
        mode: 'markers',
        marker: { 
          color: airtop10cost,
          opacity: [1,0.8, 0.6],
          size: airtop10cost.map(element=>((element>1000) ? element/100 : element/10))
        }
      };
      
      var data1 = [trace1];
      
      var layout1 = {
        scattermode: 'group',
        title: 'Top 20 airlines and Value In collection',
        xaxis: {title: 'Airlines'},
        yaxis: {title: '$ Amount'},
        showlegend: true,
        height: 700,
        width: 1200
      };
      
      Plotly.newPlot('bubble', data1, layout1)


    // @TODO: Build a Pie Chart
    var data = [{
      values: airtopModcnt,
      labels: airtop10,
      hovertext:airtop10cost.map(element=>'Tot_Value:'+element),
      hoverinfo: "hovertext",
      type: "pie"
    }];
  
    var layout = {
      height: 500,
  width: 500
    };
  
    Plotly.newPlot("pie", data, layout)

  // @TODO: Build a Guage Chart may be working
  urlstring="/dash_pane5/"+sample
  d3.json(urlstring).then((sample_g) => {
    valueg=scalecost.TotalCost

    //8888888888888888888888888888888888888

    // Enter a scrubbing freq per week between 0 and 10
var level = valueg;
console.log(level)
// Trig to calc meter point
var data = [
	{
		domain: { x: [0, 1], y: [0, 1] },
		value: level,
		title: { text: "Amount in $" },
		type: "indicator",
		mode: "gauge+number"
	}
];

var layout = { width: 500, height: 500, margin: { t: 0, b: 0 } };

Plotly.newPlot('gauge', data, layout);


    //99999999999999999999999999999999999999






  })

    // HINT: You will need to use slice() to grab the top 10 sample_values,
    // otu_ids, and labels (10 each).
})}

function init() {
  // Grab a reference to the dropdown select element
  var selector = d3.select("#selDataset");

  // Use the list of sample names to populate the select options
  d3.json("/ScaleSize").then((sampleNames) => {
    //console.log(sampleNames)
    sizedata=sampleNames
    sizedata.forEach((sample) => {
      console.log(sample)

      selector
        .append("option")
        .text(sample)
        .property("value", sample);
    });

    // Use the first sample from the list to build the initial plots
    const firstSample = sizedata[0];
    console.log(firstSample)

    ;
    buildMetadata(firstSample);
    buildCharts(firstSample)
  });
}

function optionChanged(newSample) {
  // Fetch new data each time a new sample is selected

  //d3.event.preventDefault()
  //var dset = d3.select("#selDataset").node().value
  //console.log(dset)

  buildCharts(newSample);
  buildMetadata(newSample);

}

// Initialize the dashboard
init();
