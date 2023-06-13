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
      
      console.log(test)
      if (test !=''){
        d3.select(".panel-body").html("")
      }
      //console.log(htmlclear)
      Object.entries(objsample).forEach(([key,value])=>{
        var row = htmlclear.append("h5")
        row.text(key+" : "+value)

      })
    // Use `.html("") to clear any existing metadata

    // Use `Object.entries` to add each key and value pair to the panel
    // Hint: Inside the loop, you will need to use d3 to append new
    // tags for each key-value in the metadata.

    // BONUS: Build the Gauge Chart
    // buildGauge(data.WFREQ);
})}

function buildCharts(sample) {
  var workarr=[]
  var samp_arr=[]
  var toptenarr=[]
  // @TODO: Use `d3.json` to fetch the sample data for the plots
  urlstring="/samples/"+sample
  var otuid=[]
  var otulabels=[]
  var svalues=[]
  var otuidbub=[]
  var svaluesbub=[]
  var otulabelsbub=[]

  //console.log(urlstring)
    d3.json(urlstring).then((sample_m) => {
      //console.log(sample_m)
      otuidbub=sample_m.otu_ids
      otulabelsbub=sample_m.otu_labels
      svaluesbub=sample_m.sample_values
      var length=sample_m.sample_values.length
      for (var j=0;j<length;j++)  {
            workarr=[]
            for (element in sample_m){
            workarr.push(sample_m[element][j])

          }
          //console.log(workarr)
          samp_arr.push(workarr)
         

    }
    //console.log(samp_arr)
    samp_arr.sort(function(a,b){
      return b[2]-a[2]
    })
   // console.log(samp_arr)
    toptenarr=samp_arr.slice(0,10)
    console.log(toptenarr)
  for (var x=0;x<10;x++){  
    otuid.push(toptenarr[x][0])
    otulabels.push(toptenarr[x][1])
    svalues.push(toptenarr[x][2])
}
 //console.log("otuid:",otuid)
 //console.log("otulabels:",otulabels)
 //console.log("svalues:",svalues)

      // @TODO: Build a Bubble Chart using the sample data

      var trace1 = {
        x: otuidbub,
        y: svaluesbub,
        mode: 'markers',
        marker: { 
          color: otuidbub,
          opacity: [1,0.8, 0.6],
          size: svaluesbub
        }
      };
      
      var data1 = [trace1];
      
      var layout1 = {
        title: '',
        showlegend: false,
        height: 700,
        width: 1200
      };
      
      Plotly.newPlot('bubble', data1, layout1)


    // @TODO: Build a Pie Chart
    var data = [{
      values: svalues,
      labels: otuid,
      hovertext:otulabels,
      hoverinfo: "hovertext",
      type: "pie"
    }];
  
    var layout = {
      height: 500,
  width: 500
    };
  
    Plotly.newPlot("pie", data, layout)

  // @TODO: Build a Guage Chart may be working
  urlstring="/wfreq/"+sample
  d3.json(urlstring).then((sample_g) => {
    valueg=sample_g.WFREQ

    //8888888888888888888888888888888888888

    // Enter a scrubbing freq per week between 0 and 10
var level = valueg;

// Trig to calc meter point
var degrees = 9- level,
     radius = .50;
var radians = degrees * Math.PI / 10
var x = radius * Math.cos(radians);
var y = radius * Math.sin(radians);

// Path: may have to change to create a better triangle
var mainPath = 'M -.0 -0.025 L .0 0.025 L ',
     pathX = String(x),
     space = ' ',
     pathY = String(y),
     pathEnd = ' Z';
var path = mainPath.concat(pathX,space,pathY,pathEnd);

var data = [{ type: 'scatter',
   x: [0], y:[0],
    marker: {size: 28, color:'850000'},
    showlegend: false,
    name: 'wash freq per week',
    text: level,
    hoverinfo: 'text+name'},
  { values: [50/9, 50/9, 50/9, 50/9, 50/9, 50/9, 50/9, 50/9, 50/9, 50],
  rotation: 90,
  text: ['8-9', '7-8', '6-7', '5-6', '4-5', '3-4', '2-3', '1-2', '0-1', ''],
  textinfo: 'text',
  textposition:'inside',
  marker: {colors:['rgba(14, 127, 0, .5)', 'rgba(110, 154, 22, .5)',
                         'rgba(155, 202, 42, .5)', 'rgba(202, 209, 95, .5)',
                         'rgba(210, 206, 145, .5)', 'rgba(232, 226, 202, .5)',
                         'rgba(140, 154, 22, 0)','rgba(10, 154, 22, .5)','rgba(80, 154, 22, .5)','rgba(255, 255, 255, 0)']},
  labels: ['8-9', '7-8', '6-7', '5-6', '4-5', '3-4', '2-3', '1-2', '0-1', ''],
  hoverinfo: 'label',
  hole: .5,
  type: 'pie',
  showlegend: false
}];

var layout = {
  shapes:[{
      type: 'path',
      path: path,
      fillcolor: '850000',
      line: {
        color: '850000'
      }
    }],
  title: 'Gauge Wash freq per Week',
  height: 550,
  width: 700,
  xaxis: {zeroline:false, showticklabels:false,
             showgrid: false, range: [-1, 1]},
  yaxis: {zeroline:false, showticklabels:false,
             showgrid: false, range: [-1, 1]}
};

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
  d3.json("http://127.0.0.1:5000/Size").then((sampleNames) => {
    console.log(sampleNames)
    sampleNames.forEach((sample) => {
      console.log(sample["Size"])

      selector
        .append("option")
        .text(sample["Size"])
        .property("value", sample["Size"]);
    });

    // Use the first sample from the list to build the initial plots
    /////const firstSample = sampleNames[0];
    ////console.log(firstSample)

    ////buildCharts(firstSample);
    ///buildMetadata(firstSample);
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
