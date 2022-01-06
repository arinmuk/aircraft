
urlstring='https://aircraft-apis.herokuapp.com/summarizecnt'
objdata={}
dataarr=[]
d3.json(urlstring).then(function(sample_m) {
    console.log(sample_m)
     var dataset = sample_m.map(objA => ({y:objA.Count , label :objA.AIRLINE} ))
    console.log(dataset)
    var objdata = sample_m.map(objA => ({x:objA.AIRLINE , value :objA.Count, category :objA.Group_Count} ))
    
         x=sample_m.map(row=>row.AIRLINE),
         y=sample_m.map(row=>row.Count)
        for( i=0;i<x.length;i++){
            arrbld=[]
            arrbld.push(x[i],y[i])
            dataarr.push(arrbld)
        }
        console.log(dataarr)


    var chart = new CanvasJS.Chart("plot", {
        animationEnabled: true,
        theme: "light2", // "light1", "light2", "dark1", "dark2"
        title:{
            text: "Top Airline Count"
        },
        axisY: {
            title: "Number "
        },
      
        data: [{        
            type: "column",  
            showInLegend: true, 
            legendMarkerColor: "grey",
            legendText: "top 30 Airlines",
            dataPoints: dataset     
                //{ y: sample_m.map(row=>row.Count), label: sample_m.map(row=>row.AIRLINE) },
                
            
        }]
    });
    chart.render();
    


    var chartdonut = new CanvasJS.Chart("plot2", {
        animationEnabled: true,
        title:{
            text: "Top Airline Count",
            horizontalAlign: "right"
        },
        data: [{
            type: "doughnut",
            startAngle: 60,
            //innerRadius: 60,
            indexLabelFontSize: 17,
            indexLabel: "{label} - #percent%",
            toolTipContent: "<b>{label}:</b> {y} (#percent%)",
            dataPoints: dataset
        }]
    });
    chartdonut.render();
    

    anychart.onDocumentReady(function() {
        var data3 =objdata
      
       // create a tag (word) cloud chart
        var chart3 = anychart.tagCloud(data3);
      
         // set a chart title
        chart3.title('30 of the top Airlines')
        // set an array of angles at which the words will be laid out
        chart3.angles([0])
        // enable a color range
        chart3.colorRange(true);
        // set the color range length
       chart3.colorRange().length('80%');
      
        // display the word cloud chart
        chart3.container("container");
        chart3.draw();
      });



      anychart.onDocumentReady(function () {
        // create column chart
        var chart4 = anychart.column3d();
  
        // turn on chart animation
        chart4.animation(true);
  
        // set chart title text settings
        chart4.title('Top 30 Airlines by count');
  
        // create area series with passed data
        chart4.column(dataarr);
  
        chart4
          .tooltip()
          .position('center-top')
          .anchor('center-bottom')
          .offsetX(0)
          .offsetY(5)
          .format('{%Value}');
  
        // set scale minimum
        chart4.yScale().minimum(0);
  
        // set yAxis labels formatter
        chart4.yAxis().labels().format('{%Value}{groupsSeparator: }');
  
        chart4.tooltip().positionMode('point');
        chart4.interactivity().hoverMode('by-x');
  
        chart4.xAxis().title('Airlines');
        chart4.yAxis().title('Count');
  
        // set container id for the chart
        chart4.container('plot3');
  
        // initiate chart drawing
        chart4.draw();

    })








    })















