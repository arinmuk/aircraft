
urlstring='http://127.0.0.1:5000/summarizecnt'
objdata={}
dataarr=[]
d3.json(urlstring).then(function(sample_m) {
    console.log(sample_m)
     var dataset = sample_m.map(objA => ({y:objA.Count , label :objA.AIRLINE} ))
    console.log(dataset)
    





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
    

    })
















