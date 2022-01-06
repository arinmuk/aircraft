//console.log(data1);
var ayr20222 = {}
var ayr20212 = {}
var ayr20202 = {}
var ayr20192 = {}
var ayr20182 = {}
var ayr20172 = {}
var ayr20162 = {}
var ayr20152 = {}
var ayr20142 = {}
var ayr20132 = {}
var ayr20122 = {}
var ayr20112 = {}
var data=[]
var data1=[]


function ayear20222(sale) {
        return sale.year == '2022';
      }
function ayear20212(sale) {
        return sale.year == '2021';
      }

function ayear20202(sale) {
        return sale.year == '2020';
      }

function ayear20192(sale) {
        return sale.year == '2019';
      }
function ayear20182(sale) {
        return sale.year == '2018';
      }
function ayear20172(sale) {
        return sale.year == '2017';
      }
function ayear20162(sale) {
        return sale.year == '2016';
      }
function ayear20152(sale) {
        return sale.year == '2015';
      }
function ayear20142(sale) {
        return sale.year == '2014';
      }
function ayear20132(sale) {
        return sale.year == '2013';
      }

function ayear20122(sale) {
        return sale.year == '2012';
      }
function ayear20112(sale) {
        return sale.year == '2011';
      }





var loc =0
//////
function ainittwo(){

        //console.log(yr2019)
        var trace={x:ayr20222.map(row=>row.month),
                y:ayr20222.map(row=>row.NetRecd),
                name:"2022",
                mode: 'markers',
                 type:"scatter",
                 marker: { size: ayr20222.map(row=>row.NetRecd/100) }}  
        var trace1={x:ayr20212.map(row=>row.month),
                y:ayr20212.map(row=>row.NetRecd),
                name:"2021",
                mode: 'markers',
                 type:"scatter",
                 marker: { size: ayr20212.map(row=>row.NetRecd/100) }}

        var trace2={x:ayr20202.map(row=>row.month),
                y:ayr20202.map(row=>row.NetRecd),
                name:"2020",
                mode: 'markers',
                 type:"scatter",
                 marker: { size: ayr20202.map(row=>row.NetRecd/100) }
      }
        var trace3={x:ayr20192.map(row=>row.month),
                   y:ayr20192.map(row=>row.NetRecd),
                   name:"2019",
                   mode: 'markers',
                    type:"scatter",
                    marker: { size: ayr20192.map(row=>row.NetRecd/100) }
         }
        var trace4={x:ayr20182.map(row=>row.month),
                y:ayr20182.map(row=>row.NetRecd),
                name:"2018",
                mode: 'markers',
                 type:"scatter",
                 marker: { size: ayr20182.map(row=>row.NetRecd/100) }
        }
        
         var trace5={x:ayr20172.map(row=>row.month),
                y:ayr20172.map(row=>row.NetRecd),
                name:"2017",
                mode: 'markers',
                 type:"scatter",
                 marker: { size: ayr20172.map(row=>row.NetRecd/100) }
                
         }
         var trace6={x:ayr20162.map(row=>row.month),
                y:ayr20162.map(row=>row.NetRecd),
                name:"2016",
                mode: 'markers',
                 type:"scatter",
                 marker: { size: ayr20162.map(row=>row.NetRecd/100) }
        }

         var trace7={x: ayr20152.map(row=>row.month),
                     y: ayr20152.map(row=>row.NetRecd),
                        name:"2015",
                        mode: 'markers',
                         type:"scatter",
                         marker: { size: ayr20152.map(row=>row.NetRecd/100) }
        }
        var trace8={x: ayr20142.map(row=>row.month),
                y: ayr20142.map(row=>row.NetRecd),
                   name:"2014",
                   mode: 'markers',
                    type:"scatter",
                    marker: { size: ayr20142.map(row=>row.NetRecd/100) }
   }
        var trace9={x: ayr20132.map(row=>row.month),
                y: ayr20132.map(row=>row.NetRecd),
                name:"2013",
                mode: 'markers',
                type:"scatter",
                marker: { size: ayr20132.map(row=>row.NetRecd/100) }
}    
        var trace10={x: ayr20122.map(row=>row.month),
        y: ayr20122.map(row=>row.NetRecd),
        name:"2012",
        mode: 'markers',
        type:"scatter",
        marker: { size: ayr20122.map(row=>row.NetRecd/100) }
} 
        var trace11={x: ayr20112.map(row=>row.month),
        y: ayr20112.map(row=>row.NetRecd),
        name:"2011",
        mode: 'markers',
        type:"scatter",
        marker: { size: ayr20112.map(row=>row.NetRecd/100) }
}            
        var data=[trace,trace1,trace2,trace3,trace4,trace5,trace6,trace7,trace8,trace9,trace10,trace11]

         var layout ={
                title:"Monthly Sales Net Received",
                xaxis:{title:"Months"},
                yaxis:{title:"Amount in $"},
                autosize:false,
                width:1200,
                height:500,
                margin: {
                        l: 50,
                        r: 50,
                        b: 55,
                        t: 45,
                        pad: 2
                      }
        
         }
        
         Plotly.newPlot("plot3",data,layout)

}

//////
function getData3(dataset){
 x=[]
 y=[]
 console.log(dataset)
 switch (dataset){
        case "All":
         init()
         break
         case "2022":
                x=ayr20222.map(row=>row.month),
                y=ayr20222.map(row=>row.NetRecd)
                loc=0
                break

         case "2021":
                x=ayr20212.map(row=>row.month),
                y=ayr20212.map(row=>row.NetRecd)
                loc=1
                break
         case "2020":
         x=ayr20202.map(row=>row.month),
         y=ayr20202.map(row=>row.NetRecd)
         loc=2
         break;
         case "2019":
         x=ayr20192.map(row=>row.month),
         y=ayr20192.map(row=>row.NetRecd)
         loc=3
         break;
        case "2018":
         x=ayr20182.map(row=>row.month),
         y=ayr20182.map(row=>row.NetRecd)
         loc=4
                        break;
        case "2017":
          x=ayr20172.map(row=>row.month),
          y=ayr20172.map(row=>row.NetRecd)
          loc=5
                        break;
        case "2016":
           x=ayr20162.map(row=>row.month),
           y=ayr20162.map(row=>row.NetRecd)
           loc=6
                         break; 
        case "2015":
         x=ayr20152.map(row=>row.month),
         y=ayr20152.map(row=>row.NetRecd)
         loc=7
                break;                                                                                                                     
        case "2014":
          x=ayr20142.map(row=>row.month),
          y=ayr20142.map(row=>row.NetRecd)
         loc=8
        break;
        case "2013":
         x=ayr20132.map(row=>row.month),
         y=ayr20132.map(row=>row.NetRecd)
         loc=9
         break;              
         case "2012":
         x=ayr20122.map(row=>row.month),
         y=ayr20122.map(row=>row.NetRecd)
         loc=10
         break; 
         case "2011":
                        x=ayr20112.map(row=>row.month),
                        y=ayr20112.map(row=>row.NetRecd)
                        loc=11
                        break;                

        default:
        ainittwo()
          break;
 }
          //console.log("xlength",x)
 if (x.length==0){
        ainittwo()
 }
 else{
         aupdatePlotlytwo(x,y,loc)
 }
}

function aupdatePlotlytwo(newx,newy,loc){
        ainittwo()
        arrtrc=[]
        origarrtrc=[0,1,2,3,4,5,6,7,8,9,10,11]
        for(i=0;i<origarrtrc.length;i++){
                if(origarrtrc[i] != loc){arrtrc.push(origarrtrc[i])}
        }

        console.log(arrtrc)
        var LINE = document.getElementById("plot3")
        Plotly.deleteTraces(LINE,arrtrc)
        Plotly.restyle(LINE,"x",[newx])
        Plotly.restyle(LINE,"y",[newy])
}
// YOUR CODE HERE
urlstring='http://127.0.0.1:5000/readSales'

d3.json(urlstring).then(function(sample_m) {
        data1=sample_m
        data=data1
ayr20222 = data1.filter(ayear20212)
ayr20212 = data1.filter(ayear20212)
ayr20202 = data1.filter(ayear20202)
ayr20192 = data1.filter(ayear20192)
ayr20182 = data1.filter(ayear20182)
ayr20172 = data1.filter(ayear20172)
ayr20162 = data1.filter(ayear20162)
ayr20152 = data1.filter(ayear20152)
ayr20142 = data1.filter(ayear20142)
ayr20132 = data1.filter(ayear20132)
ayr20122 = data1.filter(ayear20122)
ayr20112 = data1.filter(ayear20112)
ainittwo()
})