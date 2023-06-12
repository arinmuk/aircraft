//console.log(data1);
var yr20232 = {}
var yr20222 = {}
var yr20212 = {}
var yr20202 = {}
var yr20192 = {}
var yr20182= {}
var yr20172 = {}
var yr20162 = {}
var yr20152 = {}
var yr20142 = {}
var yr20132 = {}
var yr20122 = {}
var yr20112 = {}
var data=[]
var data1=[]
function year20232(sale) {
        return sale.year == '2023';
      }
function year20222(sale) {
        return sale.year == '2022';
      }
function year20212(sale) {
        return sale.year == '2021';
      }
function year20202(sale) {
        return sale.year == '2020';
      }

function year20192(sale) {
        return sale.year == '2019';
      }
function year20182(sale) {
        return sale.year == '2018';
      }
function year20172(sale) {
        return sale.year == '2017';
      }
function year20162(sale) {
        return sale.year == '2016';
      }
function year20152(sale) {
        return sale.year == '2015';
      }
function year20142(sale) {
        return sale.year == '2014';
      }
function year20132(sale) {
        return sale.year == '2013';
      }

function year20122(sale) {
        return sale.year == '2012';
      }
function year20112(sale) {
        return sale.year == '2011';
      }





var loc =0
//////
function inittwo(){

        //console.log(yr2019)

        var trace12={x:yr20232.map(row=>row.month),
                y:yr20232.map(row=>row.NetRecd),
                name:"2023",
                 type:"scatter"
      }    

        var trace11={x:yr20222.map(row=>row.month),
                y:yr20222.map(row=>row.NetRecd),
                name:"2022",
                 type:"bar"
      }       
        var trace={x:yr20212.map(row=>row.month),
                y:yr20212.map(row=>row.NetRecd),
                name:"2021",
                 type:"bar"
      }
        var trace1={x:yr20202.map(row=>row.month),
                y:yr20202.map(row=>row.NetRecd),
                name:"2020",
                 type:"bar"
      }
        var trace2={x:yr20192.map(row=>row.month),
                   y:yr20192.map(row=>row.NetRecd),
                   name:"2019",
                    type:"bar"
         }
        var trace3={x:yr20182.map(row=>row.month),
                y:yr20182.map(row=>row.NetRecd),
                name:"2018",
                 type:"bar"
        }
        
         var trace4={x:yr20172.map(row=>row.month),
                y:yr20172.map(row=>row.NetRecd),
                name:"2017",
                 type:"bar"
                
         }
         var trace5={x:yr20162.map(row=>row.month),
                y:yr20162.map(row=>row.NetRecd),
                name:"2016",
                 type:"bar"
        }

         var trace6={x: yr20152.map(row=>row.month),
                     y: yr20152.map(row=>row.NetRecd),
                        name:"2015",
                         type:"bar"
        }
        var trace7={x: yr20142.map(row=>row.month),
                y: yr20142.map(row=>row.NetRecd),
                   name:"2014",
                    type:"bar"
   }
        var trace8={x: yr20132.map(row=>row.month),
                y: yr20132.map(row=>row.NetRecd),
                name:"2013",
                type:"bar"
}    
var trace9={x: yr20122.map(row=>row.month),
        y: yr20122.map(row=>row.NetRecd),
        name:"2012",
        type:"bar"
} 
var trace10={x: yr20112.map(row=>row.month),
        y: yr20112.map(row=>row.NetRecd),
        name:"2011",
        type:"scatter"
}            
        var data=[trace,trace1,trace2,trace3,trace4,trace5,trace6,trace7,trace8,trace9,trace10,trace11,trace12]

         var layout ={
                title:"Monthly Sales Net Received",
                xaxis:{title:"Months"},
                yaxis:{title:"Amount in $"},
                autosize:false,
                width:900,
                height:500,
                margin: {
                        l: 50,
                        r: 50,
                        b: 55,
                        t: 45,
                        pad: 2
                      }
        
         }
        
         Plotly.newPlot("plot2",data,layout)

}

//////
function getData2(dataset){
 x=[]
 y=[]
 console.log(dataset)
 switch (dataset){
        case "All":
         init()
         break;

         case "2023":
         x=yr20232.map(row=>row.month),
         y=yr20232.map(row=>row.NetRecd)
         loc=12
         break;        
         
         case "2022":
         x=yr20222.map(row=>row.month),
         y=yr20222.map(row=>row.NetRecd)
         loc=11
         break;

         case "2021":
         x=yr20212.map(row=>row.month),
         y=yr20212.map(row=>row.NetRecd)
         loc=0
         break;
         case "2020":
         x=yr20202.map(row=>row.month),
         y=yr20202.map(row=>row.NetRecd)
         loc=1
         break;
        case "2019":
         x=yr20192.map(row=>row.month),
         y=yr20192.map(row=>row.NetRecd)
         loc=2
         break;
        case "2018":
         x=yr20182.map(row=>row.month),
         y=yr20182.map(row=>row.NetRecd)
         loc=3
                        break;
        case "2017":
          x=yr20172.map(row=>row.month),
          y=yr20172.map(row=>row.NetRecd)
          loc=4
                        break;
        case "2016":
           x=yr20162.map(row=>row.month),
           y=yr20162.map(row=>row.NetRecd)
           loc=5
                         break; 
        case "2015":
         x=yr20152.map(row=>row.month),
         y=yr20152.map(row=>row.NetRecd)
         loc=6
                break;                                                                                                                     
        case "2014":
          x=yr20142.map(row=>row.month),
          y=yr20142.map(row=>row.NetRecd)
         loc=7
        break;
        case "2013":
         x=yr20132.map(row=>row.month),
         y=yr20132.map(row=>row.NetRecd)
         loc=8
         break;              
         case "2012":
         x=yr20122.map(row=>row.month),
         y=yr20122.map(row=>row.NetRecd)
         loc=9
         break; 
         case "2011":
                        x=yr20112.map(row=>row.month),
                        y=yr20112.map(row=>row.NetRecd)
                        loc=10
                        break;                

        default:
        inittwo()
          break;
 }
          //console.log("xlength",x)
 if (x.length==0){
        inittwo()
 }
 else{
         updatePlotlytwo(x,y,loc)
 }
}

function updatePlotlytwo(newx,newy,loc){
        inittwo()
        arrtrc=[]
        origarrtrc=[0,1,2,3,4,5,6,7,8,9,10,11,12]
        for(i=0;i<origarrtrc.length;i++){
                if(origarrtrc[i] != loc){arrtrc.push(origarrtrc[i])}
        }

        console.log(arrtrc)
        var LINE = document.getElementById("plot2")
        Plotly.deleteTraces(LINE,arrtrc)
        Plotly.restyle(LINE,"x",[newx])
        Plotly.restyle(LINE,"y",[newy])
}
// YOUR CODE HERE
//urlstring='https://aircraft-apis.herokuapp.com/readSales'
urlstring='http://127.0.0.1:5000/readSales'
//your code
d3.json(urlstring).then(function(sample_m) {
        data1=sample_m
        data=data1
        yr20232 = data1.filter(year20232) 
        yr20222 = data1.filter(year20222)       
        yr20212 = data1.filter(year20212)
        yr20202 = data1.filter(year20202)
        yr20192 = data1.filter(year20192)
        yr20182 = data1.filter(year20182)
        yr20172 = data1.filter(year20172)
        yr20162 = data1.filter(year20162)
        yr20152 = data1.filter(year20152)
        yr20142 = data1.filter(year20142)
        yr20132 = data1.filter(year20132)
        yr20122 = data1.filter(year20122)
        yr20112 = data1.filter(year20112)
        inittwo()
})