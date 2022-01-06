//console.log(data1);
var yr2022 = {}
var yr2021 = {}
var yr2020 = {}
var yr2019 = {}
var yr2018 = {}
var yr2017 = {}
var yr2016 = {}
var yr2015 = {}
var yr2014 = {}
var yr2013 = {}
var yr2012 = {}
var yr2011 = {}
var data=[]
var data1=[]
function year2022(sale) {
        return sale.year == '2022';
      }
function year2021(sale) {
        return sale.year == '2021';
      }
function year2020(sale) {
        return sale.year == '2020';
      }
function year2019(sale) {
        return sale.year == '2019';
      }
function year2018(sale) {
        return sale.year == '2018';
      }
function year2017(sale) {
        return sale.year == '2017';
      }
function year2016(sale) {
        return sale.year == '2016';
      }
function year2015(sale) {
        return sale.year == '2015';
      }
function year2014(sale) {
        return sale.year == '2014';
      }
function year2013(sale) {
        return sale.year == '2013';
      }

function year2012(sale) {
        return sale.year == '2012';
      }
function year2011(sale) {
        return sale.year == '2011';
      }



var surface=[]
function surfacedata(){
        var surface=[]
        surface.push(yr2022.map(row=>row.profit_loss))
        surface.push(yr2021.map(row=>row.profit_loss))
        surface.push(yr2020.map(row=>row.profit_loss))
        surface.push(yr2019.map(row=>row.profit_loss))
        surface.push(yr2018.map(row=>row.profit_loss))
        surface.push(yr2017.map(row=>row.profit_loss))
        surface.push(yr2016.map(row=>row.profit_loss))
        surface.push(yr2015.map(row=>row.profit_loss))
        surface.push(yr2014.map(row=>row.profit_loss))
        surface.push(yr2013.map(row=>row.profit_loss))
        surface.push(yr2012.map(row=>row.profit_loss))
        surface.push(yr2011.map(row=>row.profit_loss))
        return surface
}
var loc =0
//////
function init(){

        //console.log(yr2019)
        var trace={x:yr2022.map(row=>row.month),
                y:yr2022.map(row=>row.profit_loss),
                name:"2022",
                 type:"line"
      }
        var trace1={x:yr2021.map(row=>row.month),
                y:yr2021.map(row=>row.profit_loss),
                name:"2021",
                 type:"bar"
      }
        var trace2={x:yr2020.map(row=>row.month),
                y:yr2020.map(row=>row.profit_loss),
                name:"2020",
                 type:"bar"
      }
        var trace3={x:yr2019.map(row=>row.month),
                   y:yr2019.map(row=>row.profit_loss),
                   name:"2019",
                    type:"bar"
         }
         

         
        var trace4={x:yr2018.map(row=>row.month),
                y:yr2018.map(row=>row.profit_loss),
                name:"2018",
                 type:"bar"
        }
        
         

         var trace5={x:yr2017.map(row=>row.month),
                y:yr2017.map(row=>row.profit_loss),
                name:"2017",
                 type:"bar"
                
         }
         

         var trace6={x:yr2016.map(row=>row.month),
                y:yr2016.map(row=>row.profit_loss),
                name:"2016",
                 type:"bar"
        }
        

         var trace7={x: yr2015.map(row=>row.month),
                     y: yr2015.map(row=>row.profit_loss),
                        name:"2015",
                         type:"bar"
        }
        

        var trace8={x: yr2014.map(row=>row.month),
                y: yr2014.map(row=>row.profit_loss),
                   name:"2014",
                    type:"bar"
   }
   
        var trace9={x: yr2013.map(row=>row.month),
                y: yr2013.map(row=>row.profit_loss),
                name:"2013",
                type:"bar"
}    

var trace10={x: yr2012.map(row=>row.month),
        y: yr2012.map(row=>row.profit_loss),
        name:"2012",
        type:"bar"
} 

var trace11={x: yr2011.map(row=>row.month),
        y: yr2011.map(row=>row.profit_loss),
        name:"2011",
        type:"line"
}            
surface=surfacedata()
        var data=[trace,trace1,trace2,trace3,trace4,trace5,trace6,trace7,trace8,trace9,trace10,trace11]

         var layout ={
                title:"Monthly Profit and Loss",
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
        
         Plotly.newPlot("plot",data,layout)
         Plotly.newPlot("plot4",[{
                z: surface,
                type: 'surface'
         }],layout);

}

//////
function getData(dataset){
 x=[]
 y=[]
 var surface=[]
 console.log(dataset)
 switch (dataset){
        case "All":
         init()
         break
         case "2022":
                x=yr2022.map(row=>row.month),
                y=yr2022.map(row=>row.profit_loss)
                surface.push(yr2022.map(row=>row.profit_loss))
       
                loc=0
                break;

         case "2021":
         x=yr2021.map(row=>row.month),
         y=yr2021.map(row=>row.profit_loss)
         surface.push(yr2021.map(row=>row.profit_loss))

         loc=1
         break;
         case "2020":
         x=yr2020.map(row=>row.month),
         y=yr2020.map(row=>row.profit_loss)
         surface.push(yr2020.map(row=>row.profit_loss))

         loc=2
         break;
        case "2019":
         x=yr2019.map(row=>row.month),
         y=yr2019.map(row=>row.profit_loss)
         surface.push(yr2019.map(row=>row.profit_loss))

         loc=3
         break;
        case "2018":
         x=yr2018.map(row=>row.month),
         y=yr2018.map(row=>row.profit_loss)
         surface.push(yr2018.map(row=>row.profit_loss))

         loc=4
                        break;
        case "2017":
          x=yr2017.map(row=>row.month),
          y=yr2017.map(row=>row.profit_loss)
          surface.push(yr2017.map(row=>row.profit_loss))

          loc=5
                        break;
        case "2016":
           x=yr2016.map(row=>row.month),
           y=yr2016.map(row=>row.profit_loss)
           surface.push(yr2016.map(row=>row.profit_loss))


           loc=6
                         break; 
        case "2015":
         x=yr2015.map(row=>row.month),
         y=yr2015.map(row=>row.profit_loss)
         surface.push(yr2015.map(row=>row.profit_loss))


         loc=7
                break;                                                                                                                     
        case "2014":
          x=yr2014.map(row=>row.month),
          y=yr2014.map(row=>row.profit_loss)
          surface.push(yr2014.map(row=>row.profit_loss))

         loc=8
        break;
        case "2013":
         x=yr2013.map(row=>row.month),
         y=yr2013.map(row=>row.profit_loss)
         surface.push(yr2013.map(row=>row.profit_loss))

         loc=9
         break;              
         case "2012":
         x=yr2012.map(row=>row.month),
         y=yr2012.map(row=>row.profit_loss)
         surface.push(yr2012.map(row=>row.profit_loss))
     
         loc=10
         break; 
         case "2011":
                        x=yr2011.map(row=>row.month),
                        y=yr2011.map(row=>row.profit_loss)
                        surface.push(yr2011.map(row=>row.profit_loss))
                        loc=11
                        break;                

        default:
          init()
          break;
 }
          //console.log("xlength",x)
 if (x.length==0){
         init()
 }
 else{
         updatePlotly(x,y,loc,surface)
 }
 getData2(dataset)
 getData3(dataset)
}

function updatePlotly(newx,newy,loc,surface){
        init()
        arrtrc=[]
        var layout ={
                title:"Monthly Profit and Loss",
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
        origarrtrc=[0,1,2,3,4,5,6,7,8,9,10,11]///chkkk
        for(i=0;i<origarrtrc.length;i++){
                if(origarrtrc[i] != loc){arrtrc.push(origarrtrc[i])}
        }

        console.log("XXX",surface)
        var surface1=[]
        surface1.push(surface)
        var LINE = document.getElementById("plot")
        var splot =document.getElementById("plot4")
        Plotly.deleteTraces(LINE,arrtrc)
        Plotly.deleteTraces(splot,[0])
        Plotly.restyle(LINE,"x",[newx])
        Plotly.restyle(LINE,"y",[newy])
        Plotly.restyle(splot,{"z":[surface]})
}
// YOUR CODE HERE
urlstring='http://127.0.0.1:5000/readSales'

d3.json(urlstring).then(function(sample_m) {
        data1=sample_m
        data=data1
        yr2022 = data1.filter(year2022)
        yr2021 = data1.filter(year2021)
        yr2020 = data1.filter(year2020)
        yr2019 = data1.filter(year2019)
        yr2018 = data1.filter(year2018)
        yr2017 = data1.filter(year2017)
        yr2016 = data1.filter(year2016)
        yr2015 = data1.filter(year2015)
        yr2014 = data1.filter(year2014)
        yr2013 = data1.filter(year2013)
        yr2012 = data1.filter(year2012)
        yr2011 = data1.filter(year2011)
        init()
})