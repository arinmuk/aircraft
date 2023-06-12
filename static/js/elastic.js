var aircraftfull=[]
var mainairdata=[]
var aircraft=[]
var data={}
function populaterows(tableData1){
  d3.select("tbody").selectAll("td").remove()
  var tbody = d3.select("tbody")
  tableData1.forEach(element=>{
  var trow = tbody.append("tr")
  Object.entries(element).forEach(([key,val])=>{
    if (key != "DATEOFORDER"){  
    trow.append("td").text(val)}
    else {
        var utcSeconds = val;
        //var d = new Date(utcSeconds); // The 0 there is the key, which sets the date to the epoch
        var d = new Date(utcSeconds)
        var m= d.getMonth()+1
        var dd= d.getDate()
        var year=d.getFullYear()
        dtstr=m+'/'+dd+'/'+year
        d= dtstr
        //d.setUTCSeconds(utcSeconds)
        //console.log(d)
        trow.append("td").text(d)
    }
})


})}
function populateairline(data){
  //var citydrpdn =d3.select("#city")
  //var cell =citydrpdn.append("option")

  var AIRLINE=data.map(row=>row.AIRLINE)
  console.log(AIRLINE)
  var AIRLINEu=[...new Set(AIRLINE)]
  console.log(AIRLINEu)
  var cell = inputdatectl.append("option")
  cell.text('')
  arrAIRLINE=Array.from(AIRLINEu).sort()
  arrAIRLINE.forEach(AIRLINE=>{
      var cell =inputdatectl.append("option")
      cell.text(AIRLINE)
  })
}
function populateregistration(data){
    //var citydrpdn =d3.select("#city")
    //var cell =citydrpdn.append("option")

    var REGISTRATION=data.map(row=>row.REGISTRATION)
    console.log(REGISTRATION)
    var REGISTRATIONu=[...new Set(REGISTRATION)]
    console.log(REGISTRATION)
    var cell = inputcityctl.append("option")
    cell.text('')
    arrREGISTRATION=Array.from(REGISTRATIONu).sort()
    arrREGISTRATION.forEach(REGISTRATION=>{
        var cell =inputcityctl.append("option")
        cell.text(REGISTRATION)
    })
}
function populatesize(data){
    //var citydrpdn =d3.select("#city")
    //var cell =citydrpdn.append("option")

    var SIZE=data.map(row=>row.SIZE)
    console.log(SIZE)
    var SIZEu=[...new Set(SIZE)]
    console.log(SIZEu)
    var cell =inputstatectl.append("option")
    cell.text('')
    SIZEu.forEach(SIZE=>{
        var cell =inputstatectl.append("option")
        cell.text(SIZE)
    })
}

function populateairtype(data){
    //var citydrpdn =d3.select("#city")
    //var cell =citydrpdn.append("option")

    var AIRCRAFT_TYPE=data.map(row=>row.AIRCRAFT_TYPE)
    console.log(AIRCRAFT_TYPE)
    var AIRCRAFT_TYPEu=[...new Set(AIRCRAFT_TYPE)]
    console.log(AIRCRAFT_TYPEu)
    var cell =inputcntryctl.append("option")
    cell.text('')
    arrAIRCRAFT_TYPE=Array.from(AIRCRAFT_TYPEu).sort()
    arrAIRCRAFT_TYPE.forEach(AIRCRAFT_TYPE=>{
        var cell =inputcntryctl.append("option")
        cell.text(AIRCRAFT_TYPE)
    })
}
function populatecompany(data){
    //var citydrpdn =d3.select("#city")
    //var cell =citydrpdn.append("option")

    var COMPANY=data.map(row=>row.COMPANY)
    console.log(COMPANY)
    var COMPANYu=[...new Set(COMPANY)]
    console.log(COMPANYu)
    var cell =inputshapectl.append("option")
    cell.text('')
    COMPANYu.forEach(COMPANY=>{
        var cell =inputshapectl.append("option")
        cell.text(COMPANY)
    })
}
urlstring='http://127.0.0.1:9200/aircraft/_search/?size=10000'

d3.json(urlstring).then(function(sample_m) {
  var objsample=sample_m
  //console.log(objsample)
  aircraft=objsample.hits.hits
  // objsample.forEach(element=>{
  //   console.log(element)
  // })
console.log(aircraft)
aircraft.forEach(element => {
  aircraftfull.push(element["_source"])
  
});


aircraftfull.forEach(element=>{
  delete element["@timestamp"]
  delete element["@version"]
  delete element["column17"]
  delete element["host"]
  delete element["path"]
  delete element["message"]

})
console.log(aircraftfull)
mainairdata=aircraftfull
console.log(mainairdata)
populaterows(aircraftfull)
populateairline(mainairdata)
populateregistration(mainairdata)
populatesize(mainairdata)
populateairtype(mainairdata)
populatecompany(mainairdata)
})
var tableData = data;
var datavalue=''
// YOUR CODE HERE!
var inputcityctl = d3.select("#Registration")
var inputdatectl = d3.select("#airline")
var inputstatectl = d3.select("#Scale")
var inputcntryctl = d3.select("#AIRCRAFT_TYPE")
var inputshapectl = d3.select("#COMPANY")

var filter=d3.select("#filter-btn")
filter.on("click",function(){

    d3.event.preventDefault()
   
    var inputdate=inputdatectl.property("value")
    console.log(inputdate)

    
    var inputcity=inputcityctl.property("value")
    console.log(inputcity)
    
    var inputstate =inputstatectl.property("value")
    var inputcntry =inputcntryctl.property("value")
    var inputshape =inputshapectl.property("value")

    var filterdata = aircraftfull
    if (inputdate != ''){
        var filterdata =filterdata.filter(data=> data.AIRLINE===inputdate)
        datavalue='hasvalue'
    }
    if (inputcity !=''){
        var filterdata =filterdata.filter(data=>data.REGISTRATION===inputcity)
        datavalue='hasvalue'
    }
    if (inputstate!=''){
        var filterdata=filterdata.filter(data=>data.SIZE===inputstate)
        datavalue='hasvalue'
    }
    if (inputcntry!=''){
        var filterdata=filterdata.filter(data=>data.AIRCRAFT_TYPE===inputcntry)
        datavalue='hasvalue'
    }
    if (inputshape!=''){
        var filterdata=filterdata.filter(data=>data.COMPANY===inputshape)
        datavalue='hasvalue'
    }
    if (datavalue==''){
      populaterows(mainairdata)
      }
      else{
          populaterows(filterdata)
          datavalue=''
      }
})

//populaterows(mainairdata)


