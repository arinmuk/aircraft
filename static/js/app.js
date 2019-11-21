// from data.js
var tableData = data;
var datavalue=''
// YOUR CODE HERE!
var inputcityctl = d3.select("#city")
var inputdatectl = d3.select(".form-control")
var inputstatectl = d3.select("#state")
var inputcntryctl = d3.select("#country")
var inputshapectl = d3.select("#shape")

function populaterows(tableData1){
d3.select("tbody").selectAll("td").remove()
var tbody = d3.select("tbody")
tableData1.forEach(element=>{
var trow = tbody.append("tr")

Object.values(element).forEach(val=>trow.append("td").text(val))

})}
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

    var filterdata = tableData
    if (inputdate != ''){
        var filterdata =filterdata.filter(data=> data.datetime===inputdate)
        datavalue='hasvalue'
    }
    if (inputcity !=''){
        var filterdata =filterdata.filter(data=>data.city===inputcity)
        datavalue='hasvalue'
    }
    if (inputstate!=''){
        var filterdata=filterdata.filter(data=>data.state===inputstate)
        datavalue='hasvalue'
    }
    if (inputcntry!=''){
        var filterdata=filterdata.filter(data=>data.country===inputcntry)
        datavalue='hasvalue'
    }
    if (inputshape!=''){
        var filterdata=filterdata.filter(data=>data.shape===inputshape)
        datavalue='hasvalue'
    }

    // if (inputdate != '' && inputcity !=''){
    //     var filterdata =tableData.filter(data=> data.datetime===inputdate && data.city==inputcity)
    //     datavalue='hasvalue'
    //     console.log(filterdata)
    // } else if  (inputdate != '' && inputcity ===''){
    //     var filterdata =tableData.filter(data=> data.datetime===inputdate)
    //     datavalue='hasvalue'
    //     console.log(filterdata)

    // } else if (inputcity != '' && inputdate ==='') {
    //     var filterdata =tableData.filter(data=> data.city===inputcity)
    //     datavalue='hasvalue'
    //     console.log(filterdata)
    // }

    if (datavalue==''){
        populaterows(tableData)
        }
        else{
            populaterows(filterdata)
            datavalue=''
        }
})

populaterows(tableData)

function populatecity(data){
    //var citydrpdn =d3.select("#city")
    //var cell =citydrpdn.append("option")

    var city=data.map(row=>row.city)
    console.log(city)
    var cityu=[...new Set(city)]
    console.log(cityu)
    var cell = inputcityctl.append("option")
    cell.text('')
    cityu.forEach(city=>{
        var cell =inputcityctl.append("option")
        cell.text(city)
    })
}
function populatestate(data){
    //var citydrpdn =d3.select("#city")
    //var cell =citydrpdn.append("option")

    var state=data.map(row=>row.state)
    console.log(state)
    var stateu=[...new Set(state)]
    console.log(stateu)
    var cell =inputstatectl.append("option")
    cell.text('')
    stateu.forEach(state=>{
        var cell =inputstatectl.append("option")
        cell.text(state)
    })
}

function populatecountry(data){
    //var citydrpdn =d3.select("#city")
    //var cell =citydrpdn.append("option")

    var cntry=data.map(row=>row.country)
    console.log(cntry)
    var cntryu=[...new Set(cntry)]
    console.log(cntryu)
    var cell =inputcntryctl.append("option")
    cell.text('')
    cntryu.forEach(cntry=>{
        var cell =inputcntryctl.append("option")
        cell.text(cntry)
    })
}
function populateshape(data){
    //var citydrpdn =d3.select("#city")
    //var cell =citydrpdn.append("option")

    var shape=data.map(row=>row.shape)
    console.log(shape)
    var shapeu=[...new Set(shape)]
    console.log(shapeu)
    var cell =inputshapectl.append("option")
    cell.text('')
    shapeu.forEach(shape=>{
        var cell =inputshapectl.append("option")
        cell.text(shape)
    })
}
populatecity(tableData)
populatestate(tableData)
populatecountry(tableData)
populateshape(tableData)