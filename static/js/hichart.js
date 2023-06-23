const startYear = 2000,
    endYear = 2023,
    btn = document.getElementById('play-pause-button'),
    input = document.getElementById('play-range'),
    nbr = 40;
    objAirdata={}
console.log(input.value)
let dataset, chart;
let AirlineData=[];
const AirlineName=[];



let totalmodcount = 0;
/*
 * Animate dataLabels functionality
 */
(function (H) {
    const FLOAT = /^-?\d+\.?\d*$/;

    // Add animated textSetter, just like fill/strokeSetters
    H.Fx.prototype.textSetter = function () {
        let startValue = this.start.replace(/ /g, ''),
            endValue = this.end.replace(/ /g, ''),
            currentValue = this.end.replace(/ /g, '');

        if ((startValue || '').match(FLOAT)) {
            startValue = parseInt(startValue, 10);
            endValue = parseInt(endValue, 10);

            // No support for float
            currentValue = Highcharts.numberFormat(
                Math.round(startValue + (endValue - startValue) * this.pos),
                0
            );
        }

        this.elem.endText = this.end;

        this.elem.attr(this.prop, currentValue, null, true);
    };

    // Add textGetter, not supported at all at this moment:
    H.SVGElement.prototype.textGetter = function () {
        const ct = this.text.element.textContent || '';
        return this.endText ? this.endText : ct.substring(0, ct.length / 2);
    };

    // Temporary change label.attr() with label.animate():
    // In core it's simple change attr(...) => animate(...) for text prop
    H.wrap(H.Series.prototype, 'drawDataLabels', function (proceed) {
        const attr = H.SVGElement.prototype.attr,
            chart = this.chart;

        if (chart.sequenceTimer) {
            this.points.forEach(point =>
                (point.dataLabels || []).forEach(
                    label =>
                        (label.attr = function (hash) {
                            if (
                                hash &&
                                hash.text !== undefined &&
                                chart.isResizing === 0
                            ) {
                                const text = hash.text;

                                delete hash.text;

                                return this
                                    .attr(hash)
                                    .animate({ text });
                            }
                            return attr.apply(this, arguments);

                        })
                )
            );
        }

        const ret = proceed.apply(
            this,
            Array.prototype.slice.call(arguments, 1)
        );

        this.points.forEach(p =>
            (p.dataLabels || []).forEach(d => (d.attr = attr))
        );

        return ret;
    });
}(Highcharts));


function getData(year) {
    totalmodcount=0
    const output = Object.entries(dataset)
        .map(Airline => {
            //console.log('dataset',dataset)
            //console.log('airline',Airline)
            sample_m=Airline[1]
            objAirdata=Object.assign(objAirdata,sample_m)
            //console.log(sample_m)
            //console.log('objsample@@',objAirdata)
            ///const [AirlineName, AirlineData] = Airline;
            ///const AirlineName =

           const [AirlineName] = Object.keys(Airline[1])
            //for (var key in objAirdata) { 
                //AirlineName.push(key)
             //  console.log(key, objAirdata[key])
              //  AirlineData.push(objAirdata[key])}

            totalmodcount=objAirdata[AirlineName][year]+totalmodcount
            
            console.log('*****',year,AirlineName,objAirdata[AirlineName][year],totalmodcount)
            return [AirlineName, Number(objAirdata[AirlineName][year],totalmodcount)];
        })
        .sort((a, b) => b[1] - a[1]);
        console.log('output@@@@@@!!!',output)
    return [output[0], output.slice(1, nbr)];
}

function getSubtitle() {
    let AirlineData= (getData(input.value)[0][1]).toFixed(2);
    console.log('subtitle++++++',totalmodcount)
    const a=totalmodcount
    totalmodcount=0
    return `<span style="font-size: 80px">${input.value}</span>
        <br>
        <span style="font-size: 22px">
            Total: <b>: ${a}</b> 
        </span>`;
        
}
//AirlineData
(async () => {

    dataset = await fetch(
        'http://127.0.0.1:5000/animationgraphdata'
    ).then(response => response.json());

    
    chart = Highcharts.chart('container', {
        chart: {
            animation: {
                duration: 500
            },
            marginRight: 50
        },
        title: {
            text: 'Collection Growth by Airlines',
            align: 'left'
        },
        subtitle: {
            useHTML: true,
            text: getSubtitle(),
            floating: true,
            align: 'right',
            verticalAlign: 'middle',
            y: -80,
            x: -100
        },

        legend: {
            enabled: false
        },
        xAxis: {
            type: 'category'
        },
        yAxis: {
            opposite: true,
            tickPixelInterval: 150,
            title: {
                text: null
            }
        },
        plotOptions: {
            series: {
                animation: false,
                groupPadding: 0,
                pointPadding: 0.1,
                borderWidth: 0,
                colorByPoint: true,
                dataSorting: {
                    enabled: true,
                    matchByName: true
                },
                type: 'bar',
                dataLabels: {
                    enabled: true
                }
            }
        },
        series: [
            {
                type: 'bar',
                name: startYear,
                data: getData(startYear)[1]
            }
        ],
        responsive: {
            rules: [{
                condition: {
                    maxWidth: 550
                },
                chartOptions: {
                    xAxis: {
                        visible: false
                    },
                    subtitle: {
                        x: 0
                    },
                    plotOptions: {
                        series: {
                            dataLabels: [{
                                enabled: true,
                                y: 8
                            }, {
                                enabled: true,
                                format: '{point.name}',
                                y: -8,
                                style: {
                                    fontWeight: 'normal',
                                    opacity: 0.7
                                }
                            }]
                        }
                    }
                }
            }]
        }
    });
})();

/*
 * Pause the timeline, either when the range is ended, or when clicking the pause button.
 * Pausing stops the timer and resets the button to play mode.
 */
function pause(button) {
    button.title = 'play';
    button.className = 'fa fa-play';
    clearTimeout(chart.sequenceTimer);
    chart.sequenceTimer = undefined;
}

/*
 * Update the chart. This happens either on updating (moving) the range input,
 * or from a timer when the timeline is playing.
 */
function update(increment) {
    if (increment) {
        input.value = parseInt(input.value, 10) + increment;
    }
    if (input.value >= endYear) {
        // Auto-pause
        pause(btn);
    }

    chart.update(
        {
            subtitle: {
                text: getSubtitle()
            }
        },
        false,
        false,
        false
    );

    chart.series[0].update({
        name: input.value,
        data: getData(input.value)[1]
    });
}

/*
 * Play the timeline.
 */
function play(button) {
    button.title = 'pause';
    button.className = 'fa fa-pause';
    chart.sequenceTimer = setInterval(function () {
        update(1);
    }, 500);
}

btn.addEventListener('click', function () {
    if (chart.sequenceTimer) {
        pause(this);
    } else {
        play(this);
    }
});
/*
 * Trigger the update on the range bar click.
 */
input.addEventListener('click', function () {
    update();
});
