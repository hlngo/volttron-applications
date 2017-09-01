/**
 * Created by ngoh511 on 6/5/15.
 */

$(function() {
    //Global vars
    var vc_server = '';

    var req_id = '99352-4';

    var cacheData = {};
    var preCacheData = {};
    var cacheDataPathSep = '__';
    var checkCacheData;
    var queue = [];
    var isLoading = false;
    var noOfReq = -1;

    var prefix = 'record';
    var econ_rcx = 'EconomizerAIRCx'; //'Economizer_RCx'
    var air_rcx = 'AirsideAIRCx'; //'Airside_RCx'

    var points_available = {};
    points_available[econ_rcx] = ['diagnostic message', 'energy impact'];
    points_available[air_rcx] = ['diagnostic message'];

    var siteObjs = [{
        'name': 'PNNL',
        buildings: [
        {
            'name': 'SEB',
            'devices': [{
                'name': 'AHU1',
                'dx': [econ_rcx, air_rcx]
            }, {
                'name': 'AHU2',
                'dx': [econ_rcx, air_rcx]
            }, {
                'name': 'AHU3',
                'dx': [econ_rcx, air_rcx]
            }, {
                'name': 'AHU4',
                'dx': [econ_rcx, air_rcx]
        }]},
        {
            'name': 'BUILDING3',
            'devices': [{
                'name': 'INTERIOR_AHU',
                'dx': [econ_rcx, air_rcx]
        }]
        },
        {
            'name': 'BUILDING4',
            'devices': [{
                'name': 'RTU3',
                'dx': [econ_rcx, air_rcx]
            }, {
                'name': 'RTU4',
                'dx': [econ_rcx, air_rcx]
            }, {
                'name': 'RTU5',
                'dx': [econ_rcx, air_rcx]
        }]},
        {
            'name': 'BUILDING5',
            'devices': [{
                'name': 'INTERIOR_AHU',
                'dx': [econ_rcx, air_rcx]
        }]},
        {
            'name': 'BUILDING6',
            'devices': [{
                'name': 'HP1',
                'dx': [econ_rcx, air_rcx]
            }, {
                'name': 'HP2',
                'dx': [econ_rcx, air_rcx]
            }, {
                'name': 'HP3',
                'dx': [econ_rcx, air_rcx]
            }, {
                'name': 'HP4',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP5',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP6',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP7',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP8',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP9',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP10',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP11',
                'dx': ['Economizer_RCx', 'Airside_RCx']
        }]},
        {
            'name': 'BUILDING7',
            'devices': [{
                'name': 'HP1',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP2',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP3',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP4',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP5',
                'dx': ['Economizer_RCx', 'Airside_RCx']
        }]},
        {
            'name': 'BUILDING8',
            'devices': [{
                'name': 'HP3',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP4',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP5',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP6',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP7',
                'dx': ['Economizer_RCx', 'Airside_RCx']
        }]},
        {
            'name': 'BUILDING9',
            'devices': [{
                'name': 'AHU6',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'AHU7',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'AHU8',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'AHU13',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'AHU20',
                'dx': ['Economizer_RCx', 'Airside_RCx']
        }]},
        {
            'name': 'BUILDING10',
            'devices': [{
                'name': 'HP1',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP2',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP3',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP4',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP5',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP6',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP7',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP8',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP9',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP10',
                'dx': ['Economizer_RCx', 'Airside_RCx']
            }, {
                'name': 'HP11',
                'dx': ['Economizer_RCx', 'Airside_RCx']
        }]},
        {
            'name': 'BUILDING14',
            'devices': [{
                'name': 'AHU1',
                'dx': ['Economizer_RCx', 'Airside_RCx']
        }]}
    ]}];

    var algo_set = {};
    algo_set[econ_rcx] = [
        'Temperature Sensor Dx',
        'Not Economizing When Unit Should Dx',
        'Economizing When Unit Should Not Dx',
        'Excess Outdoor-air Intake Dx',
        'Insufficient Outdoor-air Intake Dx'
    ];
    algo_set[air_rcx] = [
        'Duct Static Pressure Set Point Control Loop Dx',
        'High Duct Static Pressure Dx',
        'High Supply-air Temperature Dx',
        'Low Duct Static Pressure Dx',
        'Low Supply-air Temperature Dx',
        'No Static Pressure Reset Dx',
        'No Supply-air Temperature Reset Dx',
        'Operational Schedule Dx',
        'Supply-air Temperature Set Point Control Loop Dx'
    ];
    var color_codes = {};
    color_codes[econ_rcx] = {
        "-1": "GREY", //"No Diagnosis",
        "-99": "GREY", //"Supply fan is off, the diagnostic will only detect problems when the unit is operational.",
        "-89": "GREY", //"The OAT and RAT are too close for a conclusive diagnostic result",
        "-79": "GREY", //"The OAT sensor is outside the configured operating range.",
        "-69": "GREY", //"The RAT sensor is outside the configured operating range.",
        "-59": "GREY", //"The MAT sensor is outside the configured operating range.",
        "-49": "GREY", //"A temperature sensor problem was detected.  The remainder of the diagnostics depend on temperature sensor measurements to produce accurate diagnostic results.",
        "0": "GREEN", //"No problems detected.",
        "0.1": "RED", //"The OAT and MAT sensor readings are not consistent when the outdoor-air damper is fully open.",
        "1.1": "RED", //"A temperature sensor problem was detected the MAT sensor reading is less the the OAT and RAT sensor reading.",
        "2.1": "RED", //"A temperature sensor problem was detected the MAT sensor reading is greater the the OAT and RAT sensor reading.",
        "3.2": "GREY", //"The diagnostic resulted in an inconclusive result.",
        "10": "GREEN", //"No problems detected.",
        "11.1": "RED", //"Conditions are favorable for economizing but the OAD is frequently below 100% open.",
        "12.1": "RED", //"The OAD is open for economizing but the OAF indicates the unit is not bringing in near 100% OA.",
        "13.2": "GREY", //"The diagnostic resulted in an inconclusive result.",
        "14.2": "GREY", //"The unit is not cooling: diagnostic detects problems when unit is in cooling mode.",
        "15.2": "GREY", //"Conditions are not favorable for economizing: diagnostic detects problems when unit should be economizing.",
        "20": "GREEN", //"No problems detected.",
        "21.1": "RED", //"The OAD should be at the minimum position for ventilation but is significantly above that value.",
        "23.2": "GREY", //"The diagnostic resulted in an inconclusive result.",
        "25.2": "RED", //"Conditions are favorable for economizing: diagnostic detects problems when unit should not be economizing.",
        "30": "GREEN", //"No problems detected.",
        "31.2": "GREY", //"Inconclusive result: The OAF calculation led to an unexpected result.",
        "32.1": "RED", //"The OAD should be at the minimum for ventilation but is significantly above that value.",
        "33.1": "RED", //"Excess outdoor air is being provided.  This could significantly increase heating and cooling costs.",
        "34.1": "RED", //"The OAD should be at the minimum for ventilation but is significantly above that value. Excess outdoor air is being provided; This could significantly increase heating and cooling costs.",
        "35.2": "GREY", //"The diagnostic resulted in an inconclusive result.",
        "36.2": "GREY", //"Conditions are favorable for economizing: diagnostic detects problems when unit should not be economizing.",
        "40": "GREEN", //"No problems detected.",
        "41.2": "GREY", //"Inconclusive result: The OAF calculation led to an unexpected result.",
        "42.1": "RED", //"The OAD position is significantly below the minimum configured OAD position.",
        "43.1": "RED", //"Insufficient OA for ventilation is being provided.",
        "44.2": "GREY" //"The diagnostic resulted in an inconclusive result."
    };
    color_codes[air_rcx] = {
        "-1": "GREY", //"No Diagnosis",
        "-99": "GREY", //"Supply fan is off, the diagnostic will only detect problems when the unit is operational.",
        "-89": "GREY", //"Inconsistent date for input data.",
        "-79": "GREY", //"Insufficent input data for diagnostic.",
        "0": "GREEN", //"No problems detected.",
        "1.1": "RED", //"The duct static pressure is significantly deviating from its set point.",
        "2.2": "GREY", //"Duct static pressure set point data is not available.  The Set Point Control Loop Diagnostic requires set point data.",
        "10": "GREEN", //"No re-tuning opportunity was detected during the low duct static pressure diagnostic.",
        "11.1": "RED", //"The duct static pressure was detected to be too low (with autocorrection).",
        "12.1": "RED", //"The duct static pressure was detected to be too low but it is at the maximum configured value.",
        "13.1": "RED", //"The duct static pressure was detected to be too low (autocorrection disabled).",
        "14.1": "RED", //"The duct static pressure was detected to be too low but duct static pressure set point data is not available (cannot autocorrect).",
        "15.1": "RED", //"The duct static pressure was detected to be too low but the supply fan is running at maximum speed.",
        "20": "GREEN", //"No re-tuning opportunity was detected during the high duct static pressure diagnostic.",
        "21.1": "RED", //"The duct static pressure was detected to be too high (with autocorrection). ",
        "22.1": "RED", //"The duct static pressure was detected to be too high but its setpoint is at the minimum configured value.",
        "23.1": "RED", //"The duct static pressure was detected to be too high (autocorrection disabled).",
        "24.1": "RED", //"The duct static pressure has been detected to be too high but duct static pressure set point data is not available (cannot autocorrect).",
        "25.1": "RED", //"The duct static pressure has been detected to be too high but but the supply fan is running at minimum speed.",
        "30": "GREEN", //"No problems detected.",
        "31.1": "RED", //"The SAT is significantly deviating from its set point.",
        "32.2": "GREY", //"Supply-air temperature set point data is not available.  The Set Point Control Loop Diagnostic requires set point data.",
        "40": "GREEN", //"No re-tuning opportunity was detected during the low supply-air temperature diagnostic.",
        "41.1": "RED", //"The supply-air temperature was detected to be too low (with autocorrection).",
        "42.1": "RED", //"The supply-air temperature was detected to be too low but it is at the maximum configured value.",
        "43.1": "RED", //"The supply-air temperature has been detected to be too low but supply-air temperature set point data is not available (cannot autocorrect).",
        "44.1": "RED", //"The supply air temperature was detected to be too low (autocorrection disabled).",
        "50": "GREEN", //"No re-tuning opportunity was detected during the high supply-air temperature diagnostic.",
        "51.1": "RED", //"The supply-air temperature was detected to be too high (with autocorrection).",
        "52.1": "RED", //"The supply-air temperature was detected to be too high but it is at the minimum configured value.",
        "53.1": "RED", //"The supply air temperature was detected to be too high (autocorrection disabled).",
        "54.1": "RED", //"The supply-air temperature has been detected to be too high but supply-air temperature set point data is not available (cannot autocorrect).",
        "60": "GREEN", //"No problems detected.",
        "61.2": "GREY", //"Insufficient data for diagnostic.",
        "63.1": "RED", //"The unit is ON a significant amount of time during unoccupied schedule",
        "64.2": "GREY", //"The fan status shows the unit is off but the static pressure reading is high; verify the functionality of the pressure sensor.",
        "70": "GREEN", //"No problems detected.",
        "71.1": "RED", //"A duct static pressure reset was not detected.  Static pressure reset can save significant energy/money.",
        "80": "GREEN", //"No problems detected.",
        "81.1": "RED" //"A discharge-air temperature reset was not detected.  Discharge-air temperature reset can save significant energy/money."
    };
    var error_messages = {};
    error_messages [econ_rcx] = {
        "-1": "No Diagnosis",
        "-99": "Supply fan is off, the diagnostic will only detect problems when the unit is operational.",
        "-89": "The OAT and RAT are too close for a conclusive diagnostic result",
        "-79": "The OAT sensor is outside the configured operating range.",
        "-69": "The RAT sensor is outside the configured operating range.",
        "-59": "The MAT sensor is outside the configured operating range.",
        "-49": "A temperature sensor problem was detected.  The remainder of the diagnostics depend on temperature sensor measurements to produce accurate diagnostic results.",
        "0": "No problems detected.",
        "0.1": "The OAT and MAT sensor readings are not consistent when the outdoor-air damper is fully open.",
        "1.1": "A temperature sensor problem was detected the MAT sensor reading is less the the OAT and RAT sensor reading.",
        "2.1": "A temperature sensor problem was detected the MAT sensor reading is greater the the OAT and RAT sensor reading.",
        "3.2": "The diagnostic resulted in an inconclusive result.",
        "10": "No problems detected.",
        "11.1": "Conditions are favorable for economizing but the OAD is frequently below 100% open.",
        "12.1": "The OAD is open for economizing but the OAF indicates the unit is not bringing in near 100% OA.",
        "13.2": "The diagnostic resulted in an inconclusive result.",
        "14.2": "The unit is not cooling: diagnostic detects problems when unit is in cooling mode.",
        "15.2": "Conditions are not favorable for economizing: diagnostic detects problems when unit should be economizing.",
        "20": "No problems detected.",
        "21.1": "The OAD should be at the minimum position for ventilation but is significantly above that value.",
        "23.2": "The diagnostic resulted in an inconclusive result.",
        "25.2": "Conditions are favorable for economizing: diagnostic detects problems when unit should not be economizing.",
        "30": "No problems detected.",
        "31.2": "Inconclusive result: The OAF calculation led to an unexpected result.",
        "32.1": "The OAD should be at the minimum for ventilation but is significantly above that value.",
        "33.1": "Excess outdoor air is being provided.  This could significantly increase heating and cooling costs.",
        "34.1": "The OAD should be at the minimum for ventilation but is significantly above that value. Excess outdoor air is being provided; This could significantly increase heating and cooling costs.",
        "35.2": "The diagnostic resulted in an inconclusive result.",
        "36.2": "Conditions are favorable for economizing: diagnostic detects problems when unit should not be economizing.",
        "40": "No problems detected.",
        "41.2": "Inconclusive result: The OAF calculation led to an unexpected result.",
        "42.1": "The OAD position is significantly below the minimum configured OAD position.",
        "43.1": "Insufficient OA for ventilation is being provided.",
        "44.2": "The diagnostic resulted in an inconclusive result."
    };
    error_messages[air_rcx] = {
        "-1": "No Diagnosis",
        "-99": "Supply fan is off, the diagnostic will only detect problems when the unit is operational.",
        "-89": "Inconsistent date for input data.",
        "-89": "Inconsistent date for input data.",
        "-79": "Insufficent input data for diagnostic.",
        "0": "No problems detected.",
        "1.1": "The duct static pressure is significantly deviating from its set point.",
        "2.2": "Duct static pressure set point data is not available.  The Set Point Control Loop Diagnostic requires set point data.",
        "10": "No re-tuning opportunity was detected during the low duct static pressure diagnostic.",
        "11.1": "The duct static pressure was detected to be too low (with autocorrection).",
        "12.1": "The duct static pressure was detected to be too low but it is at the maximum configured value.",
        "13.1": "The duct static pressure was detected to be too low (autocorrection disabled).",
        "14.1": "The duct static pressure was detected to be too low but duct static pressure set point data is not available (cannot autocorrect).",
        "15.1": "The duct static pressure was detected to be too low but the supply fan is running at maximum speed.",
        "20": "No re-tuning opportunity was detected during the high duct static pressure diagnostic.",
        "21.1": "The duct static pressure was detected to be too high (with autocorrection). ",
        "22.1": "The duct static pressure was detected to be too high but its setpoint is at the minimum configured value.",
        "23.1": "The duct static pressure was detected to be too high (autocorrection disabled).",
        "24.1": "The duct static pressure has been detected to be too high but duct static pressure set point data is not available (cannot autocorrect).",
        "25.1": "The duct static pressure has been detected to be too high but but the supply fan is running at minimum speed.",
        "30": "No problems detected.",
        "31.1": "The SAT is significantly deviating from its set point.",
        "32.2": "Supply-air temperature set point data is not available.  The Set Point Control Loop Diagnostic requires set point data.",
        "40": "No re-tuning opportunity was detected during the low supply-air temperature diagnostic.",
        "41.1": "The supply-air temperature was detected to be too low (with autocorrection).",
        "42.1": "The supply-air temperature was detected to be too low but it is at the maximum configured value.",
        "43.1": "The supply-air temperature has been detected to be too low but supply-air temperature set point data is not available (cannot autocorrect).",
        "44.1": "The supply air temperature was detected to be too low (autocorrection disabled).",
        "50": "No re-tuning opportunity was detected during the high supply-air temperature diagnostic.",
        "51.1": "The supply-air temperature was detected to be too high (with autocorrection).",
        "52.1": "The supply-air temperature was detected to be too high but it is at the minimum configured value.",
        "53.1": "The supply air temperature was detected to be too high (autocorrection disabled).",
        "54.1": "The supply-air temperature has been detected to be too high but supply-air temperature set point data is not available (cannot autocorrect).",
        "60": "No problems detected.",
        "61.2": "Insufficient data for diagnostic.",
        "63.1": "The unit is ON a significant amount of time during unoccupied schedule",
        "64.2": "The fan status shows the unit is off but the static pressure reading is high; verify the functionality of the pressure sensor.",
        "70": "No problems detected.",
        "71.1": "A duct static pressure reset was not detected.  Static pressure reset can save significant energy/money.",
        "80": "No problems detected.",
        "81.1": "A discharge-air temperature reset was not detected.  Discharge-air temperature reset can save significant energy/money."
    };

    //Extensions & support functions
    Date.prototype.stdTimezoneOffset = function() {
        var jan = new Date(this.getFullYear(), 0, 1);
        var jul = new Date(this.getFullYear(), 6, 1);
        return Math.max(jan.getTimezoneOffset(), jul.getTimezoneOffset());
    };

    Date.prototype.dst = function() {
        return this.getTimezoneOffset() < this.stdTimezoneOffset();
    };

    function formatDate(d) {
        var dd = d.getDate();
        if (dd<10) dd= '0'+dd;
        var mm = d.getMonth() + 1;  // now moths are 1-12
        if (mm<10) mm= '0'+mm;
        var yy = d.getFullYear();
        return yy +'-' + mm + '-' + dd;
    }

    function formatFullDate(d) {
        var weekday = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
        var dn = weekday[d.getDay()];
        return dn + ' ' + formatDate(d);
    }

    function makeArray(lowEnd, highEnd) {
        var arr = [];
        while(lowEnd <= highEnd){
            arr.push(lowEnd++);
        }
        return arr;
    }

    function afddAggregateData(inData, legends, diagnosticList) {
        // resData = {
        //      "date": {
        //                      "diagnostic_name": {
        //                                              datetime
        //                                              diagnostic_name:
        //                                              diagnostic_message:
        //                                              energy_impact:
        //                                              color_code:
        //                      }
        //                      state: //combined state of all diagnostics
        //      }
        // }
        // arrData = [{
        //            date: ,
        //            y: ,
        //            state: ,
        //            diagnostic: ,
        //            diagnostic_message: ,
        //            energy_impact: ,
        //            hourly_result: [arrHrData]
        // }]
        // Aggregate & filter duplicated data
        var resData = {};
        inData.forEach(function(d) {
            if (d == null) { return; }
            var dt1 = d.date; //new Date(d.datetime);
            //var tsParts = d.datetime.split("T");
            var dateParts = formatDate(dt1); //tsParts[0];
            //var hrParts = dt1.getHours().toString(); //tsParts[1].split(":")[0];
            //var tsParts = d.datetime.split("T");
            //var dateParts = tsParts[0];
            //var hrParts = tsParts[1].split(":")[0];
            var hrParts = dt1.getHours().toString();
            var diagnostic = d.diagnostic_name;

            if (dateParts in resData) {
                if (diagnostic in resData[dateParts]) {
                    if (hrParts in resData[dateParts][diagnostic]) {
                        if (legends[d.color_code].state_value >=
                            legends[resData[dateParts][diagnostic][hrParts].color_code].state_value) {
                            resData[dateParts][diagnostic][hrParts] = d;
                        }
                    } else {
                        resData[dateParts][diagnostic][hrParts] = d;
                    }
                } else {
                    resData[dateParts][diagnostic] = {};
                    resData[dateParts][diagnostic][hrParts] = d;
                }
            } else {
                resData[dateParts] = {};
                resData[dateParts][diagnostic] = {};
                resData[dateParts][diagnostic][hrParts] = d;
            }
        });

        var arrData = [];
        // Get Date min & max
        var arrDate = [];
        for (var dt in resData) {
            if (resData.hasOwnProperty(dt)) {
                var dateParts = dt.split("-");
                var tempDate = new Date(dateParts[0], dateParts[1] - 1, dateParts[2], 0, 0, 0, 0);
                arrDate.push(tempDate);
            }
        }
        var domain = d3.extent(arrDate);
        var domainMax = domain[1];
        var domainMin = domain[0];
        var noDays = Math.round(Math.abs((domainMax - domainMin)/(24*60*60*1000)));

        // Convert hash to array and keep only necessary values
        // Fill in default result for hours that have no result
        // ToDo: Push all green to missing dates, missing hours in a daily-green-circle
        // ToDo: Push all grey to missing hours in a daily-one-grey-circle
        var defaultStateIfMissing = legends["GREY"].string;
        for (var numberOfDaysToAdd = 0; numberOfDaysToAdd <= noDays; numberOfDaysToAdd++) {
            var curDate = new Date(domainMin.getTime());
            curDate.setDate(curDate.getDate() + numberOfDaysToAdd);
            var strCurDate = formatDate(curDate);
            if (resData.hasOwnProperty(strCurDate)) {
                for (var i = 0; i< diagnosticList.length; i++) {
                    var energy_impact = "NA";
                    if (resData[strCurDate].hasOwnProperty(diagnosticList[i])) {
                        var arrHrData = [];
                        // Find the state for this date and the default state for missing hours
                        var state = {
                            state: legends["GREY"].string,
                            diagnostic: "",
                            diagnostic_message: legends["GREY"].value,
                            energy_impact: "NA"
                        };
                        for (var hr = 0; hr < 24; hr++) {
                            var iStr = hr.toString();//formatHour(i);
                            if (resData[strCurDate][diagnosticList[i]].hasOwnProperty(iStr)) {
                                if (resData[strCurDate][diagnosticList[i]][iStr].energy_impact != null)
                                    energy_impact = resData[strCurDate][diagnosticList[i]][iStr].energy_impact;
                                var cur_color_code = resData[strCurDate][diagnosticList[i]][iStr].color_code;
                                if (typeof cur_color_code != 'undefined')
                                {
                                    if (legends[cur_color_code].state_value >=
                                        legends[state.state].state_value) {
                                        state = {
                                            state: cur_color_code,
                                            diagnostic: resData[strCurDate][diagnosticList[i]][iStr].diagnostic_name,
                                            diagnostic_message: resData[strCurDate][diagnosticList[i]][iStr].diagnostic_message,
                                            energy_impact: energy_impact
                                        };
                                    }
                                }
                            }
                        }
//                        var defaultStateStr = state.state;
//                        if (state.state == legends["RED"].string) {
//                            defaultStateStr = legends["GREEN"].string;
//                        }
                        // Convert hash to array and fill in missing hours with default values
                        energy_impact = "NA";
                        for (var hr = 0; hr < 24; hr++) {
                            var iStr = hr.toString();//formatHour(i);
                            if (resData[strCurDate][diagnosticList[i]].hasOwnProperty(iStr)) {
                                if (resData[strCurDate][diagnosticList[i]][iStr].energy_impact != null)
                                    energy_impact = resData[strCurDate][diagnosticList[i]][iStr].energy_impact;
                                arrHrData.push({
                                    date: curDate,
                                    y: hr,
                                    state: resData[strCurDate][diagnosticList[i]][iStr].color_code,
                                    diagnostic: resData[strCurDate][diagnosticList[i]][iStr].diagnostic_name,
                                    diagnostic_message: resData[strCurDate][diagnosticList[i]][iStr].diagnostic_message,
                                    energy_impact: energy_impact
                                });
                            } else {
                                arrHrData.push({
                                    date: curDate,
                                    y: hr,
                                    state: defaultStateIfMissing,
                                    diagnostic: "",
                                    diagnostic_message: legends[defaultStateIfMissing].value,
                                    energy_impact: "NA"
                                });
                            }
                        }
                        // Set state for this date-diagnostic
                        arrData.push({
                            date: curDate,
                            y: i,
                            state: state.state,
                            diagnostic: state.diagnostic,
                            diagnostic_message: state.diagnostic_message,
                            energy_impact: state.energy_impact,
                            hourly_result: arrHrData
                        });
                    } else {
                        var arrHrData = [];
                        for (var hr=0; hr<24; hr++) {
                            arrHrData.push({
                                date: curDate,
                                y: hr,
                                //state: legends["GREEN"].string,
                                state: defaultStateIfMissing,
                                diagnostic: "",
                                //diagnostic_message: legends["GREEN"].value,
                                diagnostic_message: legends[defaultStateIfMissing].value,
                                energy_impact: "NA"
                            });
                        }
                        arrData.push({
                            date: curDate,
                            y: i,
                            //state: legends["GREEN"].string,
                            state: defaultStateIfMissing,
                            diagnostic: "",
                            diagnostic_message: legends[defaultStateIfMissing].value,
                            energy_impact: "NA",
                            hourly_result: arrHrData
                        });
                    }
                }
            } else { //set color for non-exist data = default (ie. GREY)
                for (var i = 0; i< diagnosticList.length; i++) {
                    var arrHrData = [];
                    for (var hr=0; hr<24; hr++) {
                        arrHrData.push({
                            date: curDate,
                            y: hr,
                            state: defaultStateIfMissing,
                            diagnostic: "",
                            diagnostic_message: legends[defaultStateIfMissing].value,
                            energy_impact: "NA"
                        });
                    }
                    arrData.push({
                        date: curDate,
                        y: i,
                        state: defaultStateIfMissing,
                        diagnostic: "",
                        diagnostic_message: legends[defaultStateIfMissing].value,
                        energy_impact: "NA",
                        hourly_result: arrHrData
                    });
                }
            }
        }

        return arrData;
    }

    function retroCommissioningAFDDSVG(data,timezone) {
        var econDiagnosticList = [
            'Temperature Sensor Dx',
            'Economizing When Unit Should Dx',
            'Economizing When Unit Should Not Dx',
            'Excess Outdoor-air Intake Dx',
            'Insufficient Outdoor-air Intake Dx'];
        var econDiagnosticList2 = [
            'Temperature Sensor Dx',
            'Not Economizing When Unit Should Dx',
            'Economizing When Unit Should Not Dx',
            'Excess Outdoor-air Intake Dx',
            'Insufficient Outdoor-air Intake Dx'
        ];
        var hwDiagnosticList = [
            'HW Differential Pressure Control Loop Dx',
            'HW Supply Temperature Control Loop Dx',
            'HW loop High Differential Pressure Dx',
            'HW loop Differential Pressure Reset Dx',
            'HW loop High Supply Temperature Dx',
            'HW loop Supply Temperature Reset Dx',
            'HW loop Low Delta-T Dx'];

        var arDiagnosticList = [
            'Duct Static Pressure Set Point Control Loop Dx', //'Duct Static Pressure Control Loop Dx',
            'Low Duct Static Pressure Dx',
            'High Duct Static Pressure Dx',
            'No Static Pressure Reset Dx',
            'Supply-air Temperature Set Point Control Loop Dx', //'Supply-air Temperature Control Loop Dx',
            'Low Supply-air Temperature Dx',
            'High Supply-air Temperature Dx',
            'No Supply-air Temperature Reset Dx',
            'Operational Schedule Dx'];

        var arDiagnosticList2 = [
            'Duct Static Pressure Set Point Control Loop Dx', //'Duct Static Pressure Control Loop Dx',
            'Low Duct Static Pressure Dx',
            'High Duct Static Pressure Dx',
            'No Static Pressure Reset Dx',
            'Supply-air Temperature Set Point Control Loop Dx', //'Supply-air Temperature Control Loop Dx',
            'Low Supply-air Temperature Dx',
            'High Supply-air Temperature Dx',
            'No Supply-air Temperature Reset Dx',
            'Operational Schedule Dx'
        ];

        var diagnosticList = null;
        var diagnosticList2 = null; //For display purpose
        var foundDiagnosticList = false;
        // For the purpose of deciding which Rcx is running
        //if (data.length < 1) {
        if ($.isEmptyObject(data)) {
            console.log("No data received.");
            return false; //No Data
        }
        for (var i = 0; i < data.length && !foundDiagnosticList; i++) {
            if (data[i] == null) { continue; }
            if (econDiagnosticList.indexOf(data[i].diagnostic_name) > -1) {
                diagnosticList = econDiagnosticList2;
                diagnosticList2 = econDiagnosticList2;
                foundDiagnosticList = true;
            }
            if (hwDiagnosticList.indexOf(data[i].diagnostic_name) > -1) {
                diagnosticList = hwDiagnosticList;
                diagnosticList2 = hwDiagnosticList;
                foundDiagnosticList = true;
            }
            if (arDiagnosticList.indexOf(data[i].diagnostic_name) > -1) {
                diagnosticList = arDiagnosticList2;
                diagnosticList2 = arDiagnosticList2;
                foundDiagnosticList = true;
            }
        }

        var margin = {top: 40, right: 0, bottom: 150, left: 384}; //margin of the actual plot
        var padding = {top: 30, right: 30, bottom: 50, left: 30}; //padding of the actual plot
        var containerWidth = 1004; //$(container_class).width();
        var containerHeight = 50 * diagnosticList.length + margin.top
            + padding.top + margin.bottom
            + padding.bottom; //$(container_class).height();
        var width = containerWidth - margin.left - margin.right;
        var height = containerHeight - margin.top - margin.bottom;
        var radius = 8;
        var ref_stroke_clr = "#ccc";
        var format = d3.time.format("%b %d");//d3.time.format("%m/%d/%y");

        var yAxisLabels = diagnosticList;
        var legends = {
            "GREY": {
                value: "No Diagnosis",
                color: "#B3B3B3",
                state_value: 0,
                string: "GREY"
            },
            "GREEN": {
                value: "Normal",
                color: "#509E7A",
                state_value: 1,
                string: "GREEN"
            },
            "RED": {
                value: "Fault",
                color: "#E22400",
                state_value: 2,
                string: "RED"
            }
        };
        var yCategories = diagnosticList2;
        //var y2Categories = diagnosticList2;
        var svg = d3.select(document.createElementNS('http://www.w3.org/2000/svg', 'svg'))
            .attr("width", containerWidth)
            .attr("height", containerHeight);

        //Adjust timezone before doing aggregation
        data.forEach(function(d) {
            var ts = new Date(d.datetime);
            d.date = ts;
//            if (timezone != 0) {//Not UTC
//                var tzOffset = timezone;
//                if (d.date.dst()) {
//                    tzOffset += 1;
//                }
//                d.date.setHours(d.date.getHours() + tzOffset);
//            }
        });

        var sample_data = afddAggregateData(data, legends, diagnosticList);
        var xDomain = d3.extent(sample_data, function(d) { return d.date; });
        var items_per_dayCol = yAxisLabels.length;
        var items_per_viewport = 7;
        var inline_padding = Math.floor((width-padding.left-padding.right)/items_per_viewport);
        var plot_width = inline_padding * (sample_data.length/items_per_dayCol);

        var xScale = d3.time.scale()
                .domain(xDomain)
                .range([padding.left, padding.left + plot_width]); //~70
        var yScale = d3.scale.ordinal()
                .domain(yCategories)
                //.rangeRoundBands([0, height], .1);
                .rangePoints([height - padding.top, padding.bottom ]);

        //Create axises
        var xAxis = d3.svg.axis()
                .scale(xScale)
                .orient("bottom")
                .ticks(d3.time.day)
                .tickFormat(format);

        var yAxis = d3.svg.axis()
                    .scale(yScale)
                    .orient("left");

        var zoom = d3.behavior.zoom()
                .scaleExtent([1, 1])
                .on("zoom", zoomed);
        zoom.x(xScale);

        var plot_area = svg
            .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
            .on('mousedown', function(d) {
                d3.select("#hrData").remove();
            });

        plot_area.append("rect")
                .attr("class", "pane")
                .attr("width", width)
                .attr("height", height)
                .call(zoom);

        //Tooltip
        var tip = d3.tip()
            .attr('class', 'd3-tip')
            .offset([-10, 0])
            .html(function(d) {
                return "Date: <strong>" + formatFullDate(d.date) + "</strong><br/>" +
                    "Diagnostic Message: <strong>" + d.diagnostic_message + "</strong>" + "</strong><br/>" +
                    "Energy Impact: <strong>" + d.energy_impact + "</strong>" + "</strong><br/>" + //d.energy_impact
                    "(Click to see hourly result)<br/>";
            });
        var hrTip = d3.tip()
            .attr('class', 'd3-tip')
            .offset([-10, 0])
            .html(function(d) {
                return "Date: <strong>" + formatFullDate(d.date) + "</strong><br/>" +
                    "Hour: <strong>" + (d.y+1) + "</strong><br/>" +
                    "Diagnostic Message: <strong>" + d.diagnostic_message + "</strong>" + "</strong><br/>" +
                    "Energy Impact: <strong>" + d.energy_impact + "</strong>" + "</strong><br/>"; //d.energy_impact
            });
        plot_area.call(tip);
        plot_area.call(hrTip);

        //Legends
        var legend_svg = svg.append("g")
                .attr("transform", "translate(" + containerWidth/3 + "," + margin.top/3 + ")");
        var legend_width = 450;
        var legend_height = 34;
        var lpadding = 15;
        legend_svg.append("rect")
            .attr("width", legend_width)
            .attr("height", legend_height)
            .attr("x",0)
            .attr("y",0)
            .attr("rx",5)
            .attr("ry",5)
            .style("stroke","#909090")
            .style("stroke-width",1)
            .style("fill","none");

        var lx = lpadding;
        var arrLegends = [];
        for (var k in legends) {
            if (legends.hasOwnProperty(k)) {
                arrLegends.push(legends[k]);
            }
        }

        var litem = legend_svg.selectAll("g")
                .data(arrLegends)
                .enter()
                .append("g")
                .attr("transform", function(d,i) {
                    if (i>0) {
                        var circle_width = radius * 2;
                        var text_width = getTextWidth(arrLegends[i-1].value, "17pt sans-serif");
                        lx += circle_width + text_width +30;
                    }
                    return "translate("+ lx + "," + legend_height/2 + ")";
                });
        litem.append("circle")
            .attr("cx", 0)
            .attr("cy", 0)
            .attr("r", radius)
            .attr("fill", function(d) {
                return d.color;
            })
            .attr("opacity", 1)
            .on('mouseover', null)
            .on('mouseout', null);
        litem.append("text")
                .attr("x", radius*2+1)
                .attr("y", 0)
                .attr("dy", ".35em")
                .text(function(d) { return d.value; })
                .style("font-size","1em")
                .style("font-family","sans-serif");

        //Draw axises
        var xAxisEle = plot_area.append("g")
            .attr("id", "xAxisEle_AFDD")
            .attr("class", "x axis");
        xAxisEle.attr("clip-path","url(#clip_AFDD)")
            .attr("transform", "translate(0," + (height-5) + ")");

        plot_area.append("g")
            .attr("class", "y axis");

        //Draw y-grid lines for referencing
        plot_area.selectAll("line.y")
                .data(yCategories)
                .enter().append("line")
                .attr("class", "yAxis")
                .attr("x1", 0)
                //.attr("x2", width)
                .attr("x2", plot_width)
                .attr("y1", yScale)
                .attr("y2", yScale)
                .style("stroke", ref_stroke_clr);


        //Clip area
        plot_area.append("clipPath")
                .attr("id", "clip_AFDD")
                .append("rect")
                .attr("x", 0)
                .attr("y", 0)
                .attr("width", width)
                .attr("height", height);

        var radians = 2 * Math.PI, points = 20;
        var angle = d3.scale.linear()
                .domain([0, points-1])
                .range([0, radians]);

        var line = d3.svg.line.radial()
                .interpolate("basis")
                .tension(0)
                .radius(radius)
                .angle(function(d, i) { return angle(i); });

        var clip_area = plot_area.append("g")
                .attr("clip-path","url(#clip_AFDD)");

        clip_area.selectAll("circle")
            .data(sample_data)
            .enter()
            .append("circle")
            .attr("cx", function (d) {
                return xScale(d.date);
            })
            .attr("cy", function (d) {
                return yScale(yCategories[d.y]);
            })
            .attr("r", radius)
            .attr("fill", function(d) {
                return legends[d.state].color;
            })
            .attr("opacity", 1)
            .on('mouseover', tip.show)
            .on('mouseout', tip.hide)
            .on('mousedown', function(d) {
                d3.select("#hrData").remove();
                if (d.diagnostic === "No Supply-air Temperature Reset Dx" ||
                    d.diagnostic === "No Static Pressure Reset Dx" ||
                    d.diagnostic === "") {
                    return;
                }
                //DetailedArea has 3 <g> elements: border, label, hrDataArea (actual drawing)
                var rectWidth = 24;
                var yDomainData = makeArray(1,24);
                var hrScale = d3.scale.ordinal()
                        .domain(yDomainData)
                        .rangeRoundBands([0, 24*rectWidth]);
                var hrAxis = d3.svg.axis()
                        .scale(hrScale)
                        .orient("bottom");
                var drawPosition = margin.left + 25;

                var hrDataArea = svg
                    .append("g")
                    .attr("id", "hrData")
                    .attr("width", 24*rectWidth)
                    .attr("height", rectWidth)
                    .attr("transform", "translate(0," + (height+100) + ")");

                hrDataArea.append("g")
                    .attr("class", "x axis")
                    .attr("transform", "translate(" + drawPosition + ","+ (rectWidth) +")")
                    .call(hrAxis);

                var hrLabelArea = hrDataArea.append("g")
                    .attr("class", "axis");
                hrLabelArea.append("text")
                    .attr("x", 80)
                    .attr("y", rectWidth-7)
                    .text(diagnosticList[d.y]);
                hrLabelArea.append("text")
                    .attr("x", 80)
                    .attr("y", rectWidth+20)
                    .text('(' + formatFullDate(d.date) + ')');

                hrDataArea.selectAll("rect")
                .data(d.hourly_result)
                .enter()
                .append("rect")
                .attr("x", function (d) {
                    return d.y*rectWidth + drawPosition;
                })
                .attr("y", 0)
                .attr("width", rectWidth)
                .attr("height", rectWidth)
                .attr("fill", function(d) {
                    return legends[d.state].color;
                })
                .attr("opacity", 1)
                .style({"stroke-width": 1, "stroke": "black"})
                .on('mouseover', hrTip.show)
                .on('mouseout', hrTip.hide);

//                var plot_width =
//                hrDataArea
//                    .append("g")
//                    .attr("class", "hour-data-border")
//                    .attr("width", containerWidth)
//                    .attr("height", rectWidth + 50)
//                    .attr("transform", "translate(0," + (-14) + ")")
//                    .append("rect")
//                    .attr("width", containerWidth)
//                    .attr("height", rectWidth + 50)
//                    .attr("x", 2)
//                    .attr("y", 0)
//                    .attr("fill", "transparent")
//                    .style({"stroke-width": 1, "stroke": "grey"});

                var borderWidth = 2;
                var target = d3.event.target || d3.event.srcElement;
                var pos = d3.mouse(this);
                var x = parseFloat(target.getAttribute("cx")) || pos[0];
                x = x + margin.left;
                var x0 = borderWidth;
                var xn = containerWidth-borderWidth;
                var y1 = -20;
                var y2 = -5;
                var y3 = rectWidth*2+20;
                var tilt1 = 16;
                var tilt2 = 16;

                if (x>xn-tilt2) {
                    tilt1 = 50;
                    tilt2 = -18;
                }
                var posArr = [x+" "+y1, (x-tilt1)+" "+y2, x0+" "+y2, x0 + " "+y3,
                    xn+" "+y3, xn+" "+y2, (x+tilt2)+" "+y2, x+" "+y1];
                var noteStr = posArr.join(" L ");//lineto
                hrDataArea
                    .append("g")
                    .attr("class", "hour-data-border")
                    .attr("width", containerWidth)
                    .attr("height", rectWidth + 50)
                    .attr("transform", "translate(0," + (-14) + ")")
                    .append("path")
                    .attr("d", "m " + noteStr + " z")
                    .style({"stroke-width": borderWidth, "stroke": "green"})
                    .style("fill", "none");
                d3.event.stopPropagation();
            });
        zoomed();

        return svg[0];

        function zoomed() {
            plot_area.select("g.x.axis").call(xAxis);
            plot_area.select("g.y.axis").call(yAxis);
            //plot_area.select("g.y2.axis").call(yAxis2);

            clip_area.selectAll("circle").attr("cx", function(d) {
                var value = xScale(d.date);
                if (value < 0) value = -10000;
                //if (value > width) value = 10000;
                return value;
            });
        }

        function getTextWidth(text, font) {
            // re-use canvas object for better performance
            var canvas = getTextWidth.canvas || (getTextWidth.canvas = document.createElement("canvas"));
            var context = canvas.getContext("2d");
            context.font = font;
            var metrics = context.measureText(text);
            return metrics.width;
        };
    }

    function getSelectOptions(isTopic){
        var site = $('#site').val();
        var building = $('#building').val();
        var device = $('#device').val();
        var diagnostic = $('#diagnostic').val();
        if (isTopic) {
            return [diagnostic,site,building,device];
        }
        return [site,building,device,diagnostic];
    }

    function setSelectOptions(values, id) {
        $('#'+id).children('option').remove();
        $.each(values, function(key, value) {
            $('#'+id).append($("<option></option>").attr("value",value).text(value));
        });
        $('#'+id).change();
    }

    function uiResultLoading() {
        $("body").addClass("loading");
        $('#retro-commissioning-afdd').html('');
    }

    function uiResultLoaded(resp) {
        //console.log(json);
        $("body").removeClass("loading");
        var timezone = parseInt($("#timezone").val());
        timezone = 0; //disable timezone for now
        var svg = retroCommissioningAFDDSVG(resp,timezone);
        $('#retro-commissioning-afdd').append(svg);
    }

    function get2DigitValue(value) {
        if (value<10) value = '0' + value;
        return value;
    }

    function getTsFormat(d){
        var dd = get2DigitValue(d.getDate());
        var MM = get2DigitValue(d.getMonth() + 1);
        var yy = d.getFullYear();

        var hh = get2DigitValue(d.getHours());
        var mm = get2DigitValue(d.getMinutes());
        var ss = get2DigitValue(d.getSeconds());

        var fullDatePart = [yy, MM, dd].join('-');
        var fullTimePart = [hh, mm, ss].join(':');

        return [fullDatePart, fullTimePart].join(' ');
    }

    function loadData() {
        var dxPath = getSelectOptions().join(cacheDataPathSep);
        var baseTopic = getSelectOptions(true).join('/');
        if (!cacheData.hasOwnProperty(dxPath)) {
            isLoading = true;
            var token = $('#token').val();
            var req_method = 'platform.historian.query';
            var count = 2000000;
            var order = 'LAST_TO_FIRST';
            var today = new Date();
            var end_time = getTsFormat(today);
            var start_time = new Date(today.getFullYear(), today.getMonth(), today.getDate() - 8);
            start_time = getTsFormat(start_time);

            if ($("#start-date").datepicker("getDate") != null) {
                start_time = $("#start-date").datepicker("getDate").getTime();
                start_time = new Date(start_time);
                start_time = getTsFormat(start_time);
            }
            if ($("#end-date").datepicker("getDate") != null) {
                end_time = $("#end-date").datepicker("getDate").getTime();
                end_time = new Date(end_time);
                end_time = getTsFormat(end_time);
            }
            var dx = $('#diagnostic').val();
            noOfReq = algo_set[dx].length*points_available[dx].length;
            var dataload = {
                'topic': '',
                'start': start_time,
                'end': end_time,
                'count': count,
                'order': order
            };
            var pdata = {
                jsonrpc: '2.0',
                method: req_method,
                params: dataload,
                authorization: token,
                id: req_id
            };
            var topicArr = [];
            algo_set[dx].forEach(function(algo){
                points_available[dx].forEach(function(point) {
                    topicArr.push([prefix,baseTopic, algo, point].join('/'));
                });
            });

            dataload.topic = topicArr;
            pdata.params = dataload;
            $.ajax({
                type: 'POST',
                url: vc_server + '/jsonrpc',
                data: JSON.stringify(pdata),
                dataType: 'json',
                success: function(data){
                    //parse data to conform w/ the old way of data handling
                    if (data['result'].hasOwnProperty('values')) {
                        $.each(data['result']['values'], function (key, value) {
                            preCacheData[key] = value;
                            //console.log(key);
                            //console.log(JSON.stringify(value));
                        });
                    }
                },
                error: function (jqXHR, textStatus) {
                    console.log("Ajax loading failed: " + textStatus);
                    //updatePendingRequests();
                },
                complete: function () {
                    updatePendingRequests();
                }
            });

        }
    }

    function updatePendingRequests() {
        noOfReq--;
        //if (noOfReq == 0) {
            for (var topic in preCacheData){
                // skip loop if the property is from prototype
                if (!preCacheData.hasOwnProperty(topic)) continue;
                var topicData = preCacheData[topic];
                var arr = topic.split('/');
                start_idx = 0;
                if (prefix != '') {
                    start_idx = 1;
                }
                var dx = arr[start_idx];
                var site = arr[start_idx+1];
                var building = arr[start_idx+2];
                var device = arr[start_idx+3];
                var algo = arr[start_idx+4];
                var point = arr[start_idx+5];

                if (point === 'diagnostic message'){
                    var algo_path = getSelectOptions().join(cacheDataPathSep);
                    if (!cacheData.hasOwnProperty(algo_path)){
                        cacheData[algo_path] = [];
                    }
                    //if (topicData['result'].hasOwnProperty('values')){
                        //topicData['result']['values'].forEach(function(dx_msg){
                        var curEIPointer = 0;
                        topicData.forEach(function(dx_msg){
                            var newItem = {};
                            var sensLvl = $("#sensitive").val();
                            var err_code = dx_msg[1][sensLvl]; //dx_msg[1].toString();
                            newItem['diagnostic_name'] = algo;
                            newItem['datetime'] = dx_msg[0];
                            newItem['error_code'] = err_code;
                            newItem['color_code'] = color_codes[dx][err_code];
                            newItem['diagnostic_message'] = error_messages[dx][err_code];
                            var ei_topic = [prefix,dx,site,building,device,algo,'energy impact'].join('/');
                            if (preCacheData.hasOwnProperty(ei_topic)) {
                                for (; curEIPointer < preCacheData[ei_topic].length; curEIPointer++)
                                {
                                    var ei_msg = preCacheData[ei_topic][curEIPointer];
                                    eiParts = ei_msg[0].split(':');
                                    dxParts = dx_msg[0].split(':');
                                    if (eiParts[0]===dxParts[0] && //up to hour
                                        eiParts[1]===dxParts[1]) { //minute
                                        newItem['energy_impact'] = ei_msg[1];
                                        break;
                                    }
                                }
                            }
                            cacheData[algo_path].push(newItem);
                        });
                    //}
                }
            }

            //Done
            isLoading = false;
        //}
    }

    function getItem(values, itemName) {
        var found = values.filter(function(item) {
            return item.name === itemName;
        });
        return found[0];
    }

    function initUI() {
        //UI components
        $('#start-date').datepicker();
        $('#end-date').datepicker();
        $('#afdd-body').hide();
    }

    function find_arr_item_by_name(arr, itemName) {
        var item = null;
        for (i = 0; i < arr.length; i++) {
            if (arr[i]['name'] === itemName) {
                item = arr[i];
                break;
            }
        }
        return item;
    }

    function find_arr_item(arr, itemName) {
        var item = null;
        for (i = 0; i < arr.length; i++) {
            if (arr[i] === itemName) {
                item = arr[i];
                break;
            }
        }
        return item;
    }

    function query_topics() {
        var air_rcx_prefix = [prefix, air_rcx].join('/');
        var econ_rcx_prefix = [prefix, econ_rcx].join('/');
        var token = $('#token').val();
        var post_data = {
            jsonrpc: '2.0',
            method: 'platform.historian.get_topic_list',
            authorization: token,
            id: req_id
        };
        $.ajax({
                type: 'POST',
                url: vc_server + '/jsonrpc',
                data: JSON.stringify(post_data),
                dataType: 'json',
                success: function(data){
                    if (data.hasOwnProperty('result')) {
                        var siteDetails = [];
                        $.each(data['result'], function (key, value) {
                            if (value.startsWith(econ_rcx_prefix)
                                || value.startsWith(air_rcx_prefix)) {
                                console.log(value);
                                var start_idx = 0;
                                if (prefix != '') {
                                    start_idx = 1;
                                }
                                topic_parts = value.split('/');
                                var appName = topic_parts[start_idx];
                                var siteName = topic_parts[start_idx+1];
                                var buildingName = topic_parts[start_idx+2];
                                var deviceName = topic_parts[start_idx+3];
                                var algoName = topic_parts[start_idx+4];

                                var site = find_arr_item_by_name(siteDetails, siteName);
                                if (site == null) {
                                    site = {
                                        'name': siteName,
                                        'buildings': []
                                    }
                                    siteDetails.push(site);
                                }
                                var building = find_arr_item_by_name(site['buildings'], buildingName);
                                if (building == null) {
                                    building = {
                                        'name': buildingName,
                                        'devices': []
                                    }
                                    site['buildings'].push(building);
                                }
                                var device = find_arr_item_by_name(building['devices'], deviceName);
                                if (device == null) {
                                    device = {
                                        'name': deviceName,
                                        'dx': []
                                    }
                                    building['devices'].push(device);
                                }
                                var dx = find_arr_item(device['dx'], appName);
                                if (dx == null) {
                                    device['dx'].push(appName);
                                }
                            }
                        });
                        siteObjs = siteDetails;
                    }
                },
                error: function (jqXHR, textStatus) {
                    console.log("Ajax loading failed: " + textStatus);
                },
                complete: function () {
                    var sites = [];
                    siteObjs.forEach(function(siteObj){
                        sites.push(siteObj.name);
                    });
                    setSelectOptions(sites, 'site');
                    $("body").removeClass("loading");
                }
            });


    }


    function start_diagnostic() {
        $('#login-form').hide();
        $('#afdd-body').show();
        $("body").addClass("loading");
        //Behaviours
        query_topics();
    }

    //Event processing
    $('#login_btn').click(function(){
        var username = $('#username').val();
        var password = $('#password').val();
        if (!username || !password){
            alert('Enter username and password');
            return;
        }
        var pdata = {
             jsonrpc: '2.0',
             method: "get_authorization",
             params: {
                username: username,
                password: password
             },
             id: req_id
        };
        $.ajax({
            type: 'POST',
            url: vc_server + '/jsonrpc',
            data: JSON.stringify(pdata),
            dataType: 'json',
            success: function(data){
                $('#token').val(data.result);
                if ($('#token').val() != '') {
                    start_diagnostic();
                }
            },
            failure: function(data){
                console.log(JSON.stringify(data));
            }
        });
    });
    $("#btn_view_result").click(function() {
        console.log("New run at " + getTsFormat((new Date())));
        uiResultLoading();

        cacheData = {}; //new version no cache
        preCacheData ={};
        queue = [];
        loadData();
        var path = getSelectOptions().join(cacheDataPathSep);
        checkCacheData = setInterval(function () {
            //if(cacheData.hasOwnProperty(path)) {
                if (!isLoading){
                    uiResultLoaded(cacheData[path]);
                    clearInterval(checkCacheData);
                }
            //}
        }, 200);
    });

    $("#site").bind('change', (function(){
        var buildings = [];
        var siteName = $("#site").val();
        var siteObj = getItem(siteObjs, siteName);
        var buildingObjs = siteObj.buildings;
        buildingObjs.forEach(function(buildingObj){
            buildings.push(buildingObj.name)
        })
        setSelectOptions(buildings, 'building');
    }));
    $("#building").bind('change', (function(){
        var devices = []
        var siteName = $("#site").val();
        var buildingName = $("#building").val();
        var siteObj = getItem(siteObjs, siteName);
        var buildingObj = getItem(siteObj.buildings, buildingName);
        var deviceObjs = buildingObj.devices
        deviceObjs.forEach(function(d){
            devices.push(d.name);
        })
        setSelectOptions(devices, 'device');
    }));
    $("#device").bind('change', (function(){
        var siteName = $("#site").val();
        var buildingName = $("#building").val();
        var deviceName = $("#device").val();
        var siteObj = getItem(siteObjs, siteName);
        var buildingObj = getItem(siteObj.buildings, buildingName);
        var deviceObj = getItem(buildingObj.devices, deviceName);
        setSelectOptions(deviceObj.dx, 'diagnostic');
    }));

    sens = ["normal", "high", "low"];
    setSelectOptions(sens, 'sensitive');
    $("#sensitive").bind('change', (function() {
        var path = getSelectOptions().join(cacheDataPathSep);
        uiResultLoading();
        checkCacheData = setInterval(function () {
            cacheData = {};
            updatePendingRequests();
            uiResultLoaded(cacheData[path]);
            clearInterval(checkCacheData);
        }, 200);
    }));

    //Start the app
    initUI();

//    query(
//        'PNNL/BUILDING1/AHU2/CoolingPercent',
//        null,
//        20,
//        'LAST_TO_FIRST'
//    );

//    $(document).on({
//        ajaxStart: function() { $body.addClass("loading");    },
//        ajaxStop: function() { $body.removeClass("loading"); }
//    });
});
//TODO:
//Query all the data to the CSV to compare all three sensitive levels
//
//
//Round up to 1 digit after decimal point for dx message
//
//
//Check with Craig & update the data pulling code for Woohyun
//
//Sort the combobox names

