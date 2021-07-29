// MONTHLY ANALYSIS SUPPORT METHODS         START

// These two are set by the monthly analysis 'submit data request'
// var monthlyRainfallAnalysis_Start_Date = "";  // expected format is:
// var monthlyRainfallAnalysis_End_Date = "";

function get_Year_From_YYYY_MM_DD_String(YYYY_MM_DD_String) {
    //str.split("_");
    var yearPart = (YYYY_MM_DD_String.split("_")[0] * 1);
    var monthPart = (YYYY_MM_DD_String.split("_")[1] * 1);
    var dayPart = (YYYY_MM_DD_String.split("_")[2] * 1);
    //var retDate = new Date(yearPart, monthPart - 1, dayPart);
    //return retDate;
    return yearPart;
}

function get_Month_From_YYYY_MM_DD_String(YYYY_MM_DD_String) {
    //str.split("_");
    var yearPart = (YYYY_MM_DD_String.split("_")[0] * 1);
    var monthPart = (YYYY_MM_DD_String.split("_")[1] * 1);
    var dayPart = (YYYY_MM_DD_String.split("_")[2] * 1);
    //var retDate = new Date(yearPart, monthPart - 1, dayPart);
    //return retDate;
    return monthPart;
}

// monthNumberString is a value between "1" and "12"  ("1" == Jan)
function get_category_month_name_for_monthNumberString(monthNumberString) {
    if (monthNumberString == "1") {
        return "Jan";
    }
    if (monthNumberString == "2") {
        return "Feb";
    }
    if (monthNumberString == "3") {
        return "Mar";
    }
    if (monthNumberString == "4") {
        return "Apr";
    }
    if (monthNumberString == "5") {
        return "May";
    }
    if (monthNumberString == "6") {
        return "June";
    }
    if (monthNumberString == "7") {
        return "July";
    }
    if (monthNumberString == "8") {
        return "Aug";
    }
    if (monthNumberString == "9") {
        return "Sept";
    }
    if (monthNumberString == "10") {
        return "Oct";
    }
    if (monthNumberString == "11") {
        return "Nov";
    }
    if (monthNumberString == "12") {
        return "Dec";
    }
    return "unknown";
}

// Gets the list of Index items that contain Seasonal_Forecast data
function monthlyRainfall_Analysis__Get_SeasonalForecast_IndexList(raw_data_obj) {
    var ret_index_list = [];
    for (var i = 0; i < raw_data_obj.MonthlyAnalysisOutput.dataset_info_list.length; i++) {
        var current_DatasetItem = raw_data_obj.MonthlyAnalysisOutput.dataset_info_list[i];
        if (current_DatasetItem.out_subTypeName == "SEASONAL_FORECAST") {
            ret_index_list.push(i);
        }
    }
    return ret_index_list;
}

// Gets the index which contains CHIRPS data
function monthlyRainfall_Analysis__Get_Chirps_Index(raw_data_obj) {
    for (var i = 0; i < raw_data_obj.MonthlyAnalysisOutput.dataset_info_list.length; i++) {
        var current_DatasetItem = raw_data_obj.MonthlyAnalysisOutput.dataset_info_list[i];
        if (current_DatasetItem.out_subTypeName == "CHIRPS_REQUEST") {
            return i;
        }
    }
    return -1;
}


// If there is ever an issue with mis-matching monthStrings and actual month values used,
// // the way to fix this is to also pass in the dataset index, and then iterate through that, then find the correct month index by checking the column which has the month number in it.
// This may be caused by a sort order issue.. at this time there is no reason to believe the sorting would ever be off... (since the server always returns 12 months worth of data)
function get_MonthIndex_from_MonthString(monthString, raw_data_obj) {
    // If there is trouble with this first section (I.E getting the wrong data, we may need to match month numbers)
    // Convert monthString to month_index
    // monthString is a string (that is a number) from "1" to "12", ("1" is Jan, "2" is Feb, etc)
    // month_index is a number from 0-11  (0 is Jan, 1 is Feb, etc)
    var month_index = (monthString * 1) - 1;   // Converts a "3", which is march to the number 2, which is the index inside the dataset object for the month of march data.
    return month_index;
}

// Compute a monthly average of all seasonal forecast datasets for any given month.
// Usage Examples
// monthlyRainfall_Analysis__Compute_SeasonalForecast_Average_ForMonth(raw_data_obj, "1"); // Seasonal Forecase Ensemebles average of averages for JAN
// monthlyRainfall_Analysis__Compute_SeasonalForecast_Average_ForMonth(raw_data_obj, "2"); // Seasonal Forecase Ensemebles average of averages for FEB
// monthlyRainfall_Analysis__Compute_SeasonalForecast_Average_ForMonth(raw_data_obj, "5"); // Seasonal Forecase Ensemebles average of averages for MAY
function monthlyRainfall_Analysis__Compute_SeasonalForecast_Average_ForMonth(raw_data_obj, monthString) {

    braw.push(raw_data_obj);

    // If there is trouble with this first section (I.E getting the wrong data, we may need to match month numbers)
    // Convert monthString to month_index
    // monthString is a string (that is a number) from "1" to "12", ("1" is Jan, "2" is Feb, etc)
    // month_index is a number from 0-11  (0 is Jan, 1 is Feb, etc)
    //var month_index = (monthString * 1) - 1;   // Converts a "3", which is march to the number 2, which is the index inside the dataset object for the month of march data.
    var month_index = get_MonthIndex_from_MonthString(monthString, raw_data_obj);

    // Get the full list of averages for all ensembles for a given month
    var indexList_for_SeasonalForecast_Datasets = monthlyRainfall_Analysis__Get_SeasonalForecast_IndexList(raw_data_obj);
    var singleMonth_SeasonalForecast_List_Of_Averages = [];  // List of all the averages for March (for example) for ALL ensembles.
    for (var i = 0; i < indexList_for_SeasonalForecast_Datasets.length; i++) {
        var current_dataset_index = indexList_for_SeasonalForecast_Datasets[i];
        var theAverage = raw_data_obj.MonthlyAnalysisOutput.dataset_info_list[current_dataset_index].avg_percentiles_dataLines[month_index].col02_MonthlyAverage == "nan" ? 0 : raw_data_obj.MonthlyAnalysisOutput.dataset_info_list[current_dataset_index].avg_percentiles_dataLines[month_index].col02_MonthlyAverage;
        var col02_MonthlyAverage = theAverage * 1;
        singleMonth_SeasonalForecast_List_Of_Averages.push(col02_MonthlyAverage);
        // raw_data_obj.MonthlyAnalysisOutput.dataset_info_list[0].avg_percentiles_dataLines[2] // March
        //avg_percentiles_dataLines
    }
    monthlies.push(singleMonth_SeasonalForecast_List_Of_Averages);
    // Compute the average of all the averages.
    var sum_of_averages = 0;
    var itemCount = 0;
    for (var j = 0; j < singleMonth_SeasonalForecast_List_Of_Averages.length; j++) {
        sum_of_averages = sum_of_averages + singleMonth_SeasonalForecast_List_Of_Averages[j];
        itemCount = itemCount + 1;
    }

    // don't divide by 0!
    if (itemCount < 1) {
        itemCount = 1
    }
    var finalAverage = (sum_of_averages / itemCount);

    return finalAverage;

}

var monthlies = [];
var braw = [];

// Gets the LongTermAverage value from the CHIRPS dataset for any given month.
// monthlyRainfall_Analysis__Get_Chirps_LongTermAverage_ForMonth(raw_data_obj, "5");  // MAY, ChirpsDataset - Col02_MonthlyAvg
function monthlyRainfall_Analysis__Get_Chirps_LongTermAverage_ForMonth(raw_data_obj, monthString) {
    var month_index = get_MonthIndex_from_MonthString(monthString, raw_data_obj);
    var chirps_dataset_index = monthlyRainfall_Analysis__Get_Chirps_Index(raw_data_obj);
    var col02_MonthlyAverage = raw_data_obj.MonthlyAnalysisOutput.dataset_info_list[chirps_dataset_index].avg_percentiles_dataLines[month_index].col02_MonthlyAverage * 1;
    return col02_MonthlyAverage;
}

// Gets the LongTermAverage value from the CHIRPS dataset for any given month.
// monthlyRainfall_Analysis__Get_Chirps_25thPercentile_ForMonth(raw_data_obj, "5");  // MAY, ChirpsDataset - Col03_25thPercentile
function monthlyRainfall_Analysis__Get_Chirps_25thPercentile_ForMonth(raw_data_obj, monthString) {
    var month_index = get_MonthIndex_from_MonthString(monthString, raw_data_obj);
    var chirps_dataset_index = monthlyRainfall_Analysis__Get_Chirps_Index(raw_data_obj);
    var col03_25thPercentile = raw_data_obj.MonthlyAnalysisOutput.dataset_info_list[chirps_dataset_index].avg_percentiles_dataLines[month_index].col03_25thPercentile * 1;
    return col03_25thPercentile;
}

// Gets the LongTermAverage value from the CHIRPS dataset for any given month.
// monthlyRainfall_Analysis__Get_Chirps_75thPercentile_ForMonth(raw_data_obj, "5");  // MAY, ChirpsDataset - Col04_75thPercentile
function monthlyRainfall_Analysis__Get_Chirps_75thPercentile_ForMonth(raw_data_obj, monthString) {
    var month_index = get_MonthIndex_from_MonthString(monthString, raw_data_obj);
    var chirps_dataset_index = monthlyRainfall_Analysis__Get_Chirps_Index(raw_data_obj);
    var col04_75thPercentile = raw_data_obj.MonthlyAnalysisOutput.dataset_info_list[chirps_dataset_index].avg_percentiles_dataLines[month_index].col04_75thPercentile * 1;
    return col04_75thPercentile;
}