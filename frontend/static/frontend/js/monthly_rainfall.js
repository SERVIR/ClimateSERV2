// MONTHLY ANALYSIS SUPPORT METHODS         START

function get_values_By_month(raw_data_obj, month, subtype, variable) {
    return raw_data_obj
        .MonthlyAnalysisOutput
        .dataset_info_list
        .filter(item => item.out_subTypeName == subtype)
        .map(x => {
            return x.avg_percentiles_dataLines[month - 1]
                [variable] == "nan"
                ? 0
                : parseFloat(x.avg_percentiles_dataLines[month - 1]
                    [variable]);
        });
}