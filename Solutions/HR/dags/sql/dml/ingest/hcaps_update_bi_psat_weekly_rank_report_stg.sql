BEGIN 
DECLARE tab STRING;
DECLARE sql_query STRING;
SET tab = @target;
SET sql_query = (SELECT FORMAT( '''
update `{{params.param_hr_stage_dataset_name}}.%s`
set dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'), SECOND), 
report_type_desc = TRIM(report_type_desc),
report_service_line_desc = TRIM(report_service_line_desc),
quarter_start_date = TRIM(quarter_start_date),
crnt_hca_rank_num = TRIM(crnt_hca_rank_num),
pg_rank_pct_num = TRIM(pg_rank_pct_num),
total_completed_surveys_amt = TRIM(total_completed_surveys_amt),
facility_id = TRIM(facility_id),
facility_name = TRIM(facility_name),
group_name = TRIM(group_name),
division_name = TRIM(division_name),
market_name = TRIM(market_name),
crnt_top_box_pct_num = TRIM(crnt_top_box_pct_num),
pr_top_box_pct_num = TRIM(pr_top_box_pct_num),
crnt_vs_pr_top_box_chng_amt = TRIM(crnt_vs_pr_top_box_chng_amt),
arrow1 = TRIM(arrow1),
pr_yr_top_box_pct_num = TRIM(pr_yr_top_box_pct_num),
crnt_vs_pr_yr_top_box_chng_amt = TRIM(crnt_vs_pr_yr_top_box_chng_amt),
arrow4 = TRIM(arrow4),
roll_top_box_pct_num = TRIM(roll_top_box_pct_num),
crnt_vs_roll_top_box_chng_amt = TRIM(crnt_vs_roll_top_box_chng_amt),
arrow3 = TRIM(arrow3),
pr_hca_rank_num = TRIM(pr_hca_rank_num),
crnt_vs_pr_hca_rank_chng_amt = TRIM(crnt_vs_pr_hca_rank_chng_amt),
arrow2 = TRIM(arrow2),
spacer11 = TRIM(spacer11),
spacer12 = TRIM(spacer12),
trend = TRIM(trend),
hca_rank_total_facility_amt = TRIM(hca_rank_total_facility_amt),
pg_total_facility_amt = TRIM(pg_total_facility_amt),
report_date = TRIM(report_date),
spacer1 = TRIM(spacer1),
spacer2 = TRIM(spacer2),
spacer3 = TRIM(spacer3),
spacer4 = TRIM(spacer4),
spacer5 = TRIM(spacer5),
spacer6 = TRIM(spacer6),
spacer7 = TRIM(spacer7),
spacer8 = TRIM(spacer8),
spacer9 = TRIM(spacer9),
spacer10 = TRIM(spacer10),
top_box_1 = TRIM(top_box_1),
pr_3_top_box_pct_num = TRIM(pr_3_top_box_pct_num),
pr_2_top_box_pct_num = TRIM(pr_2_top_box_pct_num),
top_box_4 = TRIM(top_box_4),
top_box_5 = TRIM(top_box_5),
benchmark_start_date = TRIM(benchmark_start_date),
report_data_level = TRIM(report_data_level),
survey_item_key = TRIM(survey_item_key),
survey_item_variable_name = TRIM(survey_item_variable_name),
survey_item_report_name = TRIM(survey_item_report_name)
where dw_last_update_date_time is null;''',tab ));
       
EXECUTE IMMEDIATE sql_query;

END;