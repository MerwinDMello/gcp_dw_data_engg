/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.bi_psat_weekly_rank_report AS SELECT
    bi_psat_weekly_rank_report.rank_report_sk,
    bi_psat_weekly_rank_report.report_type_desc,
    bi_psat_weekly_rank_report.report_data_level,
    bi_psat_weekly_rank_report.group_name,
    bi_psat_weekly_rank_report.division_name,
    bi_psat_weekly_rank_report.market_name,
    bi_psat_weekly_rank_report.facility_name,
    bi_psat_weekly_rank_report.facility_id,
    bi_psat_weekly_rank_report.report_service_line_desc,
    bi_psat_weekly_rank_report.quarter_start_date,
    bi_psat_weekly_rank_report.total_completed_surveys_amt,
    bi_psat_weekly_rank_report.crnt_top_box_pct_num,
    bi_psat_weekly_rank_report.pr_top_box_pct_num,
    bi_psat_weekly_rank_report.crnt_vs_pr_top_box_chng_amt,
    bi_psat_weekly_rank_report.pr_2_top_box_pct_num,
    bi_psat_weekly_rank_report.pr_3_top_box_pct_num,
    bi_psat_weekly_rank_report.pr_yr_top_box_pct_num,
    bi_psat_weekly_rank_report.crnt_vs_pr_yr_top_box_chng_amt,
    bi_psat_weekly_rank_report.roll_top_box_pct_num,
    bi_psat_weekly_rank_report.crnt_vs_roll_top_box_chng_amt,
    bi_psat_weekly_rank_report.hca_rank_total_facility_amt,
    bi_psat_weekly_rank_report.crnt_hca_rank_num,
    bi_psat_weekly_rank_report.pr_hca_rank_num,
    bi_psat_weekly_rank_report.crnt_vs_pr_hca_rank_chng_amt,
    bi_psat_weekly_rank_report.pg_total_facility_amt,
    bi_psat_weekly_rank_report.pg_rank_pct_num,
    bi_psat_weekly_rank_report.report_date,
    bi_psat_weekly_rank_report.benchmark_start_date,
    bi_psat_weekly_rank_report.source_system_code,
    bi_psat_weekly_rank_report.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.bi_psat_weekly_rank_report
;
