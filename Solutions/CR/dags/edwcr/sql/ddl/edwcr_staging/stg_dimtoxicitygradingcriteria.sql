CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_dimtoxicitygradingcriteria (
dimsiteid INT64
, dimtoxicitygradingcriteriaid INT64
, gs_author INT64
, eff_date STRING
, tr_typ STRING
, toxicitytypeenu STRING
, tr_comp_name STRING
, toxicitycomponentnameenu STRING
, tr_grade INT64
, tr_min_range STRING
, tr_max_range STRING
, upper_margin INT64
, lower_margin INT64
, trend INT64
, appr_flag STRING
, trans_log_userid STRING
, trans_log_tstamp STRING
, trans_log_inst_id STRING
, trans_log_muserid STRING
, trans_log_mtstamp STRING
, trans_log_minst_id STRING
, trans_trf_tstamp STRING
, bill_cd STRING
, bill_cd_typ_id INT64
, cls_scheme_id INT64
, grading_criteria_id INT64
, tr_grading_enter_desc STRING
, gradingdescenu STRING
, logid INT64
, runid INT64
, dw_last_update_date_time DATETIME
)
  ;
