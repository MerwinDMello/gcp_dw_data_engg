create table if not exists {{ params.param_hr_core_dataset_name }}.bi_psat_dept_level_smry
(
  psat_dept_level_sk numeric not null,
  year_id int64,
  qtr_id int64,
  month_id int64,
  month_id_desc_l string,
  company_code string not null,
  coid string not null,
  parent_coid string,
  facility_claim_control_num string,
  facility_claim_control_num_name string,
  functional_dept_num string,
  dept_num string,
  survey_category_code string,
  survey_category_text string,
  survey_sub_category_text string,
  question_id string not null,
  question_short_name string,
  unique_record_text string,
  total_response_count_num int64,
  score_numerator_num int64,
  top_box_pct_num numeric(33, 4),
  source_system_code string not null,
  dw_last_update_date_time datetime not null
)
cluster by psat_dept_level_sk;