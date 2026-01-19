CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.bi_psat_dept_level_smry AS SELECT
    bi_psat_dept_level_smry.psat_dept_level_sk,
    bi_psat_dept_level_smry.year_id,
    bi_psat_dept_level_smry.qtr_id,
    bi_psat_dept_level_smry.month_id,
    bi_psat_dept_level_smry.month_id_desc_l,
    bi_psat_dept_level_smry.company_code,
    bi_psat_dept_level_smry.coid,
    bi_psat_dept_level_smry.parent_coid,
    bi_psat_dept_level_smry.facility_claim_control_num,
    bi_psat_dept_level_smry.facility_claim_control_num_name,
    bi_psat_dept_level_smry.functional_dept_num,
    bi_psat_dept_level_smry.dept_num,
    bi_psat_dept_level_smry.survey_category_code,
    bi_psat_dept_level_smry.survey_category_text,
    bi_psat_dept_level_smry.survey_sub_category_text,
    bi_psat_dept_level_smry.question_id,
    bi_psat_dept_level_smry.question_short_name,
    bi_psat_dept_level_smry.unique_record_text,
    bi_psat_dept_level_smry.total_response_count_num,
    bi_psat_dept_level_smry.score_numerator_num,
    bi_psat_dept_level_smry.top_box_pct_num,
    bi_psat_dept_level_smry.source_system_code,
    bi_psat_dept_level_smry.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.bi_psat_dept_level_smry
;