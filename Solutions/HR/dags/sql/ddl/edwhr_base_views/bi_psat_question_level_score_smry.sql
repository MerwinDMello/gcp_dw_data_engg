-- Translation time: 2023-04-25T18:33:50.275152Z
-- Translation job ID: 8b921d45-66b0-461e-880c-7106e48c9005
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/SARANYADEVI/missing_views/EDWHR_Base_Views/bi_psat_question_level_score_smry.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.bi_psat_question_level_score_smry AS SELECT
    bi_psat_question_level_score_smry.qtr_id,
    bi_psat_question_level_score_smry.corporate_code,
    bi_psat_question_level_score_smry.group_code,
    bi_psat_question_level_score_smry.division_code,
    bi_psat_question_level_score_smry.market_code,
    bi_psat_question_level_score_smry.company_code,
    bi_psat_question_level_score_smry.coid,
    bi_psat_question_level_score_smry.vendor_assigned_unit_text,
    bi_psat_question_level_score_smry.question_id,
    bi_psat_question_level_score_smry.qtr_desc_dss,
    bi_psat_question_level_score_smry.corporate_type_code,
    bi_psat_question_level_score_smry.corporate_name,
    bi_psat_question_level_score_smry.group_type_code,
    bi_psat_question_level_score_smry.group_name,
    bi_psat_question_level_score_smry.division_type_code,
    bi_psat_question_level_score_smry.division_name,
    bi_psat_question_level_score_smry.market_type_code,
    bi_psat_question_level_score_smry.market_name,
    bi_psat_question_level_score_smry.coid_type_code,
    bi_psat_question_level_score_smry.coid_name,
    bi_psat_question_level_score_smry.org_level_code,
    bi_psat_question_level_score_smry.survey_category_code,
    bi_psat_question_level_score_smry.domain_id,
    bi_psat_question_level_score_smry.domain_desc,
    bi_psat_question_level_score_smry.measure_id_text,
    bi_psat_question_level_score_smry.question_short_name,
    bi_psat_question_level_score_smry.question_desc,
    bi_psat_question_level_score_smry.aggregated_top_box_score_num,
    bi_psat_question_level_score_smry.survey_response_cnt,
    bi_psat_question_level_score_smry.respondent_cnt,
    bi_psat_question_level_score_smry.top_box_pct,
    bi_psat_question_level_score_smry.source_system_code,
    bi_psat_question_level_score_smry.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.bi_psat_question_level_score_smry
;