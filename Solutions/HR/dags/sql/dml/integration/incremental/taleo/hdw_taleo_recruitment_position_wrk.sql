  /*  Generate the surrogate keys for Candidate */ --
call
  {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}',
    'ats_cust_position_stg',
    'position', 
    'RECRUITMENT_POSITION'); 

    /*  truncate worktable table     */
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.recruitment_position_wrk; 
  /*  Load Work Table with working Data */
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.recruitment_position_wrk (recruitment_position_sid,
    recruitment_position_num,
    gsd_pct,
    incentive_payout_pct,
    incentive_plan_name,
    incentive_plan_potential_pct,
    special_program_name,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CAST(xwlk.sk AS INT64) AS recruitment_position_sid,
  cast(stg.position AS INT64) AS recruitment_position_num,
  cast(stg.hcapositiongsdpercent as NUMERIC) AS gsd_pct,
  CAST(stg.hcapositionincentplantargetperc AS NUMERIC) AS incentive_payout_pct,
  stg.hcapositionincentiveplantype AS incentive_plan_name,
  CAST(stg.hcapositionincentplantargetperc AS NUMERIC) AS incentive_plan_potential_pct,
  stg.hcapositionspecialprogram AS special_program_name,
  'B' AS source_system_code,
  TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_position_stg AS stg  
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
    cast(stg.position as string) = xwlk.sk_source_txt
  AND UPPER(xwlk.sk_type) = 'RECRUITMENT_POSITION';
  
