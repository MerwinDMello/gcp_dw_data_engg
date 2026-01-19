/*  Generate the surrogate keys for Offer */
 CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'taleo_offer', 'Coalesce(TRIM(CAST(offer_number AS STRING)), \' \')', 'OFFER');

/*  Truncate Worktable Table     */
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.offer_wrk;

/*  Load Work Table with working Data */
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.offer_wrk (file_date, offer_sid, valid_from_date, valid_to_date, offer_num, submission_sid, sequence_num, offer_name, accept_date, start_date, extend_date, last_modified_date, last_modified_time, capture_date, salary_amt, sign_on_bonus_amt, salary_pay_basis_amt, hours_per_day_num, source_system_code, dw_last_update_date_time)
    SELECT
        stg.file_date AS file_date,
        CAST(xwlk.sk AS INT64) AS offer_sid,
        datetime_trunc(current_datetime('US/Central'), second) AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        stg.offer_number AS offer_num,
        sub.submission_sid AS submission_sid,
        CAST(stg.sequence AS INT64) AS sequence_num,
        stg.name AS offer_name,
        CAST(stg.accepteddate AS DATE) AS accept_date,
        CAST(stg.actualstartdate AS DATE) AS start_date,
        CAST(stg.extenddate AS DATE) AS extend_date,
        CAST(stg.lastmodifieddate AS DATE) AS last_modified_date,
        CAST(stg.lastmodifieddate AS TIME) AS last_modified_time,
        CAST(stg.capturedate AS DATE) AS capture_date,
        CASE
           trim(stg.salary)
          WHEN '' THEN NUMERIC '0'
          ELSE CAST(trim(stg.salary) as NUMERIC)
        END AS salary_amt,
        CASE
           trim(stg.signonbonus)
          WHEN '' THEN NUMERIC '0'
          ELSE CAST(trim(stg.signonbonus) as NUMERIC)
        END AS sign_on_bonus_amt,
        CASE
           trim(stg.payvalue)
          WHEN '' THEN NUMERIC '0'
          ELSE CAST(trim(stg.payvalue) as BIGNUMERIC)
        END AS salary_pay_basis_amt,
        CAST(stg.hoursperday AS NUMERIC) AS hours_per_day_num,
        stg.source_system_code,
        datetime_trunc(current_datetime('US/Central'), second) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_offer AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(substr(coalesce(trim(CAST(offer_number AS STRING)), ''), 1, 255)) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'OFFER'
        LEFT OUTER JOIN -- INNER JOIN {{ params.param_hr_stage_dataset_name }}.REF_SK_XWLK XWLK_SUB
        -- ON (CAST( TRIM(STG.APPLICATION_NUMBER) AS VARCHAR(255)) = XWLK_SUB.SK_SOURCE_TXT    AND XWLK_SUB.SK_TYPE = 'SUBMISSION')
        {{ params.param_hr_base_views_dataset_name }}.submission AS sub ON CASE
           trim(stg.application_number)
          WHEN '' THEN 0
          ELSE CAST(trim(stg.application_number) as INT64)
        END = sub.submission_num
         AND sub.valid_to_date = DATETIME("9999-12-31 23:59:59")
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
  ;


  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.ats_jobapplication_bct_stg_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.ats_jobapplication_bct_stg_wrk (candidate, jobapplication, jobrequisition)
    SELECT
        ats_cust_v3_jobapplication_stg.candidate,
        ats_cust_v3_jobapplication_stg.jobapplication,
        ats_cust_v3_jobapplication_stg.jobrequisition
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg
      WHERE ats_cust_v3_jobapplication_stg.offerstatus <> 0
      GROUP BY 1, 2, 3
  ;

CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'ats_jobapplication_bct_stg_wrk', 'CAST(JobApplication AS STRING)||\'-\'||CAST(Candidate AS STRING)||\'-\'||CAST(JobRequisition AS STRING)||\'-ATS\'', 'OFFER');


INSERT INTO {{ params.param_hr_stage_dataset_name }}.offer_wrk (offer_sid, valid_from_date, valid_to_date, offer_num, submission_sid, sequence_num, offer_name,
   accept_date, start_date, extend_date, last_modified_date, 
   last_modified_time,
    capture_date, salary_amt, sign_on_bonus_amt, salary_pay_basis_amt, hours_per_day_num, source_system_code, dw_last_update_date_time)
    SELECT DISTINCT
        CAST(xwlk.sk AS INT64) AS offer_sid,
        datetime_trunc(current_datetime('US/Central'), second) AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(cp.candidate_profile_num AS INT64) AS offer_num,
        cp.candidate_profile_sid AS submission_sid,
        NULL AS sequence_num,
        CAST(NULL AS STRING) AS offer_name,
        DATE(stg.acceptancedate) AS accept_date,
        DATE(stg.employmentstartdate) AS start_date,
        DATE(stg.offerdate) AS extend_date,
        DATE(stg._asoftimestamp) AS last_modified_date,
        CAST(NULL AS TIME) AS last_modified_time,
        DATE(stg.offerdocumentcreateddate) AS capture_date,
        CASE
           cast(stg.hcajobapplicationannualizedsalary AS STRING)
          WHEN '' THEN 0
          ELSE CAST(stg.hcajobapplicationannualizedsalary as NUMERIC)
        END AS salary_amt,
        CASE
           CAST(stg.hcajobapplicationsignonbonus AS STRING)
          WHEN '' THEN NUMERIC '0'
          ELSE CAST(stg.hcajobapplicationsignonbonus as NUMERIC)
        END AS sign_on_bonus_amt,
        CASE
           CAST(stg.hcajobapplicationhourlyrate AS STRING)
          WHEN '' THEN NUMERIC '0'
          ELSE CAST(stg.hcajobapplicationhourlyrate as BIGNUMERIC)
        END AS salary_pay_basis_amt,
        NULL AS hours_per_day_num,
        'B' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), second) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON trim(concat(CAST(jobapplication AS STRING), '-', CAST(candidate AS STRING), '-', CAST(jobrequisition AS STRING), '-ATS')) = trim(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'OFFER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS cp ON cp.job_application_num = stg.jobapplication
         AND cp.candidate_num = stg.candidate
         AND cp.requisition_num = stg.jobrequisition
         AND cp.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND upper(cp.source_system_code) = 'B'
      WHERE stg.offerstatus <> 0
      QUALIFY row_number() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = 1
  ;