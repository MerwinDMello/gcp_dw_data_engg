/*  Truncate Worktable Table     */
  
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.candidate_person_wrk;

BEGIN
	  
	  DECLARE current_dt DATETIME;
	  SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

	  
	  
/*  Generate the surrogate keys for Candidate */



CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'taleo_candidate', '(NUMBER)', 'CANDIDATE');



/*  Load Work Table with working Data */

  
    INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_person_wrk (candidate_sid, valid_from_date, first_name, middle_name, last_name, social_security_num, email_address, maiden_name, driver_license_num, driver_license_state_code, birth_date, valid_to_date, source_system_code, dw_last_update_date_time)
    SELECT
        cast(xwlk.sk  as int64)AS candidate_sid,
        current_dt AS valid_from_date,
        substr(max(trim(stg.firstname)), 1, 50) AS first_name,
        substr(max(trim(stg.middleinitial)), 1, 50) AS middle_name,
        substr(max(trim(stg.lastname)), 1, 50) AS last_name,
        substr(concat(max(trim(stg.socialsecuritynumber)), repeat(' ', 12)), 1, 12) AS social_security_num,
        substr(max(trim(stg.emailaddress)), 1, 50) AS email_address,
        cast(NULL as string) AS maiden_name,
        NULL AS driver_license_num,
        NULL AS driver_license_state_code,
        CAST((stg.birthday) as DATE) AS birth_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        'T' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_candidate AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON trim(stg.number) = xwlk.sk_source_txt
         AND upper(xwlk.sk_type) = 'CANDIDATE'
      GROUP BY 1, 2, upper(substr(trim(stg.firstname), 1, 50)), upper(substr(trim(stg.middleinitial), 1, 50)), upper(substr(trim(stg.lastname), 1, 50)), upper(substr(concat(trim(stg.socialsecuritynumber), repeat(' ', 12)), 1, 12)), upper(substr(trim(stg.emailaddress), 1, 50)), 8, 8, 8, 11, 12, 13, 14  ;

INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.candidate_person_wrk (candidate_sid,
    valid_from_date,
    first_name,
    middle_name,
    last_name,
    social_security_num,
    email_address,
    maiden_name,
    driver_license_num,
    driver_license_state_code,
    birth_date,
    valid_to_date,
    source_system_code,
    dw_last_update_date_time)
SELECT
  st.candidate_sid,
  st.valid_from_date,
  st.first_name,
  st.middle_name,
  st.last_name,
  st.social_security_num,
  st.email_address,
  st.maiden_name,
  st.driver_license_num,
  st.driver_license_state_code,
  st.birth_date,
  st.valid_to_date,
  st.source_system_code,
  st.dw_last_update_date_time
FROM (
  SELECT
    xwlk.candidate_sid,
    CURRENT_DATE('US/Central') AS valid_from_date,
    TRIM(stg.name_givenname) AS first_name,
    TRIM(stg.name_middlename) AS middle_name,
    TRIM(stg.name_familyname) AS last_name,
    TRIM(stg1.identificationnumber) AS social_security_num,
    SUBSTR(TRIM(stg.primarycontactinfo_emailaddress), 1, 100) AS email_address,
    TRIM(stg.name_maidenname) AS maiden_name,
    TRIM(stg.id_documentnumber) AS driver_license_num,
    TRIM(stg.id_state) AS driver_license_state_code,
    CAST(stg.birthdate as DATE) AS birth_date,
    DATETIME("9999-12-31 23:59:59") AS valid_to_date,
    'B' AS source_system_code,
    DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time,
    stg.updatestamp
  FROM
    {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_candidate_stg AS stg
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.candidate AS xwlk
  ON
    CAST(stg.candidate as INT64) = xwlk.candidate_num
    AND (xwlk.valid_to_date) =DATETIME("9999-12-31 23:59:59")
    AND upper(xwlk.source_system_code) = 'B'
  LEFT OUTER JOIN
    {{ params.param_hr_stage_dataset_name }}.ats_cust_candidateidentificationnumber_stg AS stg1
  ON
    stg1.candidate = CAST(stg.candidate as INT64)
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15 QUALIFY ROW_NUMBER() OVER (PARTITION BY xwlk.candidate_sid ORDER BY stg.updatestamp DESC) = 1 ) AS st ;
END;