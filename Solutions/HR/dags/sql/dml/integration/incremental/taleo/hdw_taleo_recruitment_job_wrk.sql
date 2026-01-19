  /*  Generate the surrogate keys for Candidate */
CALL
  {{ params.param_hr_core_dataset_name }}.sk_gen(
    '{{ params.param_hr_stage_dataset_name }}',
    'taleo_jobinformation',
    'coalesce(trim(cast(number as string)),"-1")',
---  'coalesce(substring(cast("number" as VARCHAR(50)),1,(character_length(cast("number" as VARCHAR(50)) )-1)),-1)',
    'RECRUITMENT_JOB');

/*  Truncate Worktable Table     */
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.recruitment_job_wrk; 
  /*  Load Work Table with working Data */

  
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.recruitment_job_wrk (file_date,
    recruitment_job_sid,
    recruitment_job_num,
    job_title_name,
    job_grade_code,
    job_schedule_id,
    overtime_status_id,
    primary_facility_location_num,
    recruiter_user_sid,
    recruitment_job_parameter_sid,
    recruitment_position_sid,
    fte_pct,
    source_system_code,
    dw_last_update_date_time)
SELECT
  stg.file_date,
  cast(xwlk.sk as int64) AS recruitment_job_sid,
  cast(stg.number as int64),
  TRIM(stg.title) AS job_title_name,
  MAX(stg.jobgrade) AS job_grade_code,
  cast(stg.jobschedule_number as int64) AS job_schedule_id,
  cast(stg.overtimestatus_number as int64) AS overtime_status_id,
  stg.primarylocation_number AS primary_facility_location_num,
  ru.recruitment_user_sid,
  rjp.recruitment_job_parameter_sid,
  NULL AS recruitment_position_sid,
  cast(NULL as NUMERIC) AS fte_pct,
  'T' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.taleo_jobinformation AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  COALESCE(TRIM(CAST(number AS STRING)),"-1") = xwlk.sk_source_txt
  AND UPPER(xwlk.sk_type) = 'RECRUITMENT_JOB'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS ru
ON
  COALESCE(CASE when stg.recruiterowner_userno is null 
       THEN 0
    ELSE
    CAST(stg.recruiterowner_userno AS INT64)
  END
    , 0) = COALESCE(ru.recruitment_user_num, 0)
  AND (ru.valid_to_date) = DATETIME("9999-12-31 23:59:59")
  AND UPPER(ru.source_system_code) = 'T'
LEFT OUTER JOIN (
  SELECT
    DISTINCT recruitment_job_parameter_detail.recruitment_job_parameter_sid,
    recruitment_job_parameter_detail.job_parameter_num
  FROM
    {{ params.param_hr_base_views_dataset_name }}.recruitment_job_parameter_detail
  WHERE
    (recruitment_job_parameter_detail.valid_to_date) = DATETIME("9999-12-31 23:59:59")
    AND UPPER(recruitment_job_parameter_detail.source_system_code) = 'T' ) AS rjp
ON
  (stg.offerparameter_number) = rjp.job_parameter_num
WHERE
  stg.number <> 0
GROUP BY
  1,
  2,
  3,
  4,
  UPPER(stg.jobgrade),
  6,
  7,
  8,
  9,
  10,
  11,
  11,
  13,
  14 ;

INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.taleo_jobinformation_rej
SELECT
---  taleo_jobinformation.*
file_date
,number
,additionalinformation
,autodeclinecandidates
,autorejectcandidates
,billratemedian
,billratenottoexceed
,creationdate
,descriptionbaselocale
,descriptionexternalhtml
,descriptioninternalhtml
,CAST(externalbonusamount as INT64) as externalbonusamount
,externalbonustracking
,externalqualificationhtml
,CAST(highquartilesalary as INT64) as highquartilesalary
,CAST(internalbonusamount as INT64) as internalbonusamount
,internalbonustracking
,internalqualificationhtml
,jobboarddescription
,jobgrade
,lastmodifieddate
,CAST(lowquartilesalary as INT64) as lowquartilesalary
,CAST(midpointsalary AS NUMERIC) AS midpointsalary
,numbertohire
,othercosts
,relocationcosts
,sourcingbudget
,title
,travelcosts
,unlimitedhire
,bonuscurrency_number
,budgetcurrency_number
,citizenshipstatus_number
,contingentcurrency_number
,creator_userno
,cswworkflow_number
,eejobgroup_number
,eejobcategory_number
,eeestablishment_number
,hiretype_number
,jobfield_number
,jobinformationgroup_number
,joblevel_number
,jobrole_number
,jobschedule_number
,jobshift_number
,jobtype_number
,offerparameter_number
,organization_number
,overtimestatus_number
,primarylocation_number
,program_number
,recruiterowner_userno
,studylevel_number
,willingnesstotravel_number
,matched_from
,ispostedexternally
,ispostedinternally
,postingboardtype
,postingdate
,postingstatus
,unpostingdate
,jobboardid
,jobboard
,source_system_code
,dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.taleo_jobinformation
WHERE
  taleo_jobinformation.number = 0 ;

CALL
  {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}',
    'ats_cust_jobrequisition_stg',
    --'trim(cast(jobrequisition as varchar(20)))||\'-ats\'',
    'jobrequisition ||\'-ATS\'',
    'RECRUITMENT_JOB');

INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.recruitment_job_wrk (file_date,
    recruitment_job_sid,
    recruitment_job_num,
    job_title_name,
    job_grade_code,
    job_schedule_id,
    overtime_status_id,
    primary_facility_location_num,
    recruiter_user_sid,
    recruitment_job_parameter_sid,
    recruitment_position_sid,
    fte_pct,
    source_system_code,
    dw_last_update_date_time)
SELECT
  a.file_date,
  cast(a.recruitment_job_sid as int64),
  CAST(a.recruitment_job_num as INT64),
  a.job_title_name,
  cast(a.job_grade_code as string),
  a.job_schedule_id,
  a.overtime_status_id,
  a.primary_facility_location_num,
  a.recruiter_user_sid,
  a.recruitment_job_parameter_sid,
  a.recruitment_position_sid,
  CAST(a.fte_pct as NUMERIC),
  a.source_system_code,
  a.dw_last_update_date_time
FROM (
  SELECT
    CURRENT_DATE() AS file_date,
    xwlk.sk AS recruitment_job_sid,
    stg.jobrequisition AS recruitment_job_num,
    stg.description AS job_title_name,
    MAX(stg.salarystructuregrade) AS job_grade_code,
    rs.job_schedule_id,
    CASE
      WHEN CAST(stg.exemptfromovertime as INT64) = 2 THEN 1
      WHEN CAST(stg.exemptfromovertime as INT64) = 1 THEN 2
    ELSE
    0
  END
    AS overtime_status_id,
    rl.location_num AS primary_facility_location_num,
    usr.recruitment_user_sid AS recruiter_user_sid,
    0 AS recruitment_job_parameter_sid,
    rp.recruitment_position_sid,
    stg.fte AS fte_pct,
    'B' AS source_system_code,
    DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time,
    stg.updatestamp
  FROM
    {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS stg
  INNER JOIN
    {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
  ON
    UPPER(TRIM(CONCAT(stg.jobrequisition, '-ATS'))) = UPPER(xwlk.sk_source_txt)
    AND UPPER(xwlk.sk_type) = 'RECRUITMENT_JOB'
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.ref_job_schedule AS rs
  ON
    rs.job_schedule_code = stg.worktype
    AND UPPER(rs.source_system_code) = 'B'
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.ref_recruitment_location AS rl
  ON
    rl.location_code_text = stg.location
    AND UPPER(rl.source_system_code) = 'B'
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS usr
  ON
    usr.recruitment_user_num = CAST(stg.recruiter as INT64)
    AND (usr.valid_to_date) = DATETIME("9999-12-31 23:59:59")
    AND UPPER(usr.source_system_code) = 'B'
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.recruitment_position AS rp
  ON
    COALESCE(CAST(stg.position as INT64),12345) = COALESCE(rp.recruitment_position_num,12345)
    AND (rp.valid_to_date) = DATETIME("9999-12-31 23:59:59")
    AND UPPER(rp.source_system_code) = 'B'
  GROUP BY
    1,
    2,
    3,
    4,
    (stg.salarystructuregrade),
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15 QUALIFY ROW_NUMBER() OVER (PARTITION BY xwlk.sk ORDER BY stg.updatestamp DESC) = 1 ) AS a ;