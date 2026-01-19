BEGIN DECLARE DUP_COUNT INT64;

DECLARE current_ts DATETIME;

SET
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}','histerr',"company||'-'||employee||'-'||TRIM(action_code)||'-'||TRIM(COALESCE(action_type,''))||'-'||action_nbr", 'Employee_Action');

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.person_action_wrk1;

/*  Load Work Table with working Data for First load*/
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.person_action_wrk1 (
    company,
    employee,
    action_code,
    action_nbr,
    action_type,
    effect_date,
    ant_end_date,
    requisition,
    applicant,
    reason_01,
    user_id,
    date_stamp,
    process_level,
    source_system_code
  )
SELECT
  -- PARTICIPNT commneted and Applicant is added by Pratap
  x.company,
  x.employee,
  x.action_code,
  x.action_nbr,
  x.action_type,
  x.effect_date,
  x.ant_end_date,
  x.requisition,
  x.applicant,
  x.reason_01,
  x.user_id,
  x.date_stamp,
  x.process_level,
  'L' AS source_system_code
FROM
  -- Below Select statement is used as it was provided by Joe in JIRA HDM-576
  (
    SELECT
      persaction.company AS company,
      CASE
        WHEN trim(persaction.action_type) = 'A' THEN persaction.participnt
        ELSE persaction.employee
      END AS employee,
      trim(persaction.action_code) AS action_code,
      persaction.action_nbr AS action_nbr,
      trim(persaction.action_type) AS action_type,
      persaction.effect_date,
      persaction.ant_end_date,
      persaction.requisition,
      /*CASE WHEN trim(persaction.action_type) = 'A'
       THEN persaction.employee
       ELSE NULL END AS applicant */
      CASE
        WHEN trim(persaction.action_type) = 'A' THEN persaction.employee
        ELSE NULL
      END AS applicant,
      persaction.reason_01,
      persaction.user_id,
      CAST(NULL as DATE) AS date_stamp,
      CASE
        WHEN coalesce(trim(persaction.process_level), '') = '' THEN '00000'
        ELSE concat(
          substr(
            '00000',
            1,
            5 - length(trim(persaction.process_level))
          ),
          trim(persaction.process_level)
        )
      END AS process_level
    FROM
      {{ params.param_hr_stage_dataset_name }}.persaction
    UNION
    DISTINCT
    SELECT
      persacthst.company AS company,
      persacthst.employee AS employee,
      trim(persacthst.action_code) AS action_code,
      persacthst.action_nbr AS action_nbr,
      trim(persacthst.action_type) AS action_type,
      persacthst.effect_date,
      persacthst.ant_end_date,
      NULL AS requisition,
      NULL AS applicant,
      persacthst.reason_01,
      persacthst.user_id,
      persacthst.date_stamp AS date_stamp,
      '00000' AS process_level
    FROM
      {{ params.param_hr_stage_dataset_name }}.persacthst
  ) AS x QUALIFY row_number() OVER (
    PARTITION BY x.action_nbr,
    x.employee,
    x.action_code,
    x.effect_date,
    x.action_type,
    x.company
    ORDER BY
      unix_date(x.date_stamp) DESC
  ) = 1;

CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen(
  '{{ params.param_hr_stage_dataset_name }}',
  'person_action_wrk1',
  "company||'-'||employee||'-'||TRIM(action_code)||'-'||TRIM(action_type)||'-'||action_nbr",
  'Employee_Action'
);

/*  Truncate Worktable Table     */
TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.person_action_wrk2;

/*  Load Work Table with working Data for First load*/
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.person_action_wrk2 (
    person_action_sid,
    valid_from_date,
    valid_to_date,
    action_code,
    employee_sid,
    employee_num,
    applicant_num,
    applicant_sid,
    action_sequence_num,
    action_type_code,
    action_from_date,
    action_to_date,
    requisition_sid,
    action_reason_text,
    person_action_update_sid,
    action_last_update_date,
    eff_from_date,
    person_action_flag,
    delete_ind,
    active_dw_ind,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  cast(xwlk.sk as INT64) AS person_action_sid,
  current_ts AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  wrk.action_code,
  coalesce(emp.employee_sid, 0) AS employee_sid,
  -- ,CASE WHEN WRK.Action_Type NOT=  'A'  THEN LKP_EMPL_SID.SK   ELSE LKP_EMPL_SID2.SK  END AS EMPLOYEE_SID
  -- Below line is added and commented by Pratap
  wrk.employee AS employee_num,
  -- ,CASE WHEN WRK.Action_Type NOT=  'A'  THEN WRK.EMPLOYEE      ELSE WRK.PARTICIPNT END AS EMPLOYEE_NUM
  -- ,CASE WHEN WRK.Action_Type    =  'A'  THEN APT.APPLICANT_NUM ELSE NULL END AS APPLICANT_NUM
  wrk.applicant AS applicant_num,
  coalesce(apt.applicant_sid, 0) AS applicant_sid,
  -- ,CASE WHEN WRK.Action_Type =   'A'  THEN APT.APPLICANT_SID ELSE NULL END AS APPLICANT_SID
  wrk.action_nbr AS action_sequence_num,
  wrk.action_type AS action_type_code,
  wrk.effect_date AS action_from_date,
  CASE
    WHEN wrk.ant_end_date = DATE '1800-01-01' THEN DATE '1999-12-31'
    ELSE wrk.ant_end_date
  END AS action_to_date,
  req.requisition_sid AS requisition_sid,
  wrk.reason_01 AS action_reason_text,
  coalesce(emp.employee_sid, 0) AS person_action_update_sid,
  coalesce(wrk.date_stamp, DATE '1900-01-01') AS action_last_update_date,
  wrk.effect_date AS eff_from_date,
  'V' AS person_action_flag,
  'A' AS delete_ind,
  'Y' AS active_dw_ind,
  coalesce(wrk.company, 0) AS lawson_company_num,
  coalesce(wrk.process_level, '00000') AS process_level_code,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.person_action_wrk1 AS wrk
  INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(
    substr(
      concat(
        wrk.company,
        '-',
        wrk.employee,
        '-',
        trim(wrk.action_code),
        '-',
        trim(coalesce(wrk.action_type, '')),
        '-',
        wrk.action_nbr
      ),
      1,
      255
    )
  ) = upper(xwlk.sk_source_txt)
  AND upper(xwlk.sk_type) = 'EMPLOYEE_ACTION'
  LEFT OUTER JOIN
  /*
   LEFT OUTER JOIN $NCR_STG_SCHEMA.Ref_SK_Xwlk LKP_EMPL_SID
   ON (Cast(Trim(WRK.Company)||'-'||Trim(WRK.Employee) AS VARCHAR(255)) = LKP_EMPL_SID.SK_Source_Txt AND LKP_EMPL_SID.SK_Type = 'Employee')*/
  (
    SELECT
      employee.employee_sid,
      employee.employee_num,
      employee.lawson_company_num
    FROM
      {{ params.param_hr_base_views_dataset_name }}.employee
    WHERE
      date(employee.valid_to_date) = "9999-12-31"
    GROUP BY
      1,
      2,
      3
  ) AS emp ON wrk.employee = emp.employee_num
  AND wrk.company = emp.lawson_company_num
  LEFT OUTER JOIN
  /* Left outer join  (select EMPLOYEE_SID, Employee_34_Login_Code from  {{ params.param_hr_core_dataset_name }}base_views.employee  where valid_to_date = date '9999-12-31' group by 1,2)   EMP1
   on (Trim(Wrk.USER_ID) = Trim(EMP1.Employee_34_Login_Code))*/
  (
    SELECT
      applicant.applicant_sid,
      applicant.lawson_company_num,
      applicant.applicant_num
    FROM
      {{ params.param_hr_base_views_dataset_name }}.applicant
    WHERE
      date(applicant.valid_to_date) = "9999-12-31"
    GROUP BY
      1,
      2,
      3
  ) AS apt ON wrk.company = apt.lawson_company_num
  AND wrk.applicant = apt.applicant_num
  LEFT OUTER JOIN (
    SELECT
      requisition.requisition_sid,
      requisition.requisition_num,
      requisition.lawson_company_num
    FROM
      {{ params.param_hr_base_views_dataset_name }}.requisition
    WHERE
      date(requisition.valid_to_date) = "9999-12-31"
    GROUP BY
      1,
      2,
      3
  ) AS req ON req.requisition_num = wrk.requisition
  AND req.lawson_company_num = wrk.company;

/*  Truncate Worktable Table     */
TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.person_action_wrk3;

/*  Load Work Table with working Data for First load*/
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.person_action_wrk3 (person_action_sid, effect_date)
SELECT
  cast(xwlk.sk as INT64) AS person_action_sid,
  hserr.effect_date
FROM
  {{ params.param_hr_stage_dataset_name }}.histerr AS hserr
  INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(
    substr(
      concat(
        hserr.company,
        '-',
        hserr.employee,
        '-',
        trim(hserr.action_code),
        '-',
        trim(coalesce(hserr.action_type, '')),
        '-',
        hserr.action_nbr
      ),
      1,
      255
    )
  ) = upper(xwlk.sk_source_txt)
  AND upper(xwlk.sk_type) = 'EMPLOYEE_ACTION' QUALIFY row_number() OVER (
    PARTITION BY person_action_sid,
    hserr.effect_date
    ORDER BY
      hserr.effect_date DESC
  ) = 1;

BEGIN TRANSACTION;

/*  Close the previous records from Target table for same key for any Changes  */
/*  Insert the New Records/Chnages into the Target Table  */
/*  Updating the records for Person_Action_Flag present in history table  */
/* Begin Transaction Block Starts Here */
UPDATE
  {{ params.param_hr_core_dataset_name }}.person_action AS tgt
SET
  --valid_to_date = DATE(stg.valid_from_date - INTERVAL 1 DAY)
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  active_dw_ind = 'N',
  dw_last_update_date_time = stg.dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.person_action_wrk2 AS stg
WHERE
  tgt.person_action_sid = stg.person_action_sid
  AND tgt.eff_from_date = stg.eff_from_date
  AND tgt.source_system_code = stg.source_system_code
  AND (
    trim(tgt.action_code) <> trim(stg.action_code)
    OR tgt.employee_sid <> stg.employee_sid
    OR tgt.applicant_sid <> stg.applicant_sid
    OR tgt.employee_num <> stg.employee_num
    OR tgt.applicant_num <> stg.applicant_num
    OR upper(trim(coalesce(tgt.action_type_code, ''))) <> upper(trim(coalesce(stg.action_type_code, '')))
    OR tgt.action_sequence_num <> stg.action_sequence_num
    OR tgt.action_from_date <> stg.action_from_date
    OR coalesce(
      substr(CAST(tgt.action_to_date as STRING), 1, 10),
      ''
    ) <> coalesce(
      substr(CAST(stg.action_to_date as STRING), 1, 10),
      ''
    )
    OR tgt.requisition_sid <> stg.requisition_sid
    OR upper(trim(coalesce(tgt.action_reason_text, ''))) <> upper(trim(coalesce(stg.action_reason_text, '')))
    OR tgt.person_action_update_sid <> stg.person_action_update_sid
    OR tgt.action_last_update_date <> stg.action_last_update_date
    OR tgt.lawson_company_num <> stg.lawson_company_num
    OR trim(tgt.process_level_code) <> trim(stg.process_level_code)
  )
  AND date(tgt.valid_to_date) = "9999-12-31"
  AND upper(tgt.person_action_flag) <> 'D'
  AND tgt.source_system_code = 'L';

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.person_action (
    person_action_sid,
    valid_from_date,
    valid_to_date,
    action_code,
    employee_sid,
    applicant_sid,
    employee_num,
    applicant_num,
    action_type_code,
    action_sequence_num,
    eff_from_date,
    action_from_date,
    action_to_date,
    requisition_sid,
    action_reason_text,
    person_action_update_sid,
    person_action_flag,
    action_last_update_date,
    active_dw_ind,
    delete_ind,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  wrk.person_action_sid,
  --wrk.valid_from_date,
  --wrk.valid_to_date,
  current_ts AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  wrk.action_code,
  wrk.employee_sid,
  wrk.applicant_sid,
  wrk.employee_num,
  wrk.applicant_num,
  wrk.action_type_code,
  wrk.action_sequence_num,
  wrk.eff_from_date,
  wrk.action_from_date,
  wrk.action_to_date,
  wrk.requisition_sid,
  wrk.action_reason_text,
  wrk.person_action_update_sid,
  wrk.person_action_flag,
  wrk.action_last_update_date,
  wrk.active_dw_ind,
  wrk.delete_ind,
  wrk.lawson_company_num,
  wrk.process_level_code,
  wrk.source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.person_action_wrk2 AS wrk
  LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.person_action AS tgt ON wrk.person_action_sid = tgt.person_action_sid
  AND wrk.eff_from_date = tgt.eff_from_date
  AND date(tgt.valid_to_date) = "9999-12-31"
  AND tgt.source_system_code = 'L'
WHERE
  tgt.person_action_sid IS NULL
  AND tgt.eff_from_date IS NULL;

UPDATE
  {{ params.param_hr_core_dataset_name }}.person_action AS tgt
SET
  person_action_flag = 'D'
FROM
  {{ params.param_hr_stage_dataset_name }}.person_action_wrk3 AS stg
WHERE
  tgt.person_action_sid = stg.person_action_sid
  AND tgt.action_from_date = stg.effect_date
  AND upper(tgt.person_action_flag) = 'V'
  AND tgt.source_system_code = 'L';

UPDATE
  {{ params.param_hr_core_dataset_name }}.person_action AS empl
SET
  delete_ind = 'D',
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  active_dw_ind = 'N'
WHERE
  date(empl.valid_to_date) = "9999-12-31"
  AND empl.source_system_code = 'L'
  AND NOT EXISTS (
    SELECT
      1
    FROM
      -- CHAR(1)
      {{ params.param_hr_stage_dataset_name }}.person_action_wrk2 AS wrk2
    WHERE
      coalesce(empl.lawson_company_num, 0) = coalesce(wrk2.lawson_company_num, 0)
      AND coalesce(empl.employee_num, 0) = coalesce(wrk2.employee_num, 0)
      AND empl.source_system_code = wrk2.source_system_code
  ) AND empl.source_system_code = 'L';

UPDATE
  {{ params.param_hr_core_dataset_name }}.person_action AS empl
SET
  delete_ind = 'A'
WHERE
  date(empl.valid_to_date) = "9999-12-31"
  AND empl.source_system_code = 'L'
  AND (
    empl.lawson_company_num,
    empl.employee_num,
    empl.source_system_code
  ) IN(
    SELECT
      AS STRUCT person_action_wrk1.company,
      person_action_wrk1.employee,
      person_action_wrk1.source_system_code
    FROM
      {{ params.param_hr_stage_dataset_name }}.person_action_wrk1
    GROUP BY
      1,
      2,
      3
  );

/* End Transaction Block comment */
/*   2ND Retire Condition 
 Compare using Company, Employee, Action_Type, Action_Code, Action_Nbr, Effect_Date from Persaction & Persacthst staging table 
 */
UPDATE
  {{ params.param_hr_core_dataset_name }}.person_action AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
WHERE
  date(tgt.valid_to_date) = "9999-12-31"
  AND tgt.source_system_code = 'L'
  AND NOT EXISTS (
    SELECT
      1
    FROM
      {{ params.param_hr_stage_dataset_name }}.person_action_wrk2 AS wrk2
    WHERE
      tgt.lawson_company_num = wrk2.lawson_company_num
      AND tgt.employee_num = wrk2.employee_num
      AND tgt.action_code = wrk2.action_code
      AND upper(coalesce(tgt.action_type_code, '')) = upper(coalesce(wrk2.action_type_code, ''))
      AND tgt.action_sequence_num = wrk2.action_sequence_num
      AND tgt.eff_from_date = wrk2.eff_from_date
      AND tgt.source_system_code = wrk2.source_system_code
  );

/* Test Unique Primary Index constraint set in Terdata */
SET
  DUP_COUNT = (
    select
      count(*)
    from
      (
        select
          person_action_sid,
          eff_from_date,
          valid_from_date
        from
          {{ params.param_hr_core_dataset_name }}.person_action
        group by
          person_action_sid,
          eff_from_date,
          valid_from_date
        having
          count(*) > 1
      )
  );

IF DUP_COUNT <> 0 THEN ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat(
  'Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.person_action'
);

ELSE COMMIT TRANSACTION;

END IF;

END;