BEGIN DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

SET
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.req_appr_stg_wrk1;

-- Load Wrk1 table from stageing table Pajobreq
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.req_appr_stg_wrk1 (
    requisition,
    company,
    approval_start_date,
    approval_end_date,
    approval_desc,
    approver_position_title_desc,
    approval_emp,
    requsition_approval_type_code,
    lawson_company_num,
    process_level_code,
    active_dw_ind
  )
SELECT
  ee.requisition,
  ee.company,
  ee.approval_start_date,
  DATE '9999-12-31' AS approval_end_date,
  max(ee.approval_desc) AS approval_desc,
  ee.approver_position_title_desc,
  ee.approval_emp,
  max(ee.requsition_approval_type_code) AS requsition_approval_type_code,
  ee.lawson_company_num AS lawson_company_num,
  trim(ee.process_level_code) AS process_level_code,
  'Y' AS active_dw_ind
FROM
  (
    SELECT
      pajobreq.requisition,
      pajobreq.company,
      pajobreq.approval_date_01 AS approval_start_date,
      pajobreq.approval_desc_01 AS approval_desc,
      pajobreq.approval_emp_01 AS approval_emp,
      'APPRVL_01' AS requsition_approval_type_code,
      pajobreq.approval_desc_01 AS approver_position_title_desc,
      pajobreq.company AS lawson_company_num,
      pajobreq.process_level AS process_level_code
    FROM
      {{ params.param_hr_stage_dataset_name }}.pajobreq
    WHERE
      pajobreq.approval_date_01 <> DATE '1800-01-01'
    UNION
    ALL
    SELECT
      pajobreq.requisition,
      pajobreq.company,
      pajobreq.approval_date_02 AS approval_start_date,
      pajobreq.approval_desc_02 AS approval_desc,
      pajobreq.approval_emp_02 AS approval_emp,
      'APPRVL_02' AS requsition_approval_type_code,
      pajobreq.approval_desc_02 AS approver_position_title_desc,
      pajobreq.company AS lawson_company_num,
      pajobreq.process_level AS process_level_code
    FROM
      {{ params.param_hr_stage_dataset_name }}.pajobreq
    WHERE
      pajobreq.approval_date_02 <> DATE '1800-01-01'
    UNION
    ALL
    SELECT
      pajobreq.requisition,
      pajobreq.company,
      pajobreq.approval_date_03 AS approval_start_date,
      pajobreq.approval_desc_03 AS approval_desc,
      pajobreq.approval_emp_03 AS approval_emp,
      'APPRVL_03' AS requsition_approval_type_code,
      pajobreq.approval_desc_03 AS approver_position_title_desc,
      pajobreq.company AS lawson_company_num,
      pajobreq.process_level AS process_level_code
    FROM
      {{ params.param_hr_stage_dataset_name }}.pajobreq
    WHERE
      pajobreq.approval_date_03 <> DATE '1800-01-01'
    UNION
    ALL
    SELECT
      pajobreq.requisition,
      pajobreq.company,
      pajobreq.approval_date_04 AS approval_start_date,
      pajobreq.approval_desc_04 AS approval_desc,
      pajobreq.approval_emp_04 AS approval_emp,
      'APPRVL_04' AS requsition_approval_type_code,
      pajobreq.approval_desc_04 AS approver_position_title_desc,
      pajobreq.company AS lawson_company_num,
      pajobreq.process_level AS process_level_code
    FROM
      {{ params.param_hr_stage_dataset_name }}.pajobreq
    WHERE
      pajobreq.approval_date_04 <> DATE '1800-01-01'
    UNION
    ALL
    SELECT
      pajobreq.requisition,
      pajobreq.company,
      pajobreq.approval_date_05 AS approval_start_date,
      pajobreq.approval_desc_05 AS approval_desc,
      pajobreq.approval_emp_05 AS approval_emp,
      'APPRVL_05' AS requsition_approval_type_code,
      pajobreq.approval_desc_05 AS approver_position_title_desc,
      pajobreq.company AS lawson_company_num,
      pajobreq.process_level AS process_level_code
    FROM
      {{ params.param_hr_stage_dataset_name }}.pajobreq
    WHERE
      pajobreq.approval_date_05 <> DATE '1800-01-01'
    UNION
    ALL
    SELECT
      pajobreq.requisition,
      pajobreq.company,
      pajobreq.approval_date_06 AS approval_start_date,
      pajobreq.approval_desc_06 AS approval_desc,
      pajobreq.approval_emp_06 AS approval_emp,
      'APPRVL_06' AS requsition_approval_type_code,
      pajobreq.approval_desc_06 AS approver_position_title_desc,
      pajobreq.company AS lawson_company_num,
      pajobreq.process_level AS process_level_code
    FROM
      {{ params.param_hr_stage_dataset_name }}.pajobreq
    WHERE
      pajobreq.approval_date_06 <> DATE '1800-01-01'
    UNION
    ALL
    SELECT
      pajobreq.requisition,
      pajobreq.company,
      pajobreq.approval_date_07 AS approval_start_date,
      pajobreq.approval_desc_07 AS approval_desc,
      pajobreq.approval_emp_07 AS approval_emp,
      'APPRVL_07' AS requsition_approval_type_code,
      pajobreq.approval_desc_07 AS approver_position_title_desc,
      pajobreq.company AS lawson_company_num,
      pajobreq.process_level AS process_level_code
    FROM
      {{ params.param_hr_stage_dataset_name }}.pajobreq
    WHERE
      pajobreq.approval_date_07 <> DATE '1800-01-01'
    UNION
    ALL
    SELECT
      pajobreq.requisition,
      pajobreq.company,
      pajobreq.approval_date_08 AS approval_start_date,
      pajobreq.approval_desc_08 AS approval_desc,
      pajobreq.approval_emp_08 AS approval_emp,
      'APPRVL_08' AS requsition_approval_type_code,
      pajobreq.approval_desc_08 AS approver_position_title_desc,
      pajobreq.company AS lawson_company_num,
      pajobreq.process_level AS process_level_code
    FROM
      {{ params.param_hr_stage_dataset_name }}.pajobreq
    WHERE
      pajobreq.approval_date_08 <> DATE '1800-01-01'
    UNION
    ALL
    SELECT
      pajobreq.requisition,
      pajobreq.company,
      pajobreq.approval_date_09 AS approval_start_date,
      pajobreq.approval_desc_09 AS approval_desc,
      pajobreq.approval_emp_09 AS approval_emp,
      'APPRVL_09' AS requsition_approval_type_code,
      pajobreq.approval_desc_09 AS approver_position_title_desc,
      pajobreq.company AS lawson_company_num,
      pajobreq.process_level AS process_level_code
    FROM
      {{ params.param_hr_stage_dataset_name }}.pajobreq
    WHERE
      pajobreq.approval_date_09 <> DATE '1800-01-01'
    UNION
    ALL
    SELECT
      pajobreq.requisition,
      pajobreq.company,
      pajobreq.approval_date_10 AS approval_start_date,
      pajobreq.approval_desc_10 AS approval_desc,
      pajobreq.approval_emp_10 AS approval_emp,
      'APPRVL_10' AS requsition_approval_type_code,
      pajobreq.approval_desc_10 AS approver_position_title_desc,
      pajobreq.company AS lawson_company_num,
      pajobreq.process_level AS process_level_code
    FROM
      {{ params.param_hr_stage_dataset_name }}.pajobreq
    WHERE
      pajobreq.approval_date_10 <> DATE '1800-01-01'
  ) AS ee
GROUP BY
  1,
  2,
  3,
  4,
  upper(ee.approval_desc),
  6,
  7,
  upper(ee.requsition_approval_type_code),
  9,
  10;

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.req_appr_stg_wrk2;

INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.req_appr_stg_wrk2 (
    requisition_approval_type_code,
    requisition_sid,
    valid_from_date,
    approval_start_date,
    approver_employee_num,
    approval_end_date,
    approval_desc,
    approver_position_title_desc,
    lawson_company_num,
    process_level_code,
    active_dw_ind,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  tgt.requsition_approval_type_code,
  tgt.requisition_sid,
  current_date() AS valid_from_date,
  tgt.approval_start_date,
  tgt.approver_employee_num,
  tgt.approval_end_date,
  tgt.approval_description,
  tgt.approver_position_title_desc,
  tgt.lawson_company_num,
  tgt.process_level_code,
  tgt.active_dw_ind,
  'L' AS source_system_code,
  current_ts AS dw_last_update_time
FROM
  (
    SELECT
      ee.requisition,
      ee.company,
      ee.approval_start_date,
      ee.approval_emp AS approver_employee_num,
      ee.approval_end_date,
      ee.approval_desc,
      ee.approver_position_title_desc,
      ee.requsition_approval_type_code,
      req.requisition_sid AS requisition_sid,
      CASE
        WHEN upper(ee.requsition_approval_type_code) = 'APPRVL_01' THEN 'FIRST APPROVAL'
        WHEN upper(ee.requsition_approval_type_code) = 'APPRVL_02' THEN 'SECOND APPROVAL'
        WHEN upper(ee.requsition_approval_type_code) = 'APPRVL_03' THEN 'THIRD APPROVAL'
        WHEN upper(ee.requsition_approval_type_code) = 'APPRVL_04' THEN 'FOURTH APPROVAL'
        WHEN upper(ee.requsition_approval_type_code) = 'APPRVL_05' THEN 'FIFTH APPROVAL'
        WHEN upper(ee.requsition_approval_type_code) = 'APPRVL_06' THEN 'SIXTH APPROVAL'
        WHEN upper(ee.requsition_approval_type_code) = 'APPRVL_07' THEN 'SEVENTH APPROVAL'
        WHEN upper(ee.requsition_approval_type_code) = 'APPRVL_08' THEN 'EIGHTH APPROVAL'
        WHEN upper(ee.requsition_approval_type_code) = 'APPRVL_09' THEN 'NINETH APPROVAL'
        WHEN upper(ee.requsition_approval_type_code) = 'APPRVL_10' THEN 'TENTH APPROVAL'
      END AS approval_description,
      ee.lawson_company_num,
      ee.process_level_code,
      ee.active_dw_ind
    FROM
      {{ params.param_hr_stage_dataset_name }}.req_appr_stg_wrk1 AS ee
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition AS req ON trim(CAST(coalesce(ee.company, 9999) as STRING)) = trim(
        CAST(coalesce(req.lawson_company_num, 9999) as STRING)
      )
      AND trim(CAST(coalesce(ee.requisition, 9999) as STRING)) = trim(
        CAST(coalesce(req.requisition_num, 9999) as STRING)
      )
      AND req.valid_to_date = DATETIME("9999-12-31 23:59:59")
  ) AS tgt;

BEGIN TRANSACTION;

UPDATE
  {{ params.param_hr_core_dataset_name }}.requisition_approval_stage AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.req_appr_stg_wrk2 AS stg
WHERE
  tgt.requisition_sid = stg.requisition_sid
  AND trim(tgt.requisition_approval_type_code) = trim(stg.requisition_approval_type_code)
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND (
    tgt.approval_start_date <> stg.approval_start_date
    OR tgt.approver_employee_num <> stg.approver_employee_num
    OR tgt.approval_desc <> stg.approval_desc
    OR tgt.process_level_code <> stg.process_level_code
    OR tgt.approver_position_title_desc <> stg.approver_position_title_desc
  )
  AND tgt.source_system_code = 'L';

UPDATE
  {{ params.param_hr_core_dataset_name }}.requisition_approval_stage AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
WHERE
  (
    tgt.requisition_sid,
    tgt.requisition_approval_type_code
  ) NOT IN(
    SELECT
      AS STRUCT req_appr_stg_wrk2.requisition_sid,
      req_appr_stg_wrk2.requisition_approval_type_code
    FROM
      {{ params.param_hr_stage_dataset_name }}.req_appr_stg_wrk2
  )
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
AND tgt.source_system_code = 'L';

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.requisition_approval_stage (
    requisition_approval_type_code,
    requisition_sid,
    valid_from_date,
    approval_start_date,
    approver_employee_num,
    valid_to_date,
    approval_desc,
    approver_position_title_desc,
    lawson_company_num,
    process_level_code,
    active_dw_ind,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  stg.requisition_approval_type_code,
  stg.requisition_sid,
  --stg.valid_From_Date,
  current_ts,
  stg.approval_start_date,
  stg.approver_employee_num,
  --stg.approval_end_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  stg.approval_desc,
  stg.approver_position_title_desc,
  stg.lawson_company_num,
  stg.process_level_code,
  stg.active_dw_ind,
  stg.source_system_code,
  stg.dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.req_appr_stg_wrk2 AS stg
WHERE
  (
    trim(stg.requisition_approval_type_code),
    stg.requisition_sid,
    stg.approval_start_date,
    stg.approver_employee_num,
    stg.process_level_code
  ) NOT IN(
    SELECT
      AS STRUCT trim(
        requisition_approval_stage.requisition_approval_type_code
      ),
      requisition_approval_stage.requisition_sid,
      requisition_approval_stage.approval_start_date,
      requisition_approval_stage.approver_employee_num,
      requisition_approval_stage.process_level_code
    FROM
      {{ params.param_hr_core_dataset_name }}.requisition_approval_stage
    WHERE
      requisition_approval_stage.valid_to_date = DATETIME("9999-12-31 23:59:59")
  );

/* Test Unique Primary Index constraint set in Terdata */
SET
  DUP_COUNT = (
    select
      count(*)
    from
      (
        select
          requisition_approval_type_code,
          requisition_sid,
          valid_from_date
        from
          {{ params.param_hr_core_dataset_name }}.requisition_approval_stage
        group by
          requisition_approval_type_code,
          requisition_sid,
          valid_from_date
        having
          count(*) > 1
      )
  );

IF DUP_COUNT <> 0 THEN ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat(
  'Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.employee_Action_Detai{{ params.param_hr_core_dataset_name }}.requisition_approval_stagel'
);

ELSE COMMIT TRANSACTION;

END IF;

END;