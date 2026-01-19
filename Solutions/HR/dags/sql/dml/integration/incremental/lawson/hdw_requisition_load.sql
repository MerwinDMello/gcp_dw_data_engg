BEGIN DECLARE current_ts DATETIME;

DECLARE DUP_COUNT INT64;

SET
  current_ts = TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.requisition_wrk0;

CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen(
  "{{ params.param_hr_stage_dataset_name }}",
  'pajobreq',
  "TRIM(COALESCE(CAST(Company AS STRING),'')) ||'-'|| TRIM(COALESCE(CAST(Requisition AS STRING),''))",
  'Requisition'
);

CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen(
  "{{ params.param_hr_stage_dataset_name }}",
  'pajobreq',
  "TRIM(COALESCE(CAST(Company AS STRING),''))",
  'HR_Company'
);

CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen(
  "{{ params.param_hr_stage_dataset_name }}",
  'pajobreq',
  "COALESCE(Company ,0) ||'-'||TRIM(COALESCE(CAST(Rep_Employee AS STRING),'0'))",
  'Employee'
);

INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.requisition_wrk0 (
    row_rank,
    sk,
    eff_from_date,
    date_stamp,
    hr_company_sid,
    application_status_sid,
    lawson_company_num,
    process_level_code,
    location_code,
    requisition_num,
    requisition_desc,
    requisition_eff_date,
    requisition_open_date,
    requisition_closed_date,
    requisition_origination_date,
    originator_login_3_4_code,
    position_needed_date,
    job_opening_cnt,
    open_fte_percent,
    filled_fte_percent,
    last_update_date,
    replacement_employee_num,
    replacement_employee_sid,
    work_schedule_code,
    union_code,
    active_dw_ind,
    security_key_text,
    source_system_code,
    replacement_flag,
    special_requirement_text,
    dw_last_update_date_time
  )
SELECT
  DENSE_RANK() OVER (
    PARTITION BY req.company,
    req.requisition
    ORDER BY
      req.date_stamp DESC,
      req.time_stamp DESC
  ) AS row_rank,
  xwlk.sk AS sk,
  DATE(current_ts) AS eff_from_date,
  req.date_stamp AS date_stamp,
  CAST(cmpy_lkp_sid.sk AS INT64) AS hr_company_sid,
  CAST(stat.status_sid AS INT64) AS application_status_sid,
  req.company AS lawson_company_num,
  CASE
    WHEN COALESCE(CAST(TRIM(req.process_level) AS string), '') = '' THEN '00000'
    ELSE req.process_level
  END AS process_level_code,
  TRIM(req.locat_code) AS location_code,
  req.requisition AS requisition_num,
  trim(req.description) AS requisition_desc,
  req.effect_date AS requisition_eff_date,
  req.open_date AS requisition_open_date,
  CASE
    WHEN req.closed_date = PARSE_DATE('%Y-%m-%d', '1800-01-01') THEN PARSE_DATE('%Y-%m-%d', '9999-12-31')
    ELSE req.closed_date
  END AS requisition_closed_date,
  CASE
    WHEN req.approval_date_01 = PARSE_DATE('%Y-%m-%d', '1800-01-01') THEN REQ.Request_Date
    WHEN req.approval_date_01 = PARSE_DATE('%Y-%m-%d', '1950-01-01') THEN REQ.Request_Date
    ELSE req.approval_date_01
  END AS requisition_origination_date,
  LEFT(TRIM(znm.originator), 7) AS originator_login_3_4_code,
  req.date_needed AS position_needed_date,
  req.openings AS job_opening_cnt,
  req.open_fte AS open_fte_percent,
  req.filled_fte AS filled_fte_percent,
  req.date_stamp AS last_update_date,
  req.rep_employee AS replacement_employee_num,
  CASE
    WHEN req.rep_employee = 0 THEN 0
    ELSE CAST(emp_sid.sk AS INT64)
  END AS replacement_employee_sid,
  TRIM(req.work_sched) AS work_schedule_code,
  TRIM(req.union_code) AS union_code,
  CASE
    WHEN DENSE_RANK() OVER (
      PARTITION BY req.company,
      req.requisition
      ORDER BY
        req.date_stamp DESC,
        req.time_stamp DESC
    ) = 1 THEN 'Y'
    ELSE 'N'
  END AS active_dw_ind,
  CONCAT(
    SUBSTR(
      '00000',
      1,
      5 - LENGTH(CAST(req.company AS string))
    ),
    CAST(req.company AS string),
    '-',
    '00000',
    '-',
    '00000'
  ) AS security_key_text,
  'L' AS source_system_code,
  TRIM(replacement) AS replacement_flag,
  TRIM(req.spec_req3) AS special_requirement_text,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.pajobreq AS req
  INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON UPPER(
    TRIM(
      CONCAT(
        CAST(req.company AS string),
        '-',
        CAST(req.requisition AS string)
      )
    )
  ) = UPPER(TRIM(xwlk.sk_source_txt))
  AND UPPER(xwlk.sk_type) = 'REQUISITION'
  LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS cmpy_lkp_sid ON UPPER(TRIM(CAST(req.company AS string))) = UPPER(TRIM(cmpy_lkp_sid.sk_source_txt))
  AND UPPER(CAST(cmpy_lkp_sid.sk_type AS string)) = 'HR_COMPANY'
  LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stat ON UPPER(TRIM(req.app_status)) = UPPER(TRIM(stat.status_code))
  AND req.company = stat.lawson_company_num
  LEFT OUTER JOIN (
    SELECT
      znmetrics.originator,
      znmetrics.key_string,
      znmetrics.start_date,
      znmetrics.start_time
    FROM
      {{ params.param_hr_stage_dataset_name }}.znmetrics QUALIFY ROW_NUMBER() OVER (
        PARTITION BY znmetrics.key_string
        ORDER BY
          znmetrics.start_date,
          znmetrics.start_time
      ) = 1
  ) AS znm ON CAST(
    SAFE_CAST(SUBSTR(TRIM(znm.key_string), 5, 9) AS BIGNUMERIC) AS INT64
  ) = req.requisition
  AND CAST(
    SAFE_CAST(SUBSTR(TRIM(znm.key_string), 1, 4) AS BIGNUMERIC) AS INT64
  ) = req.company
  LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS emp_sid ON UPPER(
    TRIM(
      CONCAT(
        CAST(req.company AS string),
        '-',
        CAST(req.rep_employee AS string)
      )
    )
  ) = UPPER(TRIM(emp_sid.sk_source_txt))
  AND UPPER(emp_sid.sk_type) = 'EMPLOYEE'
  LEFT OUTER JOIN (
    SELECT
      DISTINCT requisition.requisition_sid
    FROM
      {{ params.param_hr_base_views_dataset_name }}.requisition
  ) AS tgt ON xwlk.sk = tgt.requisition_sid
  AND UPPER(xwlk.sk_type) = 'REQUISITION';

BEGIN TRANSACTION;

UPDATE
  {{ params.param_hr_core_dataset_name }}.requisition AS req
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
FROM
  (
    SELECT
      requisition_wrk0.sk,
      requisition_wrk0.hr_company_sid,
      requisition_wrk0.application_status_sid,
      requisition_wrk0.lawson_company_num,
      requisition_wrk0.process_level_code,
      requisition_wrk0.location_code,
      requisition_wrk0.requisition_num,
      requisition_wrk0.requisition_desc,
      requisition_wrk0.requisition_eff_date,
      requisition_wrk0.requisition_open_date,
      requisition_wrk0.requisition_closed_date,
      requisition_wrk0.requisition_origination_date,
      requisition_wrk0.originator_login_3_4_code,
      requisition_wrk0.position_needed_date,
      requisition_wrk0.job_opening_cnt,
      requisition_wrk0.open_fte_percent,
      requisition_wrk0.filled_fte_percent,
      requisition_wrk0.last_update_date,
      requisition_wrk0.replacement_employee_num,
      requisition_wrk0.replacement_employee_sid,
      requisition_wrk0.work_schedule_code,
      requisition_wrk0.union_code,
      requisition_wrk0.source_system_code,
      requisition_wrk0.replacement_flag,
      requisition_wrk0.special_requirement_text
    FROM
      {{ params.param_hr_stage_dataset_name }}.requisition_wrk0
    WHERE
      requisition_wrk0.row_rank = 1
  ) AS stg
WHERE
  date(req.valid_to_date) = "9999-12-31"
  AND req.source_system_code = 'L'
  AND req.requisition_sid = stg.sk
  AND (
    COALESCE(CAST(stg.hr_company_sid AS string), '') <> COALESCE(CAST(req.hr_company_sid AS string), '')
    OR COALESCE(CAST(stg.application_status_sid AS string), '') <> COALESCE(CAST(req.application_status_sid AS string), '')
    OR stg.lawson_company_num <> req.lawson_company_num
    OR COALESCE(CAST(stg.location_code AS string), '') <> COALESCE(CAST(req.location_code AS string), '')
    OR stg.requisition_num <> req.requisition_num
    OR UPPER(TRIM(COALESCE(stg.requisition_desc, ''))) <> UPPER(TRIM(COALESCE(req.requisition_desc, '')))
    OR COALESCE(stg.requisition_open_date, DATE '0001-01-01') <> COALESCE(req.requisition_open_date, DATE '0001-01-01')
    OR COALESCE(stg.requisition_closed_date, DATE '0001-01-01') <> COALESCE(req.requisition_closed_date, DATE '0001-01-01')
    OR COALESCE(
      stg.requisition_origination_date,
      DATE '0001-01-01'
    ) <> COALESCE(
      req.requisition_origination_date,
      DATE '0001-01-01'
    )
    OR UPPER(
      TRIM(COALESCE(stg.originator_login_3_4_code, ''))
    ) <> UPPER(
      TRIM(COALESCE(req.originator_login_3_4_code, ''))
    )
    OR COALESCE(stg.position_needed_date, DATE '0001-01-01') <> COALESCE(req.position_needed_date, DATE '0001-01-01')
    OR stg.job_opening_cnt <> req.job_opening_cnt
    OR stg.open_fte_percent <> req.open_fte_percent
    OR stg.filled_fte_percent <> req.filled_fte_percent
    OR UPPER(TRIM(stg.process_level_code)) <> UPPER(TRIM(req.process_level_code))
    OR COALESCE(stg.requisition_eff_date, DATE '0001-01-01') <> COALESCE(req.requisition_eff_date, DATE '0001-01-01')
    OR stg.replacement_employee_num <> req.replacement_employee_num
    OR stg.replacement_employee_sid <> req.replacement_employee_sid
    OR UPPER(TRIM(stg.work_schedule_code)) <> UPPER(TRIM(req.work_schedule_code))
    OR UPPER(TRIM(COALESCE(stg.union_code, ''))) <> UPPER(TRIM(COALESCE(req.union_code, '')))
    OR UPPER(TRIM(COALESCE(stg.source_system_code, 'X'))) <> UPPER(TRIM(COALESCE(req.source_system_code, 'X')))
    OR UPPER(TRIM(COALESCE(stg.replacement_flag, 'X'))) <> UPPER(TRIM(COALESCE(req.replacement_flag, 'X')))
    OR UPPER(
      TRIM(COALESCE(stg.special_requirement_text, 'X'))
    ) <> UPPER(
      TRIM(COALESCE(req.special_requirement_text, 'X'))
    )
  );

UPDATE
  {{ params.param_hr_core_dataset_name }}.requisition AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
WHERE
  date(tgt.valid_to_date) = "9999-12-31"
  AND UPPER(tgt.source_system_code) = 'L'
  AND (tgt.requisition_num || tgt.lawson_company_num) NOT IN(
    SELECT
      stg.requisition || stg.company
    FROM
      {{ params.param_hr_stage_dataset_name }}.pajobreq AS stg
  );

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.requisition (
    requisition_sid,
    valid_from_date,
    valid_to_date,
    hr_company_sid,
    application_status_sid,
    lawson_company_num,
    process_level_code,
    location_code,
    requisition_num,
    requisition_desc,
    requisition_eff_date,
    requisition_open_date,
    requisition_closed_date,
    requisition_origination_date,
    originator_login_3_4_code,
    position_needed_date,
    job_opening_cnt,
    open_fte_percent,
    filled_fte_percent,
    last_update_date,
    replacement_employee_num,
    replacement_employee_sid,
    work_schedule_code,
    union_code,
    active_dw_ind,
    security_key_text,
    source_system_code,
    replacement_flag,
    special_requirement_text,
    dw_last_update_date_time
  )
SELECT
  CAST(stg.sk AS INT64),
  current_ts,
  DATETIME("9999-12-31 23:59:59"),
  stg.hr_company_sid,
  stg.application_status_sid,
  stg.lawson_company_num,
  stg.process_level_code,
  stg.location_code,
  stg.requisition_num,
  stg.requisition_desc,
  stg.requisition_eff_date,
  stg.requisition_open_date,
  stg.requisition_closed_date,
  stg.requisition_origination_date,
  stg.originator_login_3_4_code,
  stg.position_needed_date,
  stg.job_opening_cnt,
  stg.open_fte_percent,
  stg.filled_fte_percent,
  stg.last_update_date,
  stg.replacement_employee_num,
  stg.replacement_employee_sid,
  stg.work_schedule_code,
  stg.union_code,
  stg.active_dw_ind,
  stg.security_key_text,
  stg.source_system_code,
  stg.replacement_flag,
  stg.special_requirement_text,
  current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.requisition_wrk0 AS stg
  LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition AS tgt ON tgt.requisition_sid = stg.sk
  AND DATE(tgt.valid_to_date) = "9999-12-31"
WHERE
  tgt.requisition_sid IS NULL;

SET
  DUP_COUNT = (
    SELECT
      COUNT(*)
    FROM
      (
        SELECT
          Requisition_SID,
          Valid_From_Date
        FROM
          {{ params.param_hr_core_dataset_name }}.requisition
        GROUP BY
          Requisition_SID,
          Valid_From_Date
        HAVING
          COUNT(*) > 1
      )
  );

IF DUP_COUNT <> 0 THEN ROLLBACK TRANSACTION;

RAISE USING MESSAGE = CONCAT(
  'Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.requisition'
);

ELSE COMMIT TRANSACTION;

END IF;

END