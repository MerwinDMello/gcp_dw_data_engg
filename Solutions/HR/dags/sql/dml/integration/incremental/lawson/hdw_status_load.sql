BEGIN
DECLARE
  dup_count int64;
DECLARE
  current_ts datetime;
DECLARE
  lv_par string;
SET
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND) ;
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.status_wk1;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.status_wk1
SELECT
  a.company,
  a.a_field,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM (
  SELECT
    DISTINCT hrempusf.company,
    hrempusf.a_field
  FROM
    {{ params.param_hr_stage_dataset_name }}.hrempusf
  WHERE
    UPPER(hrempusf.field_key) = '88' ) AS a ;
SET
  lv_par = "trim(coalesce(cast(Company as string),''))  ||\'-\'|| trim(coalesce(A_Field ,''))||\'-\'||\'AUX\'||\'-\'||\'00000\'";
CALL
  `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}',
    'status_wk1',
    lv_par,
    'Status');
SET
  lv_par = "trim(coalesce(cast(Company as string),''))  ||\'-\'|| trim(coalesce(Req_Status ,''))||\'-\'||\'REQ\'||\'-\'||  case when trim(coalesce(process_level ,''))  = ''  then \'00000\' else trim(process_level) end";
CALL
  `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}',
    'pajobreq',
    lv_par,
    'Status');
SET
  lv_par = "trim(coalesce(cast(Company as string),''))  ||\'-\'|| trim(coalesce(Emp_Status ,''))||\'-\'||\'EMP\'||\'-\'||\'00000\'";
CALL
  `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}',
    'emstatus',
    lv_par,
    'Status');
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.status_wrk;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.status_wrk (status_sid,
    eff_from_date,
    hr_company_sid,
    lawson_company_num,
    status_type_code,
    status_code,
    status_desc,
    active_dw_ind,
    eff_to_date,
    process_level_code,
    security_key_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  status_sid,
  DATE(current_ts) AS eff_from_date,
  hr_company_sid,
  lawson_company_num,
  status_type_code,
  status_code,
  status_desc,
  'Y' AS active_dw_ind,
  CAST('9999-12-31' AS date) AS eff_to_date,
  process_level_code AS process_level_code,
  security_key_text,
  source_system_code,
  current_ts AS dw_last_update_date_time
FROM (
  SELECT
    xwlk.sk AS status_sid,
    comp.hr_company_sid,
    pj.company AS lawson_company_num,
    'REQ' AS status_type_code,
    TRIM(pj.req_status) AS status_code,
    TRIM(pc.description) AS status_desc,
    pj.process_level AS process_level_code,
    CONCAT(SUBSTR('00000', 1, 5 - LENGTH(TRIM(CAST(company AS string)))), TRIM(CAST(company AS string)), '-', '00000', '-', '00000') AS security_key_text,
    'L' AS source_system_code
  FROM
    {{ params.param_hr_stage_dataset_name }}.pajobreq AS pj
  INNER JOIN
    {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
  ON
    UPPER(SUBSTR(CONCAT(TRIM(COALESCE(CAST(pj.company AS string), '')), '-', TRIM(COALESCE(pj.req_status, '')), '-', 'REQ', '-',
          CASE
            WHEN UPPER(TRIM(COALESCE(pj.process_level, ''))) = '' THEN '00000'
          ELSE
          TRIM(pj.process_level)
        END
          ), 1, 255)) = UPPER(xwlk.sk_source_txt)
    AND UPPER(xwlk.sk_type) = 'STATUS'
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.hr_company AS comp
  ON
    pj.company = comp.lawson_company_num
    AND UPPER(comp.active_dw_ind) = 'Y'
    AND UPPER(comp.source_system_code) = 'L'
  LEFT OUTER JOIN
    {{ params.param_hr_stage_dataset_name }}.pcodes AS pc
  ON
    TRIM(COALESCE(pj.req_status, '')) = TRIM(pc.code)
    AND TRIM(pc.type) = 'RQ'
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8
  UNION DISTINCT
  SELECT
    xwlk.sk AS status_sid,
    comp.hr_company_sid,
    es.company AS lawson_company_num,
    'EMP' AS status_type_code,
    TRIM(es.emp_status) AS status_code,
    TRIM(es.description) AS status_desc,
    '00000' AS process_level_code,
    CONCAT(SUBSTR('00000', 1, 5 - LENGTH(TRIM(CAST(es.company AS string)))), TRIM(CAST(es.company AS string)), '-', '00000', '-', '00000') AS security_key_text,
    'L' AS source_system_code
  FROM
    {{ params.param_hr_stage_dataset_name }}.emstatus AS es
  INNER JOIN
    {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
  ON
    UPPER(SUBSTR(CONCAT(TRIM(COALESCE(CAST(es.company AS string), '')), '-', TRIM(COALESCE(es.emp_status, '')), '-', 'EMP', '-', '00000'), 1, 255)) = UPPER(xwlk.sk_source_txt)
    AND UPPER(xwlk.sk_type) = 'STATUS'
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.hr_company AS comp
  ON
    es.company = comp.lawson_company_num
    AND UPPER(comp.active_dw_ind) = 'Y'
    AND UPPER(comp.source_system_code) = 'L'
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8
  UNION DISTINCT
  SELECT
    xwlk.sk AS status_sid,
    comp.hr_company_sid,
    hus.company AS lawson_company_num,
    'AUX' AS status_type_code,
    TRIM(hus.a_field) AS status_code,
    TRIM(pcd.description) AS status_desc,
    '00000' AS process_level_code,
    CONCAT(SUBSTR('00000', 1, 5 - LENGTH(TRIM(CAST(hus.company AS string)))), TRIM(CAST(hus.company AS string)), '-', '00000', '-', '00000') AS security_key_text,
    'L' AS source_system_code
  FROM
    {{ params.param_hr_stage_dataset_name }}.hrempusf AS hus
  INNER JOIN
    {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
  ON
    UPPER(SUBSTR(CONCAT(TRIM(COALESCE(CAST(hus.company AS string), '')), '-', TRIM(COALESCE(hus.a_field, '')), '-', 'AUX', '-', '00000'), 1, 255)) = UPPER(xwlk.sk_source_txt)
    AND UPPER(xwlk.sk_type) = 'STATUS'
  INNER JOIN
    {{ params.param_hr_stage_dataset_name }}.pcodes AS pcd
  ON
    hus.field_key = pcd.type
    AND TRIM(hus.a_field) = pcd.code
    AND TRIM(hus.field_key) = '88'
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.hr_company AS comp
  ON
    hus.company = comp.lawson_company_num
    AND UPPER(comp.active_dw_ind) = 'Y'
    AND UPPER(comp.source_system_code) = 'L'
  WHERE
    TRIM(hus.field_key) = '88'
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8 ) ;
UPDATE
  {{ params.param_hr_core_dataset_name }}.status AS st
SET
  valid_to_date = DATE_SUB(current_ts, INTERVAL 1 second),
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.status_wrk AS stg
WHERE
  st.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND st.status_sid = stg.status_sid
  AND st.source_system_code = stg.source_system_code
  AND (st.hr_company_sid <> stg.hr_company_sid
    OR UPPER(TRIM(COALESCE(st.status_type_code, ''))) <> UPPER(TRIM(COALESCE(stg.status_type_code, '')))
    OR UPPER(TRIM(COALESCE(st.status_desc, ''))) <> UPPER(TRIM(COALESCE(stg.status_desc, '')))
    OR st.lawson_company_num <> stg.lawson_company_num
    OR UPPER(TRIM(COALESCE(st.status_code, ''))) <> UPPER(TRIM(COALESCE(stg.status_code, '')))
    OR TRIM(st.process_level_code) <> TRIM(stg.process_level_code));
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.status (status_sid,
    valid_from_date,
    hr_company_sid,
    lawson_company_num,
    status_type_code,
    status_code,
    status_desc,
    active_dw_ind,
    valid_to_date,
    process_level_code,
    security_key_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CAST(stg.status_sid AS int64),
  current_ts,
  CAST(stg.hr_company_sid AS int64),
  stg.lawson_company_num,
  stg.status_type_code,
  stg.status_code,
  stg.status_desc,
  stg.active_dw_ind,
  DATETIME("9999-12-31 23:59:59"),
  stg.process_level_code,
  stg.security_key_text,
  stg.source_system_code,
  current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.status_wrk AS stg
WHERE
  ( cast(stg.status_sid as int64),
    cast(stg.hr_company_sid as int64),
    stg.lawson_company_num,
    UPPER(TRIM(COALESCE(stg.status_type_code, ''))),
    UPPER(TRIM(COALESCE(stg.status_code, ''))),
    UPPER(TRIM(COALESCE(stg.status_desc, ''))),
    TRIM(stg.process_level_code),
    TRIM(stg.source_system_code)) NOT IN(
  SELECT
    AS STRUCT status_sid,
    hr_company_sid,
    lawson_company_num,
    UPPER(TRIM(COALESCE(status_type_code, ''))),
    UPPER(TRIM(COALESCE(status_code, ''))),
    UPPER(TRIM(COALESCE(status_desc, ''))),
    TRIM(process_level_code),
    TRIM(source_system_code)
  FROM
    {{ params.param_hr_base_views_dataset_name }}.status
  WHERE
    DATE(valid_to_date) = date '9999-12-31' ) ;
UPDATE
  {{ params.param_hr_core_dataset_name }}.status AS tgt
SET
  valid_to_date = DATE_SUB(current_ts, INTERVAL 1 second),
  active_dw_ind = 'N'
WHERE
  tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND tgt.lawson_company_num <> 300
  AND (cast(tgt.status_sid as int64),
    cast(tgt.hr_company_sid as int64),
    tgt.lawson_company_num,
    UPPER(TRIM(COALESCE(tgt.status_type_code, ''))),
    UPPER(TRIM(COALESCE(tgt.status_code, ''))),
    UPPER(TRIM(COALESCE(tgt.status_desc, ''))),
    TRIM(tgt.process_level_code),
    TRIM(tgt.source_system_code)) NOT IN(
  SELECT
    AS STRUCT CAST(status_sid AS int64),
    CAST(hr_company_sid AS int64),
    lawson_company_num,
    UPPER(TRIM(COALESCE(status_type_code, ''))),
    UPPER(TRIM(COALESCE(status_code, ''))),
    UPPER(TRIM(COALESCE(status_desc, ''))),
    TRIM(process_level_code),
    TRIM(source_system_code)
  FROM
    {{ params.param_hr_stage_dataset_name }}.status_wrk );
END
  ;