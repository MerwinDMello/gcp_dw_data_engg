BEGIN 
DECLARE  current_ts DATETIME;
DECLARE  DUP_COUNT INT64;
DECLARE  col_val string ;
SET current_ts = TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
SET col_val = "trim(cast(PFIWORKUNIT as string))||"||"\'-L\'";

TRUNCATE TABLE   {{ params.param_hr_stage_dataset_name }}.hr_workunit_wrk;
CALL  `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}',
    'pfiworkunit', col_val,'HR_Workunit');

INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.hr_workunit_wrk (workunit_sid,
    valid_from_date,
    valid_to_date,
    workunit_num,
    application_data_area_text,
    application_key_text,
    application_value_text,
    service_definition_text,
    flow_definition_text,
    flow_version_num,
    actor_id_text,
    authenticated_actor_text,
    work_title_text,
    filter_key_text,
    filter_value_text,
    execution_start_date_time,
    start_date_time,
    close_date_time,
    status_id,
    criterion_1_id_text,
    criterion_2_id_text,
    criterion_3_id_text,
    source_type_text,
    source_1_text,
    configuration_name,
    last_activity_num,
    last_message_num,
    last_variable_seq_num,
    classic_workunit_id,
    classic_workunit_entered_ind,
    key_field_value_1_text,
    key_field_value_2_text,
    key_field_value_3_text,
    key_field_value_4_text,
    key_field_value_5_text,
    key_field_value_6_text,
    key_field_value_7_text,
    key_field_value_8_text,
    key_field_value_9_text,
    key_field_value_10_text,
    lawson_company_num,
    process_level_code,
    active_dw_ind,
    source_system_code,
    dw_last_update_date_time)
SELECT
  xwlk.sk AS workunit_sid,
  current_ts AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  a.pfiworkunit AS workunit_num,
  a.appsdataarea AS application_data_area_text,
  a.appskey AS application_key_text,
  a.appsvalue AS application_value_text,
  a.servicedefinition AS service_definition_text,
  a.flowdefinition AS flow_definition_text,
  a.flowversion AS flow_version_num,
  a.actor AS actor_id_text,
  a.authenticatedactor AS authenticated_actor_text,
  a.worktitle AS work_title_text,
  a.pfifilterkey AS filter_key_text,
  a.filtervalue AS filter_value_text,
  CAST(a.executionstartdate AS DATETIME) AS execution_start_date_time,
  CAST(a.startdate AS DATETIME) AS start_date_time,
  CAST(a.closedate AS DATETIME) AS close_date_time,
  a.status AS status_id,
  a.criterion1 AS criterion_1_id_text,
  a.criterion2 AS criterion_2_id_text,
  a.criterion3 AS criterion_3_id_text,
  a.sourcetype AS source_type_text,
  a.source AS source_1_text,
  a.configurationname AS configuration_name,
  a.lastactivity AS last_activity_num,
  a.lastmessage AS last_message_num,
  a.lastvariableseqnbr AS last_variable_seq_num,
  CAST(a.classicworkunit AS int64) AS classic_workunit_id,
  a.classicworkunitentered AS classic_workunit_entered_ind,
  a.keyfield1value AS key_field_value_1_text,
  a.keyfield2value AS key_field_value_2_text,
  a.keyfield3value AS key_field_value_3_text,
  a.keyfield4value AS key_field_value_4_text,
  a.keyfield5value AS key_field_value_5_text,
  a.keyfield6value AS key_field_value_6_text,
  a.keyfield7value AS key_field_value_7_text,
  a.keyfield8value AS key_field_value_8_text,
  a.keyfield9value AS key_field_value_9_text,
  a.keyfield10value AS key_field_value_10_text,
  CASE
    WHEN CAST(com.lawson_company_num AS int64) <> 0 THEN CAST(com.lawson_company_num AS int64)
    WHEN CAST(com.lawson_company_num AS int64) = 0 AND UPPER(b.pfiworkunitvariable) = 'COMPANY' 
         THEN CAST((CASE WHEN TRIM(b.variablevalue)='' THEN '0' ELSE b.variablevalue END) AS int64)
  ELSE
  0
END
  AS lawson_company_num,
  '00000' AS process_level_code,
  'Y' AS active_dw_ind,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.pfiworkunit AS a
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  UPPER(CONCAT(TRIM(CAST(a.pfiworkunit AS string)), '-', 'L')) = UPPER(xwlk.sk_source_txt)
  AND xwlk.sk_type = 'HR_Workunit'
LEFT OUTER JOIN
  {{ params.param_hr_stage_dataset_name }}.pfiworkunitvariable AS b
ON
  a.pfiworkunit = b.pfiworkunit
  AND UPPER(b.pfiworkunitvariable) IN ('COMPANY')
LEFT OUTER JOIN (
  SELECT
    DISTINCT a_0.pfiworkunit,
    CASE
      WHEN UPPER(b_0.pfiworkunitvariable) = 'PJR_COMPANY' THEN b_0.variablevalue
      WHEN UPPER(b_0.pfiworkunitvariable) = 'INBCOMP' THEN b_0.variablevalue
      WHEN UPPER(b_0.pfiworkunitvariable) = 'OCATEXEC1' THEN SUBSTR(b_0.variablevalue, 1, 4)
      WHEN UPPER(b_0.pfiworkunitvariable) = 'OCATREC1' THEN SUBSTR(b_0.variablevalue, 1, 4)
    ELSE
    CAST(0 AS STRING)
  END
    AS lawson_company_num
  FROM
    {{ params.param_hr_stage_dataset_name }}.pfiworkunit AS a_0
  LEFT OUTER JOIN
    {{ params.param_hr_stage_dataset_name }}.pfiworkunitvariable AS b_0
  ON
    a_0.pfiworkunit = b_0.pfiworkunit
    AND UPPER(b_0.pfiworkunitvariable) IN ('PJR_COMPANY','INBCOMP','OCATEXEC1','OCATREC1') ) AS com
ON
  a.pfiworkunit = com.pfiworkunit
GROUP BY
  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45;

SET col_val = "trim(cast(PfiWorkunit as string))||"||"\'-B\'";

CALL  `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}',
    'ats_cust_pfiworkunit_stg', col_val,'HR_Workunit');

INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.hr_workunit_wrk (workunit_sid,
    valid_from_date,
    valid_to_date,
    workunit_num,
    application_data_area_text,
    application_key_text,
    application_value_text,
    service_definition_text,
    flow_definition_text,
    flow_version_num,
    actor_id_text,
    authenticated_actor_text,
    work_title_text,
    filter_key_text,
    filter_value_text,
    execution_start_date_time,
    start_date_time,
    close_date_time,
    status_id,
    criterion_1_id_text,
    criterion_2_id_text,
    criterion_3_id_text,
    source_type_text,
    source_1_text,
    configuration_name,
    last_activity_num,
    last_message_num,
    last_variable_seq_num,
    classic_workunit_id,
    classic_workunit_entered_ind,
    key_field_value_1_text,
    key_field_value_2_text,
    key_field_value_3_text,
    key_field_value_4_text,
    key_field_value_5_text,
    key_field_value_6_text,
    key_field_value_7_text,
    key_field_value_8_text,
    key_field_value_9_text,
    key_field_value_10_text,
    lawson_company_num,
    process_level_code,
    active_dw_ind,
    source_system_code,
    dw_last_update_date_time)
SELECT
  xwlk.sk AS workunit_sid,
  current_ts AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  a.pfiworkunit AS workunit_num,
  a.appsdataarea AS application_data_area_text,
  a.appskey AS application_key_text,
  a.appsvalue AS application_value_text,
  a.servicedefinition AS service_definition_text,
  a.flowdefinition AS flow_definition_text,
  a.flowversion AS flow_version_num,
  a.actor AS actor_id_text,
  a.authenticatedactor AS authenticated_actor_text,
  a.worktitle AS work_title_text,
  a.pfifilterkey AS filter_key_text,
  a.filtervalue AS filter_value_text,
  cast(left(a.executionstartdate,19) AS datetime FORMAT 'YYYY-MM-DD HH24:MI:SS')as executionstartdate,
  cast(left(a.startdate,19) AS datetime FORMAT 'YYYY-MM-DD HH24:MI:SS') as startdate,
  cast(left(a.closedate,19) AS datetime FORMAT 'YYYY-MM-DD HH24:MI:SS')as closedate,
  a.status AS status_id,
  a.criterion1 AS criterion_1_id_text,
  a.criterion2 AS criterion_2_id_text,
  a.criterion3 AS criterion_3_id_text,
  a.sourcetype AS source_type_text,
  a.source AS source_1_text,
  a.configurationname AS configuration_name,
  a.lastactivity AS last_activity_num,
  a.lastmessage AS last_message_num,
  a.lastvariableseqnbr AS last_variable_seq_num,
  CAST(a.classicworkunit AS int64) AS classic_workunit_id,
  cast(NULL as string) AS classic_workunit_entered_ind,
  a.keyfield1value AS key_field_value_1_text,
  a.keyfield2value AS key_field_value_2_text,
  a.keyfield3value AS key_field_value_3_text,
  a.keyfield4value AS key_field_value_4_text,
  a.keyfield5value AS key_field_value_5_text,
  a.keyfield6value AS key_field_value_6_text,
  a.keyfield7value AS key_field_value_7_text,
  a.keyfield8value AS key_field_value_8_text,
  a.keyfield9value AS key_field_value_9_text,
  a.keyfield10value AS key_field_value_10_text,
  COALESCE (CASE
    WHEN var.variablevalue IS NOT NULL AND UPPER(var.variablevalue) <> '0' THEN org.hcaorgunitcompany
  ELSE
  0
END, 0)
  AS lawson_company_num,
  '00000' AS process_level_code,
  'Y' AS active_dw_ind,
  'B' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_pfiworkunit_stg AS a
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  UPPER(CONCAT(TRIM(CAST(a.pfiworkunit AS string)), '-', 'B')) = UPPER(xwlk.sk_source_txt)
  AND xwlk.sk_type = 'HR_Workunit'
LEFT OUTER JOIN
  {{ params.param_hr_stage_dataset_name }}.ats_cust_pfiworkunitvariable_stg AS var
ON
  a.pfiworkunit = var.pfiworkunit
  AND UPPER(var.pfiworkunitvariable) = 'HRORGANIZATIONUNIT'
LEFT OUTER JOIN
  {{ params.param_hr_stage_dataset_name }}.ats_cust_hrorganizationunit_stg AS org
ON
  CAST(var.variablevalue AS int64) = org.hrorganizationunit QUALIFY ROW_NUMBER() OVER (PARTITION BY workunit_sid, valid_from_date ORDER BY a.repset_variation_id DESC) = 1 ;
END