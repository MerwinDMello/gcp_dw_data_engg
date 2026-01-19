BEGIN DECLARE current_ts DATETIME;

SET
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

/* delete key_string value = 'undefined'*/
DELETE FROM
  {{ params.param_hr_stage_dataset_name }}.znmetrics
WHERE
  substr(trim(znmetrics.key_string), 1, 1) NOT IN(
    '1',
    '2',
    '3',
    '4',
    '5',
    '6',
    '7',
    '8',
    '9',
    '0'
  );

/*  Generate the surrogate keys for Requisition */
/*CALL {{ params.param_hr_core_dataset_name }}_procs.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'ZNMETRICS', 'TRIM(SUBSTR(KEY_STRING,1,4)) ||\'-\'|| Trim(CAST(TRIM(SUBSTR(KEY_STRING,5,9)) AS INTEGER))', 'Requisition');*/
CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen(
  '{{ params.param_hr_stage_dataset_name }}',
  'znmetrics',
  "SUBSTR(KEY_STRING,1,4) ||'-'|| LTRIM(TRIM(SUBSTR(KEY_STRING,5,9)),'0')",
  'Requisition'
);

/*  Truncate Worktable Table     */
TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.requisition_workflow_wrk;

/*  LOAD WORK TABLE WITH WORKING DATA */
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.requisition_workflow_wrk (
    requisition_sid,
    workflow_seq_num,
    valid_from_date,
    valid_to_date,
    workflow_workunit_num_text,
    workflow_activity_num,
    workflow_role_name,
    workflow_task_name,
    start_date,
    start_time,
    end_date,
    end_time,
    workflow_user_id_code,
    lawson_company_num,
    process_level_code,
    active_dw_ind,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  cast(xwlk.sk as INT64) AS requisition_sid,
  row_number() OVER (
    PARTITION BY xwlk.sk
    ORDER BY
      znm.start_date,
      znm.start_time,
      znm.hca_pfi_actvty
  ) AS workflow_seq_num,
  current_ts AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  lpad(trim(znm.workunit),8,'0') AS workflow_workunit_num_text,
  znm.hca_pfi_actvty AS workflow_activity_num,
  znm.wf_task AS workflow_role_name,
  znm.operation AS workflow_task_name,
  znm.start_date AS start_date,
  CAST(start_time as TIME) AS start_time,
  -- ,	ZNM.End_Date AS End_Date
  CASE
    WHEN znm.end_date = DATE '1800-01-01' THEN DATE '9999-12-31'
    ELSE znm.end_date
  END AS end_date,
  CAST(znm.end_time as TIME) AS end_time,
  znm.hca_wf_id AS workflow_user_id_code,
  CASE
    substr(znm.key_string, 1, 4)
    WHEN '' THEN 0
    ELSE CAST(substr(znm.key_string, 1, 4) as INT64)
  END AS lawson_company_num,
  '00000' AS process_level_code,
  'Y' AS active_dw_ind,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.znmetrics AS znm
  INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON concat(
    trim(substr(znm.key_string, 1, 4)),
    '-',
    trim(
      CAST(
        CASE
          trim(substr(znm.key_string, 5, 9))
          WHEN '' THEN 0
          ELSE CAST(trim(substr(znm.key_string, 5, 9)) as INT64)
        END as STRING
      )
    )
  ) = xwlk.sk_source_txt
  AND upper(xwlk.sk_type) = 'REQUISITION';

INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.requisition_workflow_wrk (
    requisition_sid,
    workflow_seq_num,
    valid_from_date,
    valid_to_date,
    workflow_workunit_num_text,
    workflow_activity_num,
    workflow_role_name,
    workflow_task_name,
    start_date,
    start_time,
    end_date,
    end_time,
    workflow_user_id_code,
    lawson_company_num,
    process_level_code,
    active_dw_ind,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  a.requisition_sid,
  row_number() OVER (
    PARTITION BY a.workflow_workunit_num_text
    ORDER BY
      a.workflow_activity_num
  ) AS workflow_seq_num,
  current_ts AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  cast(a.workflow_workunit_num_text as STRING),
  cast(a.workflow_activity_num as int64),
  a.workflow_role_name,
  a.workflow_task_name,
  date(a.start_date),
  a.start_time,
  date(a.end_date),
  a.end_time,
  a.workflow_user_id_code,
  a.lawson_company_num,
  a.process_level_code,
  a.active_dw_ind,
  a.source_system_code,
  a.dw_last_update_date_time
FROM
  (
    SELECT
      r.requisition_sid AS requisition_sid,
      current_ts AS valid_from_date,
      DATETIME("9999-12-31 23:59:59") AS valid_to_date,
      w.pfiworkunit AS workflow_workunit_num_text,
      m.pfiactivity AS workflow_activity_num,
      m.pfimetrics_pfitask_taskname AS workflow_role_name,
      m.actiontaken AS workflow_task_name,
      a_0.startdate AS start_date,
      CAST(
        substr(CAST(a_0.startdate as STRING), 12, 8) as TIME
      ) AS start_time,
      a_0.enddate AS end_date,
      CAST(
        substr(CAST(a_0.enddate as STRING), 12, 8) as TIME
      ) AS end_time,
      m.pfimetrics_pfiuserprofile AS workflow_user_id_code,
      r.lawson_company_num,
      '00000' AS process_level_code,
      'Y' AS active_dw_ind,
      'B' AS source_system_code,
      current_ts AS dw_last_update_date_time
    FROM
      {{ params.param_hr_stage_dataset_name }}.ats_cust_pfiworkunit_stg AS w
      INNER JOIN {{ params.param_hr_stage_dataset_name }}.ats_cust_pfimetrics_stg AS m ON (m.pfiworkunit) = w.pfiworkunit
      AND (m.repset_variation_id) = w.repset_variation_id
      INNER JOIN {{ params.param_hr_stage_dataset_name }}.ats_cust_pfiactivity_stg AS a_0 ON cast(m.pfiworkunit as string) = cast(a_0.pfiworkunit as string)
      AND cast(m.repset_variation_id as string) = cast(a_0.repset_variation_id as string)
      AND cast(m.pfiactivity as string) = cast(a_0.pfiactivity as string)
      LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.ats_cust_pfiworkunitvariable_stg AS v ON cast(w.pfiworkunit as string) = cast(v.pfiworkunit as string)
      AND cast(w.repset_variation_id as string) = cast(w.repset_variation_id as string)
      AND upper(v.pfiworkunitvariable) = 'JOBREQUISITION'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS rr
      /*Coalesce(Cast(v.variablevalue AS INT64),Cast(split(split(w.worktitle, ' ', [SAFE_ORDINAL(3)]), ':', [SAFE_ORDINAL(1)]) AS INT64)) = rr.requisition_num*/
      ON coalesce(
        cast(v.variablevalue as INT64),
        CASE
          split(split(w.worktitle, ' ') [SAFE_ORDINAL(3)], ':') [SAFE_ORDINAL(1)]
          WHEN '' THEN 0
          ELSE CAST(
            split(split(w.worktitle, ' ') [SAFE_ORDINAL(3)], ':') [SAFE_ORDINAL(1)] as INT64
          )
        END
      ) = rr.requisition_num
      AND upper(rr.source_system_code) = 'B'
      AND rr.valid_to_date = DATETIME("9999-12-31 23:59:59")
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition AS r ON rr.lawson_requisition_sid = r.requisition_sid
      AND r.valid_to_date = DATETIME("9999-12-31 23:59:59")
    WHERE
      upper(w.appsvalue) LIKE '%APPROVEDRAFTREQUISITION%'
      AND r.requisition_sid IS NOT NULL QUALIFY row_number() OVER (
        PARTITION BY w.pfiworkunit,
        workflow_activity_num
        ORDER BY
          w.repset_variation_id DESC
      ) = 1
  ) AS a;

END;