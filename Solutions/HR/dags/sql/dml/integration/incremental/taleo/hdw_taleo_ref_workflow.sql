
/*  Load Target Table with Staging Data */
BEGIN
DECLARE DUP_COUNT INT64;
BEGIN TRANSACTION;
  
  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_workflow AS tgt USING (
    SELECT
        csw.number AS workflow_id,
        csw.available AS active_sw,
        trim(csw.mnemonic) AS workflow_code,
        trim(csw.name) AS workflow_name,
        trim(csw.description) AS workflow_desc,
        csw.source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_cswworkflow AS csw
      GROUP BY 1, 2, 3, 4, 5, 6, 7
  ) AS stg
  ON tgt.workflow_id = stg.workflow_id
     WHEN MATCHED THEN UPDATE SET active_sw = stg.active_sw, workflow_code = stg.workflow_code, workflow_name = stg.workflow_name, workflow_desc = stg.workflow_desc, source_system_code = stg.source_system_code, dw_last_update_date_time = stg.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN INSERT (workflow_id, active_sw, workflow_code, workflow_name, workflow_desc, source_system_code, dw_last_update_date_time) VALUES (stg.workflow_id, stg.active_sw, stg.workflow_code, stg.workflow_name, stg.workflow_desc, stg.source_system_code, stg.dw_last_update_date_time)
  ;

-- Modified ATS code to generate the ID's for all desc which is coing from source (HDM-1570)

  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_workflow AS tgt USING (
    SELECT
        CASE
          WHEN workflow_id IS NOT NULL THEN workflow_id
          ELSE (
            SELECT
                coalesce(max(workflow_id), CAST(100000000 as BIGNUMERIC))
              FROM
               {{ params.param_hr_base_views_dataset_name }}.ref_workflow
              WHERE Upper(Trim(source_system_code)) = 'B'
          ) + CAST(row_number() OVER (ORDER BY stg.workflow_code) as BIGNUMERIC)
        END AS workflow_id,
        stg.active_sw,
        stg.workflow_code,
        stg.workflow_name,
        stg.workflow_desc,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              active AS active_sw,
              trim('1/' ||  atsworkflow  ) AS workflow_code,
              trim(atsworkflow) AS workflow_name,
              trim(description) AS workflow_desc
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_cust_atsworkflow_stg
            WHERE trim('1/' ||  atsworkflow  ) <> ''
            GROUP BY 1, 2, 3, 4
        ) AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_workflow AS tgt_0 ON tgt_0.workflow_code = stg.workflow_code
         AND upper(tgt_0.source_system_code) = 'B'
  ) AS src
  ON tgt.workflow_id = src.workflow_id
     WHEN MATCHED THEN UPDATE SET active_sw = CAST(src.active_sw as INT64), workflow_code = src.workflow_code, workflow_name = src.workflow_name, workflow_desc = src.workflow_desc, source_system_code = src.source_system_code, dw_last_update_date_time = src.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN INSERT (workflow_id, active_sw, workflow_code, workflow_name, workflow_desc, source_system_code, dw_last_update_date_time) VALUES (CAST(src.workflow_id as INT64), CAST(src.active_sw as INT64), src.workflow_code, src.workflow_name, src.workflow_desc, src.source_system_code, src.dw_last_update_date_time)
  ;
  SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            workflow_id
        from {{ params.param_hr_core_dataset_name }}.ref_workflow
        group by workflow_id
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_workflow');
    ELSE
      COMMIT TRANSACTION;
    END IF;

END;
