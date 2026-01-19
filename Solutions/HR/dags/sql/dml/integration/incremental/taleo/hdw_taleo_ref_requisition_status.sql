BEGIN
  DECLARE DUP_COUNT INT64;

  BEGIN TRANSACTION;

  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_requisition_status AS tgt USING (
    SELECT
        requisitionstate_number AS requisition_status_id,
        trim(description) AS description,
        parentstate_number,
        source_system_code,
        dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_requisitionstate
    UNION ALL
    SELECT
        CASE
          WHEN requisition_status_id IS NOT NULL THEN requisition_status_id
          ELSE (
            SELECT
                coalesce(max(requisition_status_id), CAST(1000 as BIGNUMERIC))
              FROM
                {{ params.param_hr_base_views_dataset_name }}.ref_requisition_status
              WHERE upper(stg.source_system_code) = 'B'
          ) + CAST(row_number() OVER (ORDER BY requisition_status_id, stg.description) as BIGNUMERIC)
        END AS requisition_status_id,
        stg.description,
        stg.parentstate_number AS parentstate_number,
        stg.source_system_code AS source_system_code,
        stg.dw_last_update_date_time AS dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              trim(status_state) AS description,
              NULL AS parentstate_number,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg
        ) AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_requisition_status AS tgt_0 ON trim(tgt_0.status_desc) = stg.description
         AND upper(tgt_0.source_system_code) = 'B'
  ) AS src
  ON tgt.requisition_status_id = src.requisition_status_id
   AND tgt.source_system_code = src.source_system_code
     WHEN MATCHED THEN UPDATE SET status_desc = src.description, parent_requisition_status_id = src.parentstate_number, dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND)
     WHEN NOT MATCHED BY TARGET THEN INSERT (requisition_status_id, status_desc, parent_requisition_status_id, source_system_code, dw_last_update_date_time) VALUES (CAST(src.requisition_status_id AS INT64), src.description, src.parentstate_number, src.source_system_code, DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND))
  ;

    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Requisition_Status_Id 
        from {{ params.param_hr_core_dataset_name }}.ref_requisition_status
        group by Requisition_Status_Id
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_requisition_status');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;