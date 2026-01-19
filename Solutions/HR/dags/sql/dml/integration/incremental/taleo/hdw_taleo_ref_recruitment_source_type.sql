
/*  Load Work Table with working Data */
BEGIN
  DECLARE DUP_COUNT INT64;
  BEGIN TRANSACTION;
  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_recruitment_source_type AS tgt USING (
    SELECT
        rst.number AS recruitment_source_type_id,
        trim(rst.code) AS recruitment_source_type_code,
        trim(rst.description) AS recruitment_source_type_desc,
        rst.source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_recruitmentsourcetype AS rst
      GROUP BY 1, 2, 3, 4, 5
  ) AS stg
  ON tgt.recruitment_source_type_id = stg.recruitment_source_type_id
     WHEN MATCHED THEN UPDATE SET recruitment_source_type_code = stg.recruitment_source_type_code, recruitment_source_type_desc = stg.recruitment_source_type_desc, source_system_code = stg.source_system_code, dw_last_update_date_time = stg.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN INSERT (recruitment_source_type_id, recruitment_source_type_code, recruitment_source_type_desc, source_system_code, dw_last_update_date_time) VALUES (stg.recruitment_source_type_id, stg.recruitment_source_type_code, stg.recruitment_source_type_desc, stg.source_system_code, stg.dw_last_update_date_time)
  ;


  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_recruitment_source_type AS tgt USING (
    SELECT DISTINCT
        CASE
          WHEN recruitment_source_type_id IS NOT NULL THEN recruitment_source_type_id
          ELSE (
            SELECT
                coalesce(max(recruitment_source_type_id), CAST(70 as BIGNUMERIC))
              FROM
                {{ params.param_hr_base_views_dataset_name }}.ref_recruitment_source_type
              WHERE Upper(Trim(source_system_code)) = 'B'
          ) + CAST(row_number() OVER (ORDER BY stg.source_varchar) as BIGNUMERIC)
        END AS recruitment_source_type_id,
        stg.source_varchar AS recruitment_source_type_code,
        stg.source_varchar AS recruitment_source_type_desc,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              trim(_source) AS source_varchar
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg
            WHERE trim(trim(_source)) <> ''
            GROUP BY 1
        ) AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_recruitment_source_type AS tgt_0 ON stg.source_varchar = tgt_0.recruitment_source_type_code
         AND upper(tgt_0.source_system_code) = 'B'
  ) AS stg1
  ON tgt.recruitment_source_type_id = stg1.recruitment_source_type_id
     WHEN MATCHED THEN UPDATE SET recruitment_source_type_desc = stg1.recruitment_source_type_code, source_system_code = stg1.source_system_code, dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND)
     WHEN NOT MATCHED BY TARGET THEN INSERT (recruitment_source_type_id, recruitment_source_type_code, recruitment_source_type_desc, source_system_code, dw_last_update_date_time) VALUES (cast(stg1.recruitment_source_type_id as INT64), stg1.recruitment_source_type_code, stg1.recruitment_source_type_desc, stg1.source_system_code, stg1.dw_last_update_date_time)
  ;
  SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            recruitment_source_type_id
        from {{ params.param_hr_core_dataset_name }}.ref_recruitment_source_type
        group by recruitment_source_type_id
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_recruitment_source_type');
    ELSE
      COMMIT TRANSACTION;
    END IF;

END;