
/*  Load Work Table with working Data */
BEGIN
DECLARE DUP_COUNT INT64;
BEGIN TRANSACTION;
  
  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_recruitment_source AS tgt USING (
    SELECT
        rec_src.number AS recruitment_source_id,
        trim(rec_src.description) AS recruitment_source_desc,
        rec_src.type_number AS recruitment_source_type_id,
        rec_src.source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_recruitmentsource AS rec_src
      GROUP BY 1, 2, 3, 4, 5
  ) AS stg
  ON tgt.recruitment_source_id = stg.recruitment_source_id
     WHEN MATCHED THEN UPDATE SET recruitment_source_desc = stg.recruitment_source_desc, recruitment_source_type_id = stg.recruitment_source_type_id, source_system_code = stg.source_system_code, dw_last_update_date_time = stg.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN INSERT (recruitment_source_id, recruitment_source_desc, recruitment_source_type_id, source_system_code, dw_last_update_date_time) VALUES (stg.recruitment_source_id, stg.recruitment_source_desc, stg.recruitment_source_type_id, stg.source_system_code, stg.dw_last_update_date_time)
  ;


-- -Modified ATS code to generate the ID's for all desc which is coing from source

 
  MERGE INTO
  {{ params.param_hr_core_dataset_name }}.ref_recruitment_source AS tgt
USING
  (
  SELECT
    CASE
      WHEN recruitment_source_id IS NOT NULL THEN recruitment_source_id
    ELSE
    (
    SELECT

      COALESCE(MAX(recruitment_source_id), CAST(100000000 AS BIGNUMERIC))
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_recruitment_source
    WHERE
      Upper(Trim(source_system_code)) = 'B' ) + CAST(ROW_NUMBER() OVER (ORDER BY stg.source_varchar) AS BIGNUMERIC)
  END
    AS recruitment_source_id,
    stg.source_varchar AS recruitment_source_desc,
    rst.recruitment_source_type_id AS recruitment_source_type_id,
    'B' AS source_system_code,
    DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND)  AS dw_last_update_date_time
  FROM (
    SELECT
      TRIM(_source) AS source_varchar
    FROM
      {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg
    WHERE
      TRIM(trim(_source)) <> ''
    GROUP BY
      1 ) AS stg
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.ref_recruitment_source AS rs
  ON
    stg.source_varchar = rs.recruitment_source_desc
    AND UPPER(rs.source_system_code) = 'B'
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.ref_recruitment_source_type AS rst
  ON
    stg.source_varchar = rst.recruitment_source_type_code
    AND UPPER(rst.source_system_code) = 'B' ) AS src
ON
  tgt.recruitment_source_id = src.recruitment_source_id
  WHEN MATCHED THEN UPDATE SET recruitment_source_desc = src.recruitment_source_desc, recruitment_source_type_id = src.recruitment_source_type_id, source_system_code = src.source_system_code, dw_last_update_date_time = src.dw_last_update_date_time
  WHEN NOT MATCHED BY TARGET
  THEN
INSERT
  (recruitment_source_id,
    recruitment_source_desc,
    recruitment_source_type_id,
    source_system_code,
    dw_last_update_date_time)
VALUES
  (cast(src.recruitment_source_id as int64), src.recruitment_source_desc, src.recruitment_source_type_id, src.source_system_code, src.dw_last_update_date_time) ;

  SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            recruitment_source_id
        from {{ params.param_hr_core_dataset_name }}.ref_recruitment_source
        group by recruitment_source_id
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_recruitment_source');
    ELSE
      COMMIT TRANSACTION;
    END IF;


END;
