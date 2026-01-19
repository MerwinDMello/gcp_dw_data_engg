BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE current_dt DATETIME;
  SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

  BEGIN TRANSACTION;
  /*  Load Core Table with working Data */
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.ref_job_board_type (job_board_type_id,
    job_board_type_desc,
    source_system_code,
    dw_last_update_date_time)
SELECT
  stgg.job_board_type_id,
  stgg.job_board_type_desc,
  stgg.source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM (
  SELECT
    DISTINCT
    CASE TRIM(taleo_job_board_type_stg.job_board_type_number)
      WHEN '' THEN 0
    ELSE
    CAST(TRIM(taleo_job_board_type_stg.job_board_type_number) AS INT64)
  END
    AS job_board_type_id,
    TRIM(taleo_job_board_type_stg.description) AS job_board_type_desc,
    'T' AS source_system_code
  FROM
    {{ params.param_hr_stage_dataset_name }}.taleo_job_board_type_stg
  UNION DISTINCT
  SELECT
    DISTINCT CAST(CASE
        WHEN rjb.job_board_type_id IS NOT NULL THEN rjb.job_board_type_id
      ELSE
      (
      SELECT
        COALESCE(MAX(job_board_type_id), CAST(1000 AS BIGNUMERIC))
      FROM
        {{ params.param_hr_base_views_dataset_name }}.ref_job_board_type
      WHERE
        Upper(Trim(source_system_code)) = 'B' ) + CAST(ROW_NUMBER() OVER (ORDER BY rjb.job_board_type_id, jobboard) AS BIGNUMERIC)
    END
      AS INT64) AS job_board_type_id,
    TRIM(jobboard) AS job_board_type_desc,
    'B' AS source_system_code
  FROM
    {{ params.param_hr_stage_dataset_name }}.ats_hcm_jobboard_stg AS x
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.ref_job_board_type AS rjb
  ON
    COALESCE(TRIM(rjb.job_board_type_desc), 'XXXX') = COALESCE(TRIM(x.jobboard), 'XXXX')
    AND UPPER(rjb.source_system_code) = 'B' ) AS stgg
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_job_board_type AS tgt
ON
  COALESCE(tgt.job_board_type_id,99999)  = COALESCE(stgg.job_board_type_id,99999)
WHERE
  tgt.job_board_type_id IS NULL ;

  /* Test Unique Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
        Job_Board_Type_Id
        from {{ params.param_hr_core_dataset_name }}.ref_job_board_type
        group by 
        Job_Board_Type_Id        
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table:{{ params.param_hr_core_dataset_name }}.ref_job_board_type' );
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;    