BEGIN
  DECLARE dup_count_stg INT64;
  DECLARE dup_count_core INT64;

/* Load Work Table with working Data */
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.ref_competency_wrk;

  BEGIN TRANSACTION;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.ref_competency_wrk (competency_id, competency_desc, source_system_code, dw_last_update_date_time)
    SELECT
        (
          SELECT
              coalesce(max(competency_id), CAST(0 as INT64))
            FROM
              {{ params.param_hr_base_views_dataset_name }}.ref_competency
        ) + CAST(row_number() OVER (ORDER BY y.competency_desc) as INT64) AS competency_id,
        y.competency_desc,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              stg.competency_desc
            FROM
              (
                SELECT DISTINCT
                    TRIM(competency_ratings_report.competency) AS competency_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.competency_ratings_report
                  WHERE TRIM(competency_ratings_report.competency) IS NOT NULL
                   AND TRIM(competency_ratings_report.competency) <> ''
              ) AS stg
              LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.ref_competency_wrk AS tgt 
              ON UPPER(TRIM(COALESCE(stg.competency_desc,''))) = UPPER(TRIM(COALESCE(tgt.competency_desc,'')))
            WHERE tgt.competency_desc IS NULL
        ) AS y
  ;

  DELETE FROM
  {{ params.param_hr_stage_dataset_name }}.ref_competency_wrk
  WHERE (competency_id, UPPER(TRIM(competency_desc))) IN
  (SELECT AS STRUCT
  competency_id, UPPER(TRIM(competency_desc)) as competency_desc
  FROM
  (
    Select
    row_number() OVER (PARTITION BY UPPER(TRIM(competency_desc)) ORDER BY competency_desc) as row_num,
    competency_id,
    competency_desc,
    source_system_code,
    dw_last_update_date_time
    FROM
    {{ params.param_hr_stage_dataset_name }}.ref_competency_wrk
  ) ref_competency_wrk
  WHERE row_num = 2)
  ;

/*  Collect Statistics on the WRK Table    */
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_competency (competency_id, competency_desc, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(row_number() OVER (ORDER BY wrk.competency_id) as INT64) + (
          SELECT
              coalesce(max(competency_id), CAST(0 as INT64))
            FROM
              {{ params.param_hr_core_dataset_name }}.ref_competency
        ) AS competency_id,
        TRIM(wrk.competency_desc),
        wrk.source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_competency_wrk AS wrk
      WHERE UPPER(COALESCE(REGEXP_REPLACE(wrk.competency_desc, r'([^\x20-\x7E]+)', ''),'')) NOT IN(
        SELECT
            UPPER(COALESCE(REGEXP_REPLACE(competency_desc, r'([^\x20-\x7E]+)', ''),''))
          FROM
            {{ params.param_hr_core_dataset_name }}.ref_competency
      )
  ;

    /* Test Unique Index constraint set in Terdata */
    SET dup_count_core = ( 
        select count(*)
        from (
        select
            competency_id
        from {{ params.param_hr_core_dataset_name }}.ref_competency
        group by competency_id
        having count(*) > 1
        )
    );
    IF dup_count_core <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_competency');
    END IF;
    COMMIT TRANSACTION;
END;
