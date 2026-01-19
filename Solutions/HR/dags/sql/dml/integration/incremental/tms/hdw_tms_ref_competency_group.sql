BEGIN
  DECLARE dup_count_stg INT64;
  DECLARE dup_count_core INT64;


/* Load Work Table with working Data */
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.ref_competency_group_wrk;

  BEGIN TRANSACTION;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.ref_competency_group_wrk (competency_group_id, competency_group_desc, source_system_code, dw_last_update_date_time)
    SELECT
        (
          SELECT
              COALESCE(max(competency_group_id), CAST(0 as INT64))
            FROM
              {{ params.param_hr_base_views_dataset_name }}.ref_competency_group
        ) + CAST(row_number() OVER (ORDER BY y.competency_group_desc) as INT64) AS competency_group_id,
        y.competency_group_desc,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              stg.competency_group_desc
            FROM
              (
                SELECT DISTINCT
                    TRIM(competency_ratings_report.competency_group) AS competency_group_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.competency_ratings_report
                  WHERE TRIM(competency_ratings_report.competency_group) IS NOT NULL
                   AND TRIM(competency_ratings_report.competency_group) <> ''
              ) AS stg
              LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.ref_competency_group_wrk AS tgt 
              ON UPPER(TRIM(COALESCE(stg.competency_group_desc,''))) = UPPER(TRIM(COALESCE(tgt.competency_group_desc,'')))
            WHERE tgt.competency_group_desc IS NULL
        ) AS y
  ;

/*  Collect Statistics on the WRK Table    */
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_competency_group (competency_group_id, competency_group_desc, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(row_number() OVER (ORDER BY wrk.competency_group_id) as INT64) + (
          SELECT
              COALESCE(max(competency_group_id), CAST(0 as INT64))
            FROM
              {{ params.param_hr_core_dataset_name }}.ref_competency_group
        ) AS competency_group_id,
        TRIM(wrk.competency_group_desc),
        wrk.source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_competency_group_wrk AS wrk
      WHERE UPPER(wrk.competency_group_desc) NOT IN(
        SELECT
            UPPER(competency_group_desc)
          FROM
            {{ params.param_hr_core_dataset_name }}.ref_competency_group
      )
  ;

    /* Test Unique Index constraint set in Terdata */
    SET dup_count_core = ( 
        select count(*)
        from (
        select
            competency_group_id
        from {{ params.param_hr_core_dataset_name }}.ref_competency_group
        group by competency_group_id
        having count(*) > 1
        )
    );
    IF dup_count_core <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_competency_group');
    END IF;
    COMMIT TRANSACTION;
END;
