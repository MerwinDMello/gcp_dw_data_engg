BEGIN
DECLARE DUP_COUNT INT64;
DECLARE current_ts DATETIME;
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
BEGIN TRANSACTION;

DELETE FROM {{ params.param_hr_stage_dataset_name }}.ref_disciplinary_indicator_wrk WHERE 1=1;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.ref_disciplinary_indicator_wrk (disciplinary_ind, disciplinary_desc, source_system_code, dw_last_update_date_time)
    SELECT
        CASE
          WHEN upper(pagrdi.pagrdistep_type) = '' THEN '0'
          ELSE pagrdi.pagrdistep_type
        END AS disciplinary_ind,
        CASE
          WHEN upper(pagrdi.pagrdistep_type) = '1' THEN 'Grievance'
          WHEN upper(pagrdi.pagrdistep_type) = '2' THEN 'Disciplinary'
          ELSE 'Dummy Value'
        END AS disciplinary_desc,
        'L' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.pagrdi
    UNION DISTINCT
    SELECT
        CASE
          WHEN upper(pagrdistep.pagrdistep_type) = '' THEN '0'
          ELSE pagrdistep.pagrdistep_type
        END AS disciplinary_ind,
        CASE
          WHEN upper(pagrdistep.pagrdistep_type) = '1' THEN 'Grievance'
          WHEN upper(pagrdistep.pagrdistep_type) = '2' THEN 'Disciplinary'
          ELSE 'Dummy Value'
        END AS disciplinary_desc,
        'L' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.pagrdistep
  ;
  /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select Disciplinary_Ind
        from {{ params.param_hr_stage_dataset_name }}.ref_disciplinary_indicator_wrk
        group by Disciplinary_Ind
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_stage_dataset_name }}.ref_disciplinary_indicator_wrk');
    ELSE
      COMMIT TRANSACTION;
    END IF;

BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_disciplinary_indicator (disciplinary_ind, disciplinary_desc, source_system_code, dw_last_update_date_time)
    SELECT
        stg.disciplinary_ind,
        stg.disciplinary_desc,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_disciplinary_indicator_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.ref_disciplinary_indicator AS tgt ON stg.disciplinary_ind = tgt.disciplinary_ind
      WHERE tgt.disciplinary_ind IS NULL;

  /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select Disciplinary_Ind
        from {{ params.param_hr_core_dataset_name }}.ref_disciplinary_indicator
        group by Disciplinary_Ind
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_stage_dataset_name }}.ref_disciplinary_indicator');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;
