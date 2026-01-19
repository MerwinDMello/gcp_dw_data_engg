
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.ref_nursing_school_campus_wrk0;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.ref_nursing_school_campus_wrk0 (campus_name, campus_code, addr_sid, dw_last_update_date_time)
    SELECT
        trim(stg.campus_desc) AS campus_name,
        trim(cast(stg.campus_code as string)) AS campus_code,
        a.addr_sid AS addr_sid,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.galen_stg AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.address AS a ON coalesce(trim(stg.campus_city), '') = coalesce(trim(a.city_name), '')
         AND coalesce(trim(stg.campus_state), '') = coalesce(trim(a.state_code), '')
         AND coalesce(trim(stg.campus_zip), '') = coalesce(trim(a.zip_code), '')
         AND upper(a.addr_type_code) = 'NCA'
         AND upper(a.source_system_code) = 'C'
      WHERE stg.campus_desc IS NOT NULL
       OR upper(stg.campus_desc) <> ''
      GROUP BY 1, 2, 3, 4
  ;

/*  GENERATE THE SURROGATE KEYS FOR NURSING STUDENT */
CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'ref_nursing_school_campus_wrk0', 'TRIM(CAMPUS_NAME)|| COALESCE(CAST(ADDR_SID AS STRING),\'\')||\'-C\'', 'Ref_Nursing_School_Campus');

/*  truncate work Table */

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.ref_nursing_school_campus_wrk;

/*  Load Work Table */

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.ref_nursing_school_campus_wrk (campus_id, campus_name, campus_code, nursing_school_id, addr_sid, source_system_code, dw_last_update_date_time)
    SELECT DISTINCT
        cast(xwlk.sk as int64) AS campus_id,
        stg.campus_name,
        stg.campus_code,
        1 AS nursing_school_id,
        stg.addr_sid,
        'C' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_nursing_school_campus_wrk0 AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON concat(trim(stg.campus_name), coalesce(cast(stg.addr_sid as string), ''), '-C') = trim(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'REF_NURSING_SCHOOL_CAMPUS'
      GROUP BY 1, 2, 3, 4, 5, 6, 7
  ;
