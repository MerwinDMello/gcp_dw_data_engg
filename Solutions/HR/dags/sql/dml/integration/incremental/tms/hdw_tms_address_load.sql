BEGIN
  DECLARE dup_count INT64;

  CALL `{{ params.param_hr_core_dataset_name }}.sk_gen`("{{ params.param_hr_stage_dataset_name }}","work_history_report","'PWR' ||'-'|| TRIM(COALESCE(work_history_address,''))||'-'|| TRIM(COALESCE( work_history_city,''))||'-'|| TRIM(COALESCE( work_history_postal_code,''))||'-'|| TRIM(COALESCE( work_history_country,''))", "Address");

  BEGIN TRANSACTION;
/*	Load only the new records to the Core Table	*/
  INSERT INTO {{ params.param_hr_core_dataset_name }}.address (addr_sid, addr_type_code, addr_line_1_text, addr_line_2_text, addr_line_3_text, addr_line_4_text, city_name, state_code, zip_code, county_name, country_code, location_code, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(stg.sk AS INT64) AS addr_sid,
        max(trim(stg.addr_type_cd)) AS addr_type_code,
        max(stg.addr1) AS addr_line_1_text,
        max(stg.addr2) AS addr_line_2_text,
        max(stg.addr3) AS addr_line_3_text,
        max(stg.addr4) AS addr_line_4_text,
        max(stg.city) AS city_name,
        max(stg.state) AS state_code,
        max(stg.zip) AS zip_code,
        max(stg.county) AS county_name,
        max(stg.country_code) AS country_code,
        max(stg.location_code) AS location_code,
        max(stg.source_system_code) AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              xwlk.sk,
              'PWR' AS addr_type_cd,
              trim(coalesce(pwr.work_history_address, '')) AS addr1,
              CAST(NULL as STRING) AS addr2,
              CAST(NULL as STRING) AS addr3,
              CAST(NULL as STRING) AS addr4,
              trim(coalesce(pwr.work_history_city, '')) AS city,
              '' AS state,
              trim(coalesce(pwr.work_history_postal_code, '')) AS zip,
              '' AS county,
              trim(coalesce(pwr.work_history_country, '')) AS country_code,
              CAST(NULL as STRING) AS location_code,
              'M' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.work_history_report AS pwr
              INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat('PWR', '-', trim(coalesce(pwr.work_history_address, '')), '-', trim(coalesce(pwr.work_history_city, '')), '-', trim(coalesce(pwr.work_history_postal_code, '')), '-', trim(coalesce(pwr.work_history_country, '')))) = upper(xwlk.sk_source_txt)
               AND upper(xwlk.sk_type) = 'ADDRESS'
        ) AS stg
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.address AS tgt ON stg.sk = tgt.addr_sid
      WHERE tgt.addr_sid IS NULL
      GROUP BY 1, upper(trim(stg.addr_type_cd)), upper(stg.addr1), upper(stg.addr2), upper(stg.addr3), upper(stg.addr4), upper(stg.city), upper(stg.state), upper(stg.zip), upper(stg.county), upper(stg.country_code), upper(stg.location_code), upper(stg.source_system_code), 14
  ;


    /* Test Unique Index constraint set in Terdata */
    SET dup_count = ( 
        select count(*)
        from (
        select
            addr_sid, addr_type_code
        from {{ params.param_hr_core_dataset_name }}.address
        group by addr_sid, addr_type_code
        having count(*) > 1
        )
    );
    IF dup_count <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.address');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;
