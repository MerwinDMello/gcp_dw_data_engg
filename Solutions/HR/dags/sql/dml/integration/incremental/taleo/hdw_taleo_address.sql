BEGIN
  DECLARE DUP_COUNT INT64;

  BEGIN TRANSACTION;

  INSERT INTO {{ params.param_hr_core_dataset_name }}.address (addr_sid, addr_type_code, addr_line_1_text, addr_line_2_text, addr_line_3_text, addr_line_4_text, city_name, state_code, zip_code, county_name, country_code, location_code, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(xwlk.sk AS INT64) AS addr_sid,
        stg.addr_type_code,
        stg.addr_line_1_text,
        stg.addr_line_2_text,
        stg.addr_line_3_text,
        stg.addr_line_4_text,
        stg.city_name,
        stg.state_code,
        stg.zip_code,
        stg.county_name,
        stg.country_code,
        stg.location_code,
        stg.source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_address_wrk1 AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON concat(trim(stg.addr_type_code), '-', trim(coalesce(stg.addr_line_1_text, '')), '-', trim(coalesce(stg.addr_line_2_text, '')), '-', trim(coalesce(stg.addr_line_3_text, '')), '-', trim(coalesce(stg.addr_line_4_text, '')), '-', trim(coalesce(stg.city_name, '')), '-', trim(coalesce(stg.state_code, '')), '-', trim(coalesce(stg.zip_code, '')), '-', trim(coalesce(stg.county_name, '')), '-', '', '-', trim(coalesce(stg.location_code, ''))) = xwlk.sk_source_txt
         AND upper(xwlk.sk_type) = 'ADDRESS'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.address AS tgt ON xwlk.sk = tgt.addr_sid
      WHERE tgt.addr_sid IS NULL
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
      QUALIFY row_number() OVER (PARTITION BY addr_sid, stg.addr_type_code ORDER BY stg.source_system_code DESC) = 1
  ;
    /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Addr_SID ,Addr_Type_Code 
        from {{ params.param_hr_core_dataset_name }}.address
        group by Addr_SID ,Addr_Type_Code 
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.address');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;