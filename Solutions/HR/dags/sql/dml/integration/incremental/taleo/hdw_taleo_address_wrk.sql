
TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.taleo_address_wrk1;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.taleo_address_wrk1 (addr_type_code, addr_line_1_text, addr_line_2_text, addr_line_3_text, addr_line_4_text, city_name, state_code, zip_code, county_name, country_code, location_code, source_system_code, dw_last_update_date_time)
   /* SELECT
        'CAN' AS addr_type_code,
        coalesce(substr(trim(a.parsedfields_addressline1), 1, 100), '') AS addr_line_1_text,
        coalesce(substr(trim(a.parsedfields_addressline2), 1, 100), '') AS addr_line_2_text,
        '' AS addr_line_3_text,
        '' AS addr_line_4_text,
        coalesce(substr(trim(a.parsedfields_municipality), 1, 100), '') AS city_name,
        coalesce(substr(concat(trim(a.parsedfields_stateprovince), '  '), 1, 2), '') AS state_code,
        coalesce(substr(concat(trim(a.parsedfields_postalcode), repeat(' ', 20)), 1, 20), '') AS zip_code,
        coalesce(substr(trim(a.parsedfields_county), 1, 40), '') AS county_name,
        CAST(max(NULL) as STRING) AS country_code,
        CAST(max(NULL) as STRING) AS location_code,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_candidate_stg AS a
      GROUP BY 1, 2, 3, 6, 7, 8, 9, upper(NULL), upper(NULL) */

    SELECT DISTINCT
        'CAN' AS addr_type_code,
        coalesce(substr(trim(a.primarycontactinfo_postaladdress_deliveryaddress_addressline1), 1, 100), '') AS addr_line_1_text,
        coalesce(substr(trim(a.primarycontactinfo_postaladdress_deliveryaddress_addressline2), 1, 100), '') AS addr_line_2_text,
        coalesce(substr(trim(a.primarycontactinfo_postaladdress_deliveryaddress_addressline3), 1, 100), '') AS addr_line_3_text,
        coalesce(substr(trim(a.primarycontactinfo_postaladdress_deliveryaddress_addressline4), 1, 100), '') AS addr_line_4_text,
        coalesce(substr(trim(a.primarycontactinfo_postaladdress_municipality), 1, 100), '') AS city_name,
        coalesce(substr(concat(trim(a.primarycontactinfo_postaladdress_stateprovince), '  '), 1, 2), '') AS state_code,
        coalesce(substr(concat(trim(a.primarycontactinfo_postaladdress_postalcode), repeat(' ', 20)), 1, 20), '') AS zip_code,
        coalesce(substr(trim(a.primarycontactinfo_postaladdress_county), 1, 40), '') AS county_name,
        coalesce(substr(trim(a.primarycontactinfo_postaladdress_country), 1, 40), '') AS country_code,
        CAST(NULL as STRING) AS location_code,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_candidate_stg AS a
    UNION ALL
    SELECT
        'ATS' AS addr_type_code,
        coalesce(substr(trim(a.address_deliveryaddress_addressline1), 1, 100), '') AS addr_line_1_text,
        coalesce(substr(trim(a.address_deliveryaddress_addressline2), 1, 100), '') AS addr_line_2_text,
        substr(max(coalesce(substr(trim(a.address_deliveryaddress_addressline3), 1, 100), '')), 1, 100) AS addr_line_3_text,
        substr(max(coalesce(substr(trim(a.address_deliveryaddress_addressline4), 1, 100), '')), 1, 100) AS addr_line_4_text,
        coalesce(substr(trim(a.address_municipality), 1, 100), '') AS city_name,
        coalesce(substr(concat(trim(a.address_stateprovince), '  '), 1, 2), '') AS state_code,
        coalesce(substr(concat(trim(a.address_postalcode), repeat(' ', 20)), 1, 20), '') AS zip_code,
        coalesce(substr(trim(a.address_county), 1, 40), '') AS county_name,
        CAST(max(NULL) as STRING) AS country_code,
        substr(concat(max(coalesce(substr(concat(trim(a.hrlocation), repeat(' ', 10)), 1, 10), '')), repeat(' ', 10)), 1, 10) AS location_code,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_hrlocation_stg AS a
      GROUP BY 1, 2, 3, upper(substr(coalesce(substr(trim(a.address_deliveryaddress_addressline3), 1, 100), ''), 1, 100)), upper(substr(coalesce(substr(trim(a.address_deliveryaddress_addressline4), 1, 100), ''), 1, 100)), 6, 7, 8, 9, upper(NULL), upper(substr(concat(coalesce(substr(concat(trim(a.hrlocation), repeat(' ', 10)), 1, 10), ''), repeat(' ', 10)), 1, 10))
    UNION ALL
    SELECT
        'CAN' AS addr_type_code,
        coalesce(substr(trim(a.address), 1, 100), '') AS addr_line_1_text,
        coalesce(substr(trim(a.address2), 1, 100), '') AS addr_line_2_text,
        '' AS addr_line_3_text,
        '' AS addr_line_4_text,
        coalesce(substr(trim(a.city), 1, 100), '') AS city_name,
        -- ,COALESCE(CAST( TRIM(B.STATE_CODE) AS CHAR(2)),'') AS STATE_CODE
        '' AS state_code,
        coalesce(substr(concat(trim(a.zipcode), repeat(' ', 20)), 1, 20), '') AS zip_code,
        -- ,COALESCE(CAST(TRIM(B.COUNTY_NAME) AS VARCHAR(40)),'') AS COUNTY_NAME --
        '' AS county_name,
        CAST(max(NULL) as STRING) AS country_code,
        -- ,'' AS LOCATION_CODE
        CAST(max(NULL) as STRING) AS location_code,
        'T' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_candidate AS a
      GROUP BY 1, 2, 3, 6, 4, 8, 4, upper(NULL), upper(NULL)
    UNION ALL
    SELECT
        /*LEFT JOIN EDW_PUB_VIEWS.GEOGRAPHIC_LOCATION B --
        ON A.ZIPCODE = B.ZIP_CODE */
        'TAL' AS addr_type_code,
        coalesce(substr(trim(a.worklocation_address1), 1, 100), '') AS addr_line_1_text,
        coalesce(substr(trim(a.worklocation_address2), 1, 100), '') AS addr_line_2_text,
        '' AS addr_line_3_text,
        '' AS addr_line_4_text,
        coalesce(substr(trim(a.worklocation_city), 1, 100), '') AS city_name,
        -- ,COALESCE(CAST( TRIM(B.STATE_CODE) AS CHAR(2)),'') AS STATE_CODE
        '' AS state_code,
        coalesce(substr(concat(trim(a.zipcode), repeat(' ', 20)), 1, 20), '') AS zip_code,
        -- ,COALESCE(CAST(TRIM(B.COUNTY_NAME) AS VARCHAR(40)),'') AS COUNTY_NAME
        '' AS county_name,
        CAST(max(NULL) as STRING) AS country_code,
        substr(concat(max(coalesce(substr(concat(trim(a.worklocation_code), repeat(' ', 10)), 1, 10), '')), repeat(' ', 10)), 1, 10) AS location_code,
        'T' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_location AS a
      GROUP BY 1, 2, 3, 6, 4, 8, 4, upper(NULL), upper(substr(concat(coalesce(substr(concat(trim(a.worklocation_code), repeat(' ', 10)), 1, 10), ''), repeat(' ', 10)), 1, 10))
    UNION ALL
    SELECT
        /*LEFT JOIN EDW_PUB_VIEWS.GEOGRAPHIC_LOCATION B
        ON A.ZIPCODE = B.ZIP_CODE */
        'NCA' AS addr_type_code,
        '0' AS addr_line_1_text,
        '' AS addr_line_2_text,
        '' AS addr_line_3_text,
        '' AS addr_line_4_text,
        coalesce(substr(trim(a.campus_city), 1, 100), '') AS city_name,
        coalesce(substr(trim(a.campus_state), 1, 2), '') AS addr_line_1_text,
        coalesce(substr(concat(trim(a.campus_zip), repeat(' ', 20)), 1, 20), '') AS zip_code,
        '' AS county_name,
        CAST(max(NULL) as STRING) AS country_code,
        '          ' AS location_code,
        'C' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.galen_stg AS a
      GROUP BY 1, 2, 3, 6, 7, 8, 3, upper(NULL)
    UNION ALL
    SELECT
        'SHA' AS addr_type_code,
        coalesce(a.home_address_street, '') AS addr_line_1_text,
        '' AS addr_line_2_text,
        '' AS addr_line_3_text,
        '' AS addr_line_4_text,
        coalesce(substr(trim(a.home_address_city), 1, 100), '') AS city_name,
        coalesce(substr(trim(a.home_address_state), 1, 2), '') AS addr_line_1_text,
        coalesce(substr(concat(trim(a.home_address_zip), repeat(' ', 20)), 1, 20), '') AS zip_code,
        '' AS county_name,
        CAST(max(NULL) as STRING) AS country_code,
        '          ' AS location_code,
        'C' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.galen_stg AS a
      GROUP BY 1, 2, 3, 6, 7, 8, 3, upper(NULL)
  ;
  
  CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'taleo_address_wrk1', " TRIM(addr_type_code) ||'-'|| TRIM(COALESCE(addr_line_1_text,''))||'-'|| TRIM(COALESCE(addr_line_2_text,''))||'-'||TRIM(COALESCE( addr_line_3_text,''))||'-'||TRIM(COALESCE(addr_line_4_text,''))||'-'|| TRIM(COALESCE(city_name,''))||'-'|| TRIM(COALESCE( state_code,''))||'-'|| TRIM(COALESCE( zip_code,''))||'-'|| TRIM(COALESCE( county_name,''))|| '-' || '-' || TRIM(COALESCE( location_code,''))", 'ADDRESS');