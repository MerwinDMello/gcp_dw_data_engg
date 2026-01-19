BEGIN
  
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.ref_recruitment_location_wrk;

  
  
  
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.ref_recruitment_location_wrk (file_date, location_num, level_num, location_name, location_code_text, work_location_code_text, addr_sid, source_system_code, dw_last_update_date_time)
    SELECT
        stg.file_date AS file_date,
        CASE
           stg.number
          WHEN '' THEN NUMERIC '0'
          ELSE CAST(stg.number as NUMERIC)
        END AS location_num,
        CASE
           trim(stg.level)
          WHEN '' THEN 0
          ELSE CAST(trim(stg.level) as INT64)
        END AS level_num,
        trim(stg.name) AS location_name,
        trim(stg.code) AS location_code_text,
        trim(stg.worklocation_code) AS work_location_code_text,
        adr.addr_sid AS addr_sid,
        'T' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_location AS stg
        LEFT OUTER JOIN 
          (
            SELECT
               addr_sid
              ,addr_type_code
              ,addr_line_1_text
              ,addr_line_2_text
              ,addr_line_3_text
              ,addr_line_4_text
              ,city_name
              ,zip_code
              ,county_name
              ,country_code
              ,state_code
              ,location_code
              ,source_system_code
              ,dw_last_update_date_time
              ,row_number() over(partition by coalesce(trim(addr_type_code), ''), coalesce(trim(addr_line_1_text), ''), coalesce(trim(addr_line_2_text), ''), coalesce(trim(city_name), ''), coalesce(trim(zip_code), ''), coalesce(trim(location_code), ''), coalesce(trim(source_system_code), '')
              order by dw_last_update_date_time desc) RN

            FROM {{ params.param_hr_base_views_dataset_name }}.address
          ) AS adr
         ON coalesce(trim(stg.worklocation_address1), '') = coalesce(trim(adr.addr_line_1_text), '')
         AND coalesce(trim(stg.worklocation_address2), '') = coalesce(trim(adr.addr_line_2_text), '')
         AND coalesce(trim(stg.worklocation_city), '') = coalesce(trim(adr.city_name), '')
         AND coalesce(trim(stg.zipcode), '') = coalesce(trim(adr.zip_code), '')
         AND coalesce(trim(stg.worklocation_code), '') = coalesce(trim(adr.location_code), '')
         AND trim(upper(adr.addr_type_code)) = 'TAL'
         AND adr.RN = 1
    UNION ALL
    SELECT
        current_date() AS file_date,
        CASE
          WHEN tgt.location_num IS NOT NULL THEN tgt.location_num
          ELSE (
            SELECT
                coalesce(max(location_num), CAST(0 as NUMERIC))
              FROM
                {{ params.param_hr_base_views_dataset_name }}.ref_recruitment_location
              WHERE upper(stgg.source_system_code) = 'B'
          ) + CAST(row_number() OVER (ORDER BY tgt.location_num, stgg.location_code_text) as NUMERIC)
        END AS location_num,
        CAST(stgg.level_num as INT64) AS level_num,
        stgg.location_name,
        stgg.location_code_text,
        stgg.work_location_code_text,
        stgg.addr_sid,
        stgg.source_system_code,
        stgg.dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              NULL AS level_num,
              trim(stg.description) AS location_name,
              trim(stg.hrlocation) AS location_code_text,
              trim(stg.hrlocation) AS work_location_code_text,
              adr.addr_sid AS addr_sid,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_cust_hrlocation_stg AS stg
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.address AS adr ON coalesce(trim(stg.address_deliveryaddress_addressline1), '') = coalesce(trim(adr.addr_line_1_text), '')
               AND coalesce(trim(stg.address_deliveryaddress_addressline2), '') = coalesce(trim(adr.addr_line_2_text), '')
               AND coalesce(trim(stg.address_municipality), '') = coalesce(trim(adr.city_name), '')
               AND coalesce(trim(stg.address_postalcode), '') = coalesce(trim(adr.zip_code), '')
               AND coalesce(trim(stg.hrlocation), '') = coalesce(trim(adr.location_code), '')
               AND trim(adr.addr_type_code) = 'ATS'
               QUALIFY row_number() OVER (PARTITION BY adr.addr_type_code, trim(adr.addr_line_1_text),trim(adr.city_name) ,trim(adr.zip_code),trim(adr.location_code),adr.source_system_code ORDER BY adr.dw_last_update_date_time desc)=1			   
        ) AS stgg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_recruitment_location AS tgt ON trim(tgt.location_code_text) = trim(stgg.location_code_text)
         AND upper(tgt.source_system_code) = 'B'
  ;

  
  
END;
