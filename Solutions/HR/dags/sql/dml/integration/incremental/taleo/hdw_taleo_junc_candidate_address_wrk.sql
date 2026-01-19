TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.junc_candidate_address_wrk; 

BEGIN
DECLARE
  DUP_COUNT INT64;
  BEGIN TRANSACTION;


  /*  Insert the New Records/Chnages into the Target Table  */ /* Begin Transaction Block Starts Here */
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.junc_candidate_address_wrk (candidate_sid,
    valid_from_date,
    addr_sid,
    addr_type_code,
    valid_to_date,
    source_system_code,
    dw_last_update_date_time)
SELECT
  j.candidate_sid,
  j.valid_from_date,
  j.addr_sid,
  'CAN',
  j.valid_to_date,
  j.source_system_code,
  datetime(j.dw_last_update_date_time)
FROM (
  SELECT
    COALESCE(SUBSTR(TRIM(stg.address), 1, 30), '') AS addr_line_1_text,
    COALESCE(SUBSTR(TRIM(stg.address2), 1, 30), '') AS addr_line_2_text,
    '' AS addr_line_3_text,
    '' AS addr_line_4_text,
    COALESCE(SUBSTR(TRIM(stg.city), 1, 25), '') AS city_name,
    '' AS state_code,
    COALESCE(SUBSTR(CONCAT(TRIM(stg.zipcode), '       '), 1, 7), '') AS zip_code,
    '' AS county_name,
    CAST(NULL AS STRING) AS country_code,
    CAST(NULL AS STRING) AS location_code,
    can.candidate_sid,
    stg.file_date AS valid_from_date,
    ad.addr_sid,
    DATETIME("9999-12-31 23:59:59") AS valid_to_date,
    'T' AS source_system_code,
    DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
  FROM
    {{ params.param_hr_stage_dataset_name }}.taleo_candidate AS stg
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.candidate AS can
  ON
    number = cast(can.candidate_num as string)
    AND can.valid_to_date = DATETIME("9999-12-31 23:59:59")
    AND UPPER(can.source_system_code) = 'T'
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.address AS ad
  ON
    UPPER(COALESCE(Trim(ad.addr_line_1_text), '')) = UPPER(COALESCE(Trim(stg.address), ''))
    AND UPPER(COALESCE(Trim(ad.addr_line_2_text), '')) = UPPER(COALESCE(Trim(stg.address2), ''))
    AND UPPER(COALESCE(TRIM(ad.addr_line_3_text), '')) = UPPER(COALESCE('', ''))
    AND UPPER(COALESCE(TRIM(ad.addr_line_4_text), '')) = UPPER(COALESCE('', ''))
    AND UPPER(COALESCE(Trim(ad.city_name), '')) = UPPER(COALESCE(Trim(stg.city), ''))
    AND UPPER(COALESCE(TRIM(ad.state_code), '')) = UPPER(COALESCE('', ''))
    AND UPPER(COALESCE(Trim(ad.zip_code), '')) = UPPER(COALESCE(Trim(stg.zipcode), ''))
    AND UPPER(COALESCE(TRIM(ad.county_name), '')) = UPPER(COALESCE('', ''))
    AND UPPER(COALESCE(Trim(ad.location_code), '')) = UPPER(COALESCE(CAST(NULL AS STRING), ''))
    AND UPPER(TRIM(ad.source_system_code)) = 'T'
  WHERE
    UPPER(addr_type_code) = 'CAN' QUALIFY ROW_NUMBER() OVER (PARTITION BY can.candidate_sid ORDER BY ad.addr_sid DESC) = 1
  UNION DISTINCT
  SELECT
    COALESCE(SUBSTR(TRIM(stg.primarycontactinfo_postaladdress_deliveryaddress_addressline1
), 1, 30), '') AS addr_line_1_text,
    COALESCE(SUBSTR(TRIM(stg.primarycontactinfo_postaladdress_deliveryaddress_addressline2
), 1, 30), '') AS addr_line_2_text,
    SUBSTR(COALESCE(SUBSTR(TRIM(stg.primarycontactinfo_postaladdress_deliveryaddress_addressline3
), 1, 30), ''), 1, 0) AS addr_line_3_text,
    SUBSTR(COALESCE(SUBSTR(TRIM(stg.primarycontactinfo_postaladdress_deliveryaddress_addressline4
), 1, 30), ''), 1, 0) AS addr_line_4_text,
    SUBSTR(COALESCE(SUBSTR(TRIM(stg.primarycontactinfo_postaladdress_municipality
), 1, 30), ''), 1, 25) AS city_name,
    SUBSTR(COALESCE(SUBSTR(TRIM(stg.primarycontactinfo_postaladdress_stateprovince
), 1, 30), ''), 1, 0) AS state_code,
    SUBSTR(COALESCE(SUBSTR(TRIM(stg.primarycontactinfo_postaladdress_postalcode), 1, 30), ''), 1, 7) AS zip_code,
    SUBSTR(COALESCE(SUBSTR(TRIM(stg.primarycontactinfo_postaladdress_county
), 1, 30), ''), 1, 0) AS county_name,
    '   ' AS country_code,
    SUBSTR(CONCAT(COALESCE(SUBSTR(TRIM(stg.primarycontactinfo_contactlocation), 1, 30), ''), REPEAT(' ', 10)), 1, 10) AS location_code,
    can.candidate_sid,
    can.valid_from_date,
    ad.addr_sid,
    DATETIME("9999-12-31 23:59:59") AS valid_to_date,
    'B' AS source_system_code,
    DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
  FROM
    {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_candidate_stg AS stg
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.candidate AS can
  ON
    CAST(stg.candidate as INT64) = CAST(can.candidate_num as INT64)
    AND can.valid_to_date = DATETIME("9999-12-31 23:59:59")
    AND UPPER(can.source_system_code) = 'B'
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.address AS ad
  ON
    UPPER(COALESCE(TRIM(ad.addr_line_1_text), '')) = UPPER(COALESCE(TRIM(stg.primarycontactinfo_postaladdress_deliveryaddress_addressline1), ''))
    AND UPPER(COALESCE(TRIM(ad.addr_line_2_text), '')) = UPPER(COALESCE(TRIM(stg.primarycontactinfo_postaladdress_deliveryaddress_addressline2), ''))
    AND UPPER(COALESCE(TRIM(ad.addr_line_3_text), '')) = UPPER(COALESCE(TRIM(stg.primarycontactinfo_postaladdress_deliveryaddress_addressline3), ''))
    AND UPPER(COALESCE(TRIM(ad.addr_line_4_text), '')) = UPPER(COALESCE(TRIM(stg.primarycontactinfo_postaladdress_deliveryaddress_addressline4), ''))
    AND UPPER(COALESCE(TRIM(ad.city_name), '')) = UPPER(COALESCE(TRIM(stg.primarycontactinfo_postaladdress_municipality), ''))
    AND UPPER(COALESCE(TRIM(ad.state_code), '')) = UPPER(COALESCE(TRIM(stg.primarycontactinfo_postaladdress_stateprovince), ''))
    AND UPPER(COALESCE(TRIM(ad.zip_code), '')) = UPPER(COALESCE(TRIM(stg.primarycontactinfo_postaladdress_postalcode), ''))
    AND UPPER(COALESCE(TRIM(ad.county_name), '')) = UPPER(COALESCE(TRIM(stg.primarycontactinfo_postaladdress_county), ''))
  INNER JOIN
    -- COALESCE(AD.Location_Code,'') = COALESCE(STG.PriContInfoContLoca,'')    AND -- AD.Source_System_code = 'B'
     (
  SELECT
    ats_cust_v2_candidate_stg.candidate,
    MAX(ats_cust_v2_candidate_stg.updatestamp) AS updatestamp
  FROM
    {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_candidate_stg
  GROUP BY
    1 ) AS x
ON
  stg.candidate = x.candidate
  AND stg.updatestamp = x.updatestamp
WHERE
  UPPER(trim(addr_type_code)) = 'CAN' QUALIFY ROW_NUMBER() OVER (PARTITION BY can.candidate_sid ORDER BY ad.addr_sid DESC) = 1 ) AS j ;


  
    /* Test Unique Primary Index constraint set in Terdata */
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
    candidate_sid ,valid_from_date ,addr_sid ,addr_type_code 
    FROM
      {{ params.param_hr_stage_dataset_name }}.junc_candidate_address_wrk
    GROUP BY candidate_sid ,valid_from_date ,addr_sid ,addr_type_code 
	
    HAVING COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.junc_candidate_address_wrk');
  ELSE
COMMIT TRANSACTION;
END IF;
END  ;
