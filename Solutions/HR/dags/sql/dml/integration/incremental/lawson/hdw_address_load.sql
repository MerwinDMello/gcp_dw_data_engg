BEGIN
DECLARE DUP_COUNT INT64; 

DECLARE current_ts datetime;
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}','employee',"'EMP'||'-'||TRIM(COALESCE(Addr1,''))||'-'||TRIM(COALESCE(Addr2,''))||'-'||TRIM(COALESCE(Addr3,''))||'-'||TRIM(COALESCE(Addr4,''))||'-'||TRIM(COALESCE(city,''))||'-'||TRIM(COALESCE(state,''))||'-'||TRIM(COALESCE(zip,''))||'-'||TRIM(COALESCE(county,''))||'-'||TRIM(COALESCE(Country_Code,''))", 'ADDRESS');
CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}','pcodesdtl',"'LOC'||'-'||TRIM(COALESCE(Addr1,''))||'-'||TRIM(COALESCE(Addr2,''))||'-'||TRIM(COALESCE(Addr3,''))||'-'||TRIM(COALESCE(Addr4,''))||'-'||TRIM(COALESCE(city,''))||'-'||TRIM(COALESCE(state,''))||'-'||TRIM(COALESCE(zip,''))||'-'||TRIM(COALESCE(county,''))||'-'||TRIM(COALESCE(Country_Code,''))||'-'||TRIM(COALESCE(Code,''))", 'ADDRESS');
CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}','prsystem',"'PRS'||'-'||TRIM(COALESCE(Addr1,''))||'-'||TRIM(COALESCE(Addr2,''))||'-'||TRIM(COALESCE(Addr3,''))||'-'||TRIM(COALESCE(Addr4,''))||'-'||TRIM(COALESCE(city,''))||'-'||TRIM(COALESCE(state,''))||'-'||TRIM(COALESCE(zip,''))||'-'||TRIM(COALESCE(Country_Code,''))", 'ADDRESS');


BEGIN TRANSACTION ;
/*	Generate the surrogate keys for Address	*/

/*	Load only the new records to the Core Table	*/
INSERT INTO {{ params.param_hr_core_dataset_name }}.address (addr_sid, addr_type_code, addr_line_1_text, addr_line_2_text, addr_line_3_text, addr_line_4_text, city_name, state_code, zip_code, county_name, country_code, location_code, source_system_code, dw_last_update_date_time)
    SELECT
        cast(stg.sk as INT64) AS addr_sid,
        max(stg.addr_type_cd) AS addr_type_code,
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
        current_ts AS dw_last_update_date_time
      FROM
        (
          SELECT
              xwlk.sk,
              'EMP' AS addr_type_cd,
              substr(trim(coalesce(emp.addr1, '')), 1, 100) AS addr1,
              substr(trim(coalesce(emp.addr2, '')), 1, 100) AS addr2,
              substr(trim(coalesce(emp.addr3, '')), 1, 100) AS addr3,
              substr(trim(coalesce(emp.addr4, '')), 1, 100) AS addr4,
              substr(trim(coalesce(emp.city, '')), 1, 100) AS city,
              trim(coalesce(emp.state, '')) AS state,
              trim(coalesce(emp.zip, '')) AS zip,
              trim(coalesce(emp.county, '')) AS county,
              trim(coalesce(emp.country_code, '')) AS country_code,
              CAST(NULL as STRING) AS location_code,
              'L' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.employee AS emp
              INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat('EMP', '-', trim(coalesce(emp.addr1, '')), '-', trim(coalesce(emp.addr2, '')), '-', trim(coalesce(emp.addr3, '')), '-', trim(coalesce(emp.addr4, '')), '-', trim(coalesce(emp.city, '')), '-', trim(coalesce(emp.state, '')), '-', trim(coalesce(emp.zip, '')), '-', trim(coalesce(emp.county, '')), '-', trim(coalesce(emp.country_code, '')))) = upper(xwlk.sk_source_txt)
               AND upper(xwlk.sk_type) = 'ADDRESS'
          UNION DISTINCT
          SELECT
              xwlk_0.sk,
              'LOC' AS addr_type_cd,
              substr(trim(coalesce(pcd.addr1, '')), 1, 100) AS addr1,
              substr(trim(coalesce(pcd.addr2, '')), 1, 100) AS addr2,
              substr(trim(coalesce(pcd.addr3, '')), 1, 100) AS addr3,
              substr(trim(coalesce(pcd.addr4, '')), 1, 100) AS addr4,
              substr(trim(coalesce(pcd.city, '')), 1, 100) AS city,
              trim(coalesce(pcd.state, '')) AS state,
              trim(coalesce(pcd.zip, '')) AS zip,
              trim(coalesce(pcd.county, '')) AS county,
              trim(coalesce(pcd.country_code, '')) AS country_code,
              substr(concat(trim(coalesce(pcd.code, '')), repeat(' ', 20)), 1, 20) AS location_code,
              'L' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.pcodesdtl AS pcd
              INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk_0 ON upper(concat('LOC', '-', trim(coalesce(pcd.addr1, '')), '-', trim(coalesce(pcd.addr2, '')), '-', trim(coalesce(pcd.addr3, '')), '-', trim(coalesce(pcd.addr4, '')), '-', trim(coalesce(pcd.city, '')), '-', trim(coalesce(pcd.state, '')), '-', trim(coalesce(pcd.zip, '')), '-', trim(coalesce(pcd.county, '')), '-', trim(coalesce(pcd.country_code, '')), '-', trim(coalesce(pcd.code, '')))) = upper(xwlk_0.sk_source_txt)
               AND upper(xwlk_0.sk_type) = 'ADDRESS'
            WHERE upper(pcd.type) = 'LO'
          UNION DISTINCT
          SELECT
              xwlk_1.sk,
              'PRS' AS addr_type_cd,
              substr(trim(coalesce(prs.addr1, '')), 1, 100) AS addr1,
              substr(trim(coalesce(prs.addr2, '')), 1, 100) AS addr2,
              substr(trim(coalesce(prs.addr3, '')), 1, 100) AS addr3,
              substr(trim(coalesce(prs.addr4, '')), 1, 100) AS addr4,
              substr(trim(coalesce(prs.city, '')), 1, 100) AS city,
              trim(coalesce(prs.state, '')) AS state,
              trim(coalesce(prs.zip, '')) AS zip,
              '' AS county,
              trim(coalesce(prs.country_code, '')) AS country_code,
              substr(concat(NULL, repeat(' ', 20)), 1, 20) AS location_code,
              'L' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.prsystem AS prs
              INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk_1 ON upper(concat('PRS', '-', trim(coalesce(prs.addr1, '')), '-', trim(coalesce(prs.addr2, '')), '-', trim(coalesce(prs.addr3, '')), '-', trim(coalesce(prs.addr4, '')), '-', trim(coalesce(prs.city, '')), '-', trim(coalesce(prs.state, '')), '-', trim(coalesce(prs.zip, '')), '-', trim(coalesce(prs.country_code, '')))) = upper(xwlk_1.sk_source_txt)
               AND upper(xwlk_1.sk_type) = 'ADDRESS'
			   
			   /**Removed msh_employee_stg msh_prsystem_stg mission table code */
            
        ) AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.address AS tgt ON stg.sk = tgt.addr_sid
      WHERE tgt.addr_sid IS NULL
      GROUP BY 1, upper(stg.addr_type_cd), upper(stg.addr1), upper(stg.addr2), upper(stg.addr3), upper(stg.addr4), upper(stg.city), upper(stg.state), upper(stg.zip), upper(stg.county), upper(stg.country_code), upper(stg.location_code), upper(stg.source_system_code), 14
  ;
/* Test Unique Primary Index constraint set in Terdata */
     SET DUP_COUNT = ( 
        select count(*)
        from (
        select Addr_SID ,Addr_Type_Code
        from {{ params.param_hr_core_dataset_name }}.address 
        group by Addr_SID ,Addr_Type_Code		
        having count(*) > 1
        )
    );
	IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}`.address ');
    ELSE
      COMMIT TRANSACTION;
    END IF;  
  
END ;
