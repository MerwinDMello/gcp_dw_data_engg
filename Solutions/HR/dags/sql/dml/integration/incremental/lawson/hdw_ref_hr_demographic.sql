BEGIN
DECLARE DUP_COUNT INT64;
DECLARE current_ts datetime;
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
DELETE FROM {{ params.param_hr_core_dataset_name }}.ref_hr_demographic WHERE source_system_code='L';
BEGIN TRANSACTION ;

  MERGE into {{ params.param_hr_core_dataset_name }}.ref_hr_demographic tgt

        using (
          select * from (SELECT
              hr.hrctry_code AS demographic_code,
              hr.type AS demographic_type_code,
              hr.active_flag,
              hr.description AS demographic_desc,
              row_number() OVER (PARTITION BY hr.hrctry_code, hr.type ORDER BY hr.description DESC) as rnk,
              'L' AS source_system_code,
              current_ts AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.hrctrycode AS hr
              where hr.active_flag = 'A' or hr.hrctry_code = 'U' ) intrm_src where rnk=1 ) as src
		
		on upper(tgt.demographic_code) = upper(src.demographic_code) and 
		upper(tgt.demographic_type_code) = upper(src.demographic_type_code)
		when matched then 
    update
		set tgt.demographic_desc =src.demographic_desc,tgt.source_system_code=src.source_system_code,tgt.active_flag=src.active_flag ,tgt.dw_last_update_date_time=src.dw_last_update_date_time
		when not matched then 
		insert (demographic_code, demographic_type_code, active_flag, demographic_desc, source_system_code, dw_last_update_date_time) values (demographic_code,demographic_type_code,active_flag,demographic_desc,source_system_code,dw_last_update_date_time);
  
 /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select demographic_code ,demographic_type_code
        from {{ params.param_hr_core_dataset_name }}.ref_hr_demographic
        group by demographic_code ,demographic_type_code		
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_hr_demographic');
    ELSE
      COMMIT TRANSACTION;
    END IF; 
   
END ;
