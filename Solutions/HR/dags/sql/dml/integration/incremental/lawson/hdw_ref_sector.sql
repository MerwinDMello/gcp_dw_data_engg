BEGIN
DECLARE DUP_COUNT INT64;
DECLARE ts datetime;
SET ts = datetime_trunc(current_datetime('US/Central'), SECOND) ;

begin transaction;
merge {{ params.param_hr_core_dataset_name }}.ref_sector tgt
using {{ params.param_hr_stage_dataset_name }}.ref_sector_stg src
on tgt.sector_code = src.sector_code
when matched then 
update set tgt.sector_desc = src.sector_desc,
tgt.dw_last_update_date_time = ts
when not matched then
insert(sector_code, sector_desc, source_system_code, dw_last_update_date_time)
values(sector_code, sector_desc, 'L', ts);

SET DUP_COUNT = ( 
        select count(*)
        from (
        select sector_code
        from {{ params.param_hr_core_dataset_name }}.ref_sector
        group by sector_code
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: ref_sector');
    ELSE
      COMMIT TRANSACTION;
    END IF;  
END;