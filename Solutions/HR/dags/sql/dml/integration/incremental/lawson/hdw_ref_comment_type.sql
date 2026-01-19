BEGIN
DECLARE DUP_COUNT INT64;
DECLARE ts datetime;
SET ts = datetime_trunc(current_datetime('US/Central'), SECOND) ;


BEGIN TRANSACTION;

delete from {{ params.param_hr_core_dataset_name }}.ref_comment_type where true;
insert into {{ params.param_hr_core_dataset_name }}.ref_comment_type (comment_type_code, comment_type_desc, source_system_code, dw_last_update_date_time)
select comment_type_code, comment_type_desc, 'L', ts
from {{ params.param_hr_stage_dataset_name }}.ref_comment_type_stg
where comment_type_code is not null;

SET DUP_COUNT = ( 
        select count(*)
        from (
        select comment_type_code
        from {{ params.param_hr_core_dataset_name }}.ref_comment_type
        group by comment_type_code
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: ref_comment_type');
    ELSE
      COMMIT TRANSACTION;
    END IF;  
END;