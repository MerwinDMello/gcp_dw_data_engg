declare ts datetime;
declare dup_count int64;
set ts = datetime_trunc(current_datetime('US/Central'), second);

begin
begin transaction;
merge {{ params.param_hr_core_dataset_name }}.ref_status_type tgt
using {{ params.param_hr_stage_dataset_name }}.ref_status_type_stg src
on tgt.status_type_code = src.status_type_code
when matched then
update set tgt.status_type_desc = src.status_type_desc,
tgt.dw_last_update_date_time = ts
when not matched then
insert(status_type_code,status_type_desc, source_system_code, dw_last_update_date_time)
values(status_type_code, status_type_desc, 'L', ts);

SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
       status_type_code
    FROM
      {{ params.param_hr_core_dataset_name }}.ref_status_type
    GROUP BY
       status_type_code
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: ref_status_type');
  ELSE
COMMIT TRANSACTION;
END IF;
END;
