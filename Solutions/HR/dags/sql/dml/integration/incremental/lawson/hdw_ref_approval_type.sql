declare ts datetime;
declare dup_count int64;
set ts = datetime_trunc(current_datetime('US/Central'), second);

begin
begin transaction;
merge {{ params.param_hr_core_dataset_name }}.ref_approval_type tgt
using {{ params.param_hr_stage_dataset_name }}.ref_approval_type_stg src
on tgt.approval_type_code = src.approval_type_code
when matched then
update set dw_last_update_date_time = ts
when not matched then
insert(approval_type_code, approval_type_desc, source_system_code, dw_last_update_date_time)
values(approval_type_code, approval_type_desc, 'L', ts);

SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
       approval_type_code
    FROM
      {{ params.param_hr_core_dataset_name }}.ref_approval_type
    GROUP BY
       approval_type_code
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: ref_approval_type');
  ELSE
COMMIT TRANSACTION;
END IF;
END;

