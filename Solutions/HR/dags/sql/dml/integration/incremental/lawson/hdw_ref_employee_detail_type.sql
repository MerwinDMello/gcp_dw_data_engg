declare ts datetime;
declare dup_count int64;

set ts = datetime_trunc(current_datetime('US/Central'), second);
begin

create temp table hruserflds_tmp as (
select a.field_type, 
case when b.employee_detail_type_desc is null 
then 'Unknown' 
else b.employee_detail_type_desc
end as employee_detail_type_desc from
(
SELECT
distinct field_type
from {{ params.param_hr_stage_dataset_name }}.hruserflds
UNION ALL
SELECT 'M' AS field_type 
from {{ params.param_hr_stage_dataset_name }}.hruserflds
) a
left outer join {{ params.param_hr_stage_dataset_name }}.ref_employee_detail_type_stg b
on a.field_type = b.employee_detail_type_code
group by a.field_type,b.employee_detail_type_desc);

begin transaction;
merge {{ params.param_hr_core_dataset_name }}.ref_employee_detail_type tgt
using hruserflds_tmp src
on tgt.employee_detail_type_code = src.field_type
when matched then 
update set 
tgt.employee_detail_type_desc = src.employee_detail_type_desc,
tgt.dw_last_update_date_time = ts
when not matched then 
insert (employee_detail_type_code, employee_detail_type_desc, source_system_code, dw_last_update_date_time)
values(field_type, employee_detail_type_desc, 'L', ts);

SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
       employee_detail_type_code
    FROM
      {{ params.param_hr_core_dataset_name }}.ref_employee_detail_type
    GROUP BY
       employee_detail_type_code
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: ref_employee_detail_type');
  ELSE
COMMIT TRANSACTION;
END IF;
END;
