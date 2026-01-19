declare ts datetime;
declare dup_count int64;

set ts = datetime_trunc(current_datetime('US/Central'), second);
begin

create temp table hruserflds_tmp as (
select

hr.Field_Key,
hr.Field_Type,
hr.Field_Name,
hr.Beg_date
from {{ params.param_hr_stage_dataset_name }}.hruserflds hr
inner join 
(select
Field_Key,
MAX(Beg_date) as Beg_date
from {{ params.param_hr_stage_dataset_name }}.hruserflds
group by 1)a

on hr.Field_Key =a.Field_Key
AND hr.Beg_date =a.Beg_date
GROUP BY 1,2,3,4
);

begin transaction;
merge {{ params.param_hr_core_dataset_name }}.ref_employee_detail tgt
using hruserflds_tmp src
on tgt.employee_detail_code = src.field_key
when matched then 
update set 
employee_detail_type_code = field_type,
employee_detail_desc = field_name,
dw_last_update_date_time = ts
when not matched then 
insert (employee_detail_code, employee_detail_type_code, employee_detail_desc, source_system_code, dw_last_update_date_time)
values(field_key, field_type, field_name, 'L', ts);

SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
       employee_detail_code
    FROM
      {{ params.param_hr_core_dataset_name }}.ref_employee_detail
    GROUP BY
       employee_detail_code
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: ref_employee_detail');
  ELSE
COMMIT TRANSACTION;
END IF;
END;

