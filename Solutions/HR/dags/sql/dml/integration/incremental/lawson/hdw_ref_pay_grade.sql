
declare dup_count int64;
DECLARE current_ts datetime;
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

begin
create temp table ref_pay_grade_temp as (
select 
pay_grade_code,
pay_grade_desc,
source_system_code,
dw_last_update_date_time
from
(
select 
 pay_grade as pay_grade_code,
'null' as pay_grade_desc,
source_system_code,
current_ts as dw_last_update_date_time
from (
select distinct(trim(pay_grade)) as pay_grade,'L' as source_system_code ,1 as priority  from 
{{ params.param_hr_stage_dataset_name }}.employee
union distinct
select distinct(trim(pay_grade)) as pay_grade,'L' as source_system_code,1 as priority  from 
{{ params.param_hr_stage_dataset_name }}.pajobreq
union distinct
select distinct(trim(pay_grade)) as pay_grade,'L' as source_system_code ,1 as priority from 
{{ params.param_hr_stage_dataset_name }}.jobcode
union distinct
select distinct(trim(pay_grade)) as pay_grade,'L' as source_system_code ,1 as priority from 
{{ params.param_hr_stage_dataset_name }}.prsagdtl
union distinct
select distinct(trim(pay_grade)) as pay_grade,'L' as source_system_code ,1 as priority from 
{{ params.param_hr_stage_dataset_name }}.paposition
union distinct
select distinct(trim(pay_grade)) as pay_grade,'L' as source_system_code,1 as priority  from 
{{ params.param_hr_stage_dataset_name }}.paemppos

-- union
-- (
-- select distinct(trim(pay_grade)) as pay_grade,'a' as source_system_code,2 as priority from 
-- {{ params.param_hr_stage_dataset_name }}.msh_employee_stg
-- union
-- select distinct(trim(pay_grade)) as pay_grade,'a' as source_system_code,2 as priority  from 
-- {{ params.param_hr_stage_dataset_name }}.msh_pajobreq_stg
-- union 
-- select distinct(trim(pay_grade)) as pay_grade,'a' as source_system_code,2 as priority  from 
-- {{ params.param_hr_stage_dataset_name }}.msh_jobcode_stg
-- union 
-- select distinct(trim(pay_grade)) as pay_grade,'a' as source_system_code,2 as priority from 
-- {{ params.param_hr_stage_dataset_name }}.msh_prsagdtl_stg
-- union
-- select distinct(trim(pay_grade)) as pay_grade,'a' as source_system_code,2 as priority  from 
-- {{ params.param_hr_stage_dataset_name }}.msh_paposition_stg
-- union 
-- select distinct(trim(pay_grade)) as pay_grade,'a' as source_system_code,2 as priority  from 
-- {{ params.param_hr_stage_dataset_name }}.msh_paemppos_stg)
-- )a  
-- qualify row_number() over (partition by pay_grade order by priority )=1
))
where pay_grade_code not in (select pay_grade_code from {{ params.param_hr_core_dataset_name }}.ref_pay_grade )
and case when pay_grade_code='' then null else pay_grade_code end is not null
);


begin transaction;
merge {{ params.param_hr_core_dataset_name }}.ref_pay_grade tgt
using ref_pay_grade_temp src
on tgt.pay_grade_code = src.pay_grade_code
when matched then 
update set
tgt.pay_grade_desc = src.pay_grade_desc,
tgt.source_system_code = src.source_system_code,
tgt.dw_last_update_date_time = src.dw_last_update_date_time
when not matched then
insert(pay_grade_code, pay_grade_desc, source_system_code, dw_last_update_date_time)
values(pay_grade_code, pay_grade_desc, source_system_code, dw_last_update_date_time);

SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
       pay_grade_code
    FROM
      {{ params.param_hr_core_dataset_name }}.ref_pay_grade
    GROUP BY
       pay_grade_code
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: ref_action');
  ELSE
COMMIT TRANSACTION;
END IF;
END;