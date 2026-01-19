declare ts datetime;
declare DUP_COUNT int64;
set ts = datetime_trunc(current_datetime('US/Central'), second);

begin
begin transaction;
create temp table hrempusf as (
SELECT 
DISTINCT emp_app AS applicant_type_id,
CASE
                WHEN emp_app=0 THEN 'Employee'
                WHEN emp_app=1 THEN 'Applicant'
                ELSE 'Unknown' 
                END AS applicant_type_desc
FROM 
{{ params.param_hr_stage_dataset_name }}.hrempusf
);

merge {{ params.param_hr_core_dataset_name }}.ref_applicant_type tgt
using hrempusf src
on tgt.applicant_type_id = src.applicant_type_id
when matched then
update set tgt.dw_last_update_date_time = ts
when not matched then
insert(applicant_type_id, applicant_type_desc, source_system_code, dw_last_update_date_time )
values(applicant_type_id, applicant_type_desc, 'L', ts);


SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
       applicant_type_id
    FROM
      {{ params.param_hr_core_dataset_name }}.ref_applicant_type
    GROUP BY
       applicant_type_id
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: ref_applicant_type');
  ELSE
COMMIT TRANSACTION;
END IF;
END;

