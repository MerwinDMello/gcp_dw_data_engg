BEGIN

create temp table t1 as
select 
report_number,
nullif(trim(report_created_date) ,'') as report_created_date,
trim(report_status) as report_status,
CASE
WHEN trim(applicant_last_name) is null then ''
ELSE trim(applicant_last_name)
END as applicant_last_name,
trim(applicant_first_name) as applicant_first_name,
CASE
WHEN trim(applicant_middle_name) is null then ''
ELSE trim(applicant_middle_name)
END as applicant_middle_name,
CASE
WHEN trim(ssn) is null then ''
ELSE trim(ssn)
END as ssn,
nullif(trim(report_reopened_date) ,'') as report_reopened_date,
nullif(trim(report_completion_date) ,'') as report_completion_date,
CASE
WHEN processlevel is null then '0'
ELSE processlevel 
END as processlevel,
CASE
WHEN trim(requisition) is null then ''
ELSE trim(requisition)
END as requisition,
trim(account_name)account_name,
trim(elapsed_days)elapsed_days,
criminal_searches_ordered,
criminal_searches_pending,
  mvr_ordered ,
  mvr_pending ,
  emp_verifications_ordered ,
  emp_verifications_pending ,
  edu_verifications_ordered ,
  edu_verifications_pending ,
  license_verifications_ordered ,
  license_verifications_pending ,
  personal_references_ordered ,
  personal_references_pending ,
  sanctioncheck_ordered ,
  sanctioncheck_pending ,
percentage_completed,
resultsurl,
dw_last_update_date_time
from {{ params.param_hr_stage_dataset_name }}.precheck_stg;

truncate table {{ params.param_hr_stage_dataset_name }}.precheck_stg ;

insert into {{ params.param_hr_stage_dataset_name }}.precheck_stg
select
report_number ,
  report_created_date ,
  report_status ,
  applicant_last_name ,
  applicant_first_name ,
  applicant_middle_name ,
  ssn ,
  report_reopened_date ,
  report_completion_date ,
  processlevel,
  requisition ,
  account_name ,
  elapsed_days ,
  criminal_searches_ordered ,
  criminal_searches_pending ,
  mvr_ordered ,
  mvr_pending ,
  emp_verifications_ordered ,
  emp_verifications_pending ,
  mvr_ordered ,
  mvr_pending ,
  emp_verifications_ordered ,
  emp_verifications_pending ,
  edu_verifications_ordered ,
  edu_verifications_pending ,
  license_verifications_ordered ,
  license_verifications_pending ,
  percentage_completed ,
  resultsurl ,
  datetime_trunc(current_datetime('US/Central'), SECOND)
from t1
where length(processlevel) <= 5
and report_status != 'Report Status';

END;