BEGIN

create temp table t1 as
select * from {{ params.param_hr_stage_dataset_name }}.galen_stg;

truncate table {{ params.param_hr_stage_dataset_name }}.galen_stg ;

insert into {{ params.param_hr_stage_dataset_name }}.galen_stg
select  
  campus_desc ,
  campus_code ,
  campus_city ,
  campus_state ,
  campus_zip ,
  grad_date ,
  program_ver_desc ,
  program_degree ,
  cumulative_gpa ,
  student_id ,
  ssn ,
  last_name ,
  first_name ,
  middle_initial ,
  gender ,
  hisp_latin_ethnicity_ind ,
  ethnicity_5_options ,
  birthdate ,
  home_address_street ,
  home_address_city ,
  home_address_state ,
  home_address_zip ,
  rn_exam_date ,
  pn_vn_exam_date ,
  pell_grant_eligibility ,
  first_generation_college_grad ,
  datetime_trunc(current_datetime('US/Central'), SECOND)
from t1
where campus_desc != 'CampusDesc' 
and student_id != ''
and student_id is not null;

END;