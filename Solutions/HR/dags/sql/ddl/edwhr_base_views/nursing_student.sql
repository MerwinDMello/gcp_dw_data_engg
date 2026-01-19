CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.nursing_student AS SELECT
    nursing_student.student_sid,
    nursing_student.valid_from_date,
    nursing_student.valid_to_date,
    nursing_student.student_num,
    nursing_student.student_ssn,
    nursing_student.student_first_name,
    nursing_student.student_last_name,
    nursing_student.student_middle_name,
    nursing_student.birth_date,
    nursing_student.gender_code,
    nursing_student.ethnic_origin_desc,
    nursing_student.addr_sid,
    nursing_student.pell_grant_eligibility_ind,
    nursing_student.first_gen_college_grad_ind,
    nursing_student.employee_num,
    nursing_student.source_system_code,
    nursing_student.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.nursing_student
;