CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.student_program_graduation AS SELECT
    student_program_graduation.student_sid,
    student_program_graduation.nursing_program_id,
    student_program_graduation.valid_from_date,
    student_program_graduation.graduation_date,
    student_program_graduation.cumulative_gpa,
    student_program_graduation.nursing_school_id,
    student_program_graduation.campus_id,
    student_program_graduation.valid_to_date,
    student_program_graduation.source_system_code,
    student_program_graduation.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.student_program_graduation
;