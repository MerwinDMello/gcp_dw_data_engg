
CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.junc_student_exam AS SELECT
    junc_student_exam.student_sid,
    junc_student_exam.exam_id,
    junc_student_exam.valid_from_date,
    junc_student_exam.valid_to_date,
    junc_student_exam.exam_date,
    junc_student_exam.source_system_code,
    junc_student_exam.dw_last_update_date_time
  FROM
    {{params.param_hr_core_dataset_name}}.junc_student_exam
;