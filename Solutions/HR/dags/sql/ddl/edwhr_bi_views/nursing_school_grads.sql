CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.nursing_school_grads AS SELECT
    ns.employee_num,
    rns.nursing_school_name,
    rnsc.campus_name,
    rnp.program_degree_text,
    rnp.program_name,
    spg.graduation_date
  FROM
    {{ params.param_hr_base_views_dataset_name }}.nursing_student AS ns
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.student_program_graduation AS spg ON ns.student_sid = spg.student_sid
     AND date(spg.valid_to_date) = date'9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_nursing_school_campus AS rnsc ON spg.campus_id = rnsc.campus_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_nursing_school AS rns ON rnsc.nursing_school_id = rns.nursing_school_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_nursing_program AS rnp ON spg.nursing_program_id = rnp.nursing_program_id
  WHERE date(ns.valid_to_date) = date'9999-12-31'
;
