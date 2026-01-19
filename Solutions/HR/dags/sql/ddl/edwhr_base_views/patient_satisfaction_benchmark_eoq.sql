CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.patient_satisfaction_benchmark_eoq AS SELECT
    patient_satisfaction_benchmark_eoq.domain_id,
    patient_satisfaction_benchmark_eoq.measure_id_text,
    patient_satisfaction_benchmark_eoq.benchmark_rank_num,
    patient_satisfaction_benchmark_eoq.benchmark_owner_name,
    patient_satisfaction_benchmark_eoq.quarter_id,
    patient_satisfaction_benchmark_eoq.top_box_num,
    patient_satisfaction_benchmark_eoq.source_system_code,
    patient_satisfaction_benchmark_eoq.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.patient_satisfaction_benchmark_eoq
;