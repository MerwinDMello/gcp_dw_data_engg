CREATE TABLE IF NOT EXISTS
  {{ params.param_hr_stage_dataset_name }}.patient_satisfaction_benchmark_eoq ( domain_id int64 NOT NULL,
    measure_id string NOT NULL,
    percentile int64 NOT NULL,
    endofperiod_date date NOT NULL,
    top_box numeric ) ;