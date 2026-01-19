create table if not exists
    {{ params.param_hr_core_dataset_name }}.patient_satisfaction_benchmark_eoq ( 
    domain_id int64 not null options(description="this is the domain identifier assigned by the vendor."),
    measure_id_text string not null options(description="this is a grouping for some of the questions; this is a static list maintained by the csg."),
    benchmark_rank_num int64 not null options(description="this is the vendor assigned percentile rank for each measure. benchmark is reported against it."),
    benchmark_owner_name string not null options(description="is the name of the owner of the benchmark data."),
    quarter_id int64 not null options(description="this is the identifier for the quarter."),
    top_box_num numeric options(description="this is the top box score for the organization level. this is computed by the vendor."),
    source_system_code string not null options(description="a one character code indicating the specific source system from which the data originated."),
    dw_last_update_date_time datetime not null options(description="datetime of update or load of this record to the enterprise data warehouse.") )
cluster by
  domain_id,
  measure_id_text,
  benchmark_rank_num,
  benchmark_owner_name options(description="this table holds the benchmark numbers for the patient satisfaction data");