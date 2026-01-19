create table if not exists
  {{ params.param_hr_core_dataset_name }}.patient_satisfaction_domain_data_eoq ( time_period_name string not null options(description="identifies the time period for the data."),
    company_code string not null options(description="part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes"),
    corporate_code string not null options(description="five character code for the corporate company like hca, lifepoint."),
    group_code string not null options(description="five character code for the group in the organization structure."),
    division_code string not null options(description="five character code for the division in the organization structure."),
    market_code string not null options(description="five character code for the market in the organization structure."),
    parent_coid string not null options(description="five character code for the parent coid in the organization structure."),
    coid string not null options(description="five character code for the parent coid in the organization structure. this is only to implement security."),
    facility_claim_control_num string not null options(description="medicare provider claim control number."),
    domain_id int64 not null options(description="this is the domain identifier assigned by the vendor."),
    organization_level_text string options(description="this is the level in the organization hierarchy like group, division etc."),
    respondent_count int64 options(description="count of distinct patients that responded to the survey"),
    top_box_num numeric options(description="this is the top box score for the organization level. this is computed by the vendor."),
    vendor_assigned_percentile_rank_num int64 options(description="this is the vendor assigned percentile rank for the organzation level."),
    reporting_period_text string options(description="groups the data into assigned reporting buckets"),
    source_system_code string not null options(description="a one character code indicating the specific source system from which the data originated."),
    dw_last_update_date_time datetime not null options(description="timestamp of update or load of this record to the enterprise data warehouse.") )
cluster by
  time_period_name,
  company_code,
  corporate_code,
  group_code options(description="contains the quarterly patient domain summary data obtained from the vendor.");