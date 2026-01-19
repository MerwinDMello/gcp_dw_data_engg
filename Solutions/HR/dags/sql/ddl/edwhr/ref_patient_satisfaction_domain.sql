create table if not exists {{ params.param_hr_core_dataset_name }}.ref_patient_satisfaction_domain (
domain_id int64 not null options(description="is the domain identifier assigned by the vendor and determines the domain for the patient satisfaction surveys.")
, domain_desc string options(description="description for the domain.")
, domain_group_id int64 options(description="this is the domain group identifier assigned by the vendor.")
, domain_group_desc string options(description="description for the domain group.")
, source_system_code string not null options(description="a one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time datetime not null options(description="timestamp of update or load of this record to the enterprise data warehouse.")
)
 cluster by domain_id
options(description="reference table for the domains assigned by the vendor");
