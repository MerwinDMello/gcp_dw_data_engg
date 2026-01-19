create table if not exists {{ params.param_hr_core_dataset_name }}.ref_nursing_program (
nursing_program_id int64 not null options(description="unique  nursing program identifier")
, program_name string not null options(description="contains nursing program name")
, program_type_code string options(description="it contains program type code online or on prem.")
, program_degree_text string options(description="contains nursing program degree")
, source_system_code string not null options(description="a one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time datetime not null options(description="timestamp of update or load of this record to the enterprise data warehouse.")
)
cluster by nursing_program_id
options(description="this table contains various nursing programs and degrees associated with it, that a student can enroll.");
