create table if not exists {{ params.param_hr_core_dataset_name }}.ref_phone_mode_adjustment (
measure_id_text string not null options(description="this is a grouping for some of the questions; this is a static list maintained by the csg.")
, eff_from_date date not null options(description="effective start date of a record")
, mode_adjustment_amt numeric options(description="is the value that is subtracted from the topbox value for the phone mode adjustment")
, bottom_mode_adjustment_amt numeric options(description="is the value that is subtracted from the bottombox value for the phone mode adjustment")
, eff_to_date date not null options(description="record effective end date")
, source_system_code string not null options(description="a one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time datetime not null options(description="timestamp of update or load of this record to the enterprise data warehouse.")
)
partition by eff_from_date
cluster by measure_id_text, eff_from_date
options(description="this is a static table that holds phone mode adjustment values to compare mail and phone surveys");
