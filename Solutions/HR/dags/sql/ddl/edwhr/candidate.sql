CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.candidate
(
candidate_sid INT64 NOT NULL OPTIONS(description="Unique identifier for each candidate.")
, valid_from_date DATETIME NOT NULL OPTIONS(description="Date the record is valid from based on when it was loaded.")
, candidate_num INT64 OPTIONS(description="The unique number for a candidate coming from the source.")
, in_hiring_process_sw INT64 OPTIONS(description="Numeric boolean to where 0 = false/no and 1 = true/yes")
, internal_candidate_sw INT64 OPTIONS(description="Numeric boolean to where 0 = false/no and 1 = true/yes")
, referred_sw INT64 OPTIONS(description="Numeric boolean to where 0 = false/no and 1 = true/yes")
, last_modified_date_time DATETIME OPTIONS(description="The most recent modified date of the candidates profile.")
, candidate_creation_date_time DATETIME OPTIONS(description="The date the candidate was created in the source.")
, residence_location_num INT64 OPTIONS(description="A number that represents the candidates location.")
, travel_preference_code STRING OPTIONS(description="Describes whether a candidate is willing to travel.")
, relocation_preference_code STRING OPTIONS(description="Descrbes whether a candidate is willing to relocate.")
, valid_to_date DATETIME OPTIONS(description="Date the record is valid to based on when it was loaded.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Datetime of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY candidate_sid
OPTIONS(description="The detail information from the candidates profile in the recruitment source.");