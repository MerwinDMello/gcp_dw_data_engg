CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.student_program_graduation (
student_sid INT64 NOT NULL OPTIONS(description="Unique system generated identifier  for student")
, nursing_program_id INT64 NOT NULL OPTIONS(description="Unique  nursing program identifier")
, valid_from_date DATETIME NOT NULL OPTIONS(description="Date the record is valid from based on when it was loaded.")
, graduation_date DATE NOT NULL OPTIONS(description="Date of graduation for nursing program")
, cumulative_gpa NUMERIC(4,2) OPTIONS(description="Cumulative grade point average of nursing student at the time of graduation")
, nursing_school_id INT64 OPTIONS(description="Unique nursing school identifier")
, campus_id INT64 OPTIONS(description="This contains unique campus identifier associated with nursing school.")
, valid_to_date DATETIME OPTIONS(description="Date the record is valid to based on when it was loaded.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
 CLUSTER BY Student_SID
OPTIONS(description="This table contains information related to students and their nursing program graduation.");