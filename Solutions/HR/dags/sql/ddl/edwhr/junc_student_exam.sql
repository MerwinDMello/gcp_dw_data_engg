CREATE TABLE IF NOT EXISTS {{params.param_hr_core_dataset_name}}.junc_student_exam
(
  student_sid INT64 NOT NULL OPTIONS(description="Unique system generated identifier for student"),
  exam_id INT64 NOT NULL OPTIONS(description="Unique system generated identifier for nursing licensure exam"),
  valid_from_date DATETIME NOT NULL OPTIONS(description="Date the record is valid from based on when it was loaded."),
  valid_to_date DATETIME OPTIONS(description="Date the record is valid to based on when it was loaded."),
  exam_date DATE OPTIONS(description="It contains the date of exam"),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")

)
CLUSTER BY student_sid OPTIONS(description="This table contains the exam date of National Council Licensure Examination for nursing graduates.");