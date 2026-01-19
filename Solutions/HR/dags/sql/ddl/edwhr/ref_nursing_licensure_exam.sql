CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_nursing_licensure_exam (
exam_id INT64 NOT NULL OPTIONS(description="Unique system generated identifier for nursing licensure exam")
, exam_name STRING OPTIONS(description="This contains the name of licensure exam")
, exam_desc STRING OPTIONS(description="It contains the description of licensure exam")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
 CLUSTER BY Exam_Id
OPTIONS(description="This table contains the information related to nursing licensure exams.");