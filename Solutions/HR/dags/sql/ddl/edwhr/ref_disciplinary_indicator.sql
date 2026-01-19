create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_disciplinary_indicator`
(
  disciplinary_ind STRING NOT NULL OPTIONS(description="indicates whether the action is a grievance (1) or disciplinary (2) issue or dummy value (0)."),
  disciplinary_desc STRING OPTIONS(description="contains disciplinary or grievance action description."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY disciplinary_ind
OPTIONS(
  description="unique list of all employee / non employee disciplinary action information is maintained in this field"
);