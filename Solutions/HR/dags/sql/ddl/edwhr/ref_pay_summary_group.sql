create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_pay_summary_group`
(
  pay_summary_group_code STRING NOT NULL OPTIONS(description="code that groups pay codes together."),
  lawson_company_num INT64 NOT NULL OPTIONS(description="unique four digit numeric value of lawson generated hr company maintained in this field"),
  pay_summary_group_desc STRING OPTIONS(description="contains the summary group description."),
  pay_summary_abbreviation_desc STRING OPTIONS(description="the pay summary group description that prints on the payment or pay receipt."),
  overtime_eligibility_pay_ind STRING OPTIONS(description="indicates if the pay associated with the pay summary group is eligible to be included in the employees regular rate."),
  overtime_eligibility_hour_ind STRING OPTIONS(description="indicates if the hours associated with the pay summary group are eligible to be included in the employees regular rate."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY pay_summary_group_code, lawson_company_num
OPTIONS(
  description="contains pay summary group information. the system uses pay summary groups to represent a group of pay codes."
);