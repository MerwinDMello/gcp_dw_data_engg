create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_address_type`
(
  addr_type_code STRING NOT NULL OPTIONS(description="it captures address type code for an business facility or individual person address. for example ind - individual person address, bus-business facility address"),
  addr_type_desc STRING NOT NULL OPTIONS(description="it maintains description of account type code like business facility address, individual person address"),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY addr_type_code
OPTIONS(description="it maintains unique list of address type codes in this table.");