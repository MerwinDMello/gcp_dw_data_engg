CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_surgical_procedure_classification
(
  procedure_code STRING NOT NULL OPTIONS(description='This is the surgical procedure or service provided by the practitioner or healthcare provider.'),
  procedure_type_code STRING NOT NULL OPTIONS(description='Distinguishes type of Procedure, ICD9, ICD10 or HCPCs'),
  procedure_desc STRING OPTIONS(description='Description of the surgical procedure.'),
  procedure_site_name STRING OPTIONS(description='The body location where the procedure took place.'),
  procedure_category_name STRING OPTIONS(description='High level categorization of the procedure defined by the business.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY procedure_code, procedure_type_code
OPTIONS(
  description='Table maintained by the business that assigns a procedure site and category for each procedure code and procedure type code.'
);
