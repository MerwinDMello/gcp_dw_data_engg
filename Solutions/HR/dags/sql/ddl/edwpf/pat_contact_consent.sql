CREATE TABLE IF NOT EXISTS {{ params.param_pf_core_dataset_name }}.pat_contact_consent (
patient_dw_id NUMERIC(18,0) NOT NULL OPTIONS(description="A system generated number used to uniquely identify a patient.")
, medical_record_num STRING NOT NULL OPTIONS(description="The Unique Identifier for a patient within the confines of a Hospital.  It may span multiple encounters.")
, company_code STRING NOT NULL OPTIONS(description="Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.")
, coid STRING NOT NULL OPTIONS(description="The company identifier which uniquely identifies a facility to corporate and all other facilities.")
, pat_acct_num NUMERIC(12,0) OPTIONS(description="A unique number assigned by the hospital to the patient at time of registration.")
, pat_contact_consent_ind STRING OPTIONS(description="Contains the indicator designating whether the contact flag comes from Vista.")
, vendor_contact_consent_ind STRING OPTIONS(description="Indicator designates if the contact flag comes from the vendors do not contact list.")
, exclusion_type_code STRING OPTIONS(description="Contains the type of patient exclusion from Vista.")
, vendor_notified_date DATE OPTIONS(description="Contains the date when the patient entered to the do not contact list.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
 CLUSTER BY Patient_DW_Id
OPTIONS(description="Contains the information of patients who do not wish to be contacted about surveys.");
