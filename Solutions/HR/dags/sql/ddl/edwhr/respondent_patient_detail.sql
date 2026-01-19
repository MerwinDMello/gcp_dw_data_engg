CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.respondent_patient_detail
(
respondent_id NUMERIC NOT NULL OPTIONS(description="This is the respondent identifier that is assigned by the vendor.")
, survey_receive_date DATE NOT NULL OPTIONS(description="The date the survey was received")
, respondent_type_code STRING NOT NULL OPTIONS(description="A one character code indicating the type of the respondent.")
, survey_sid INT64 NOT NULL OPTIONS(description="It is the ETL generated unique sequence number for each survey.")
, company_code STRING OPTIONS(description="Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.")
, coid STRING OPTIONS(description="The company identifier which uniquely identifies a facility to corporate and all other facilities.")
, parent_coid STRING OPTIONS(description="The company identifier which uniquely identifies a facility to corporate and all other facilities.")
, pat_acct_num NUMERIC OPTIONS(description="A unique number assigned by the hospital to the patient at time of registration.")
, patient_dw_id NUMERIC OPTIONS(description="Is the data warehouse identifer for the patient. Any PHI infomation can be null for surveys that dont distribute PHI data like OASCAHPS")
, discharge_date DATE OPTIONS(description="Discharge date of the patient.")
, facility_claim_control_num STRING OPTIONS(description="Medicare provider claim control number.")
, exclusion_reason_code STRING OPTIONS(description="This is the reason code for the exclusions")
, cms_exclusion_ind STRING OPTIONS(description="This identifies if the record is excluded from CMS reporting.")
, final_record_ind STRING OPTIONS(description="This indicator defines if the response record is the final record. Final data is received in Feb,May,Aug,Nov for the previous quarter data.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY respondent_id, respondent_type_code, survey_sid
OPTIONS(description="This table contains the details of the patients who responded to the patient related surveys.");