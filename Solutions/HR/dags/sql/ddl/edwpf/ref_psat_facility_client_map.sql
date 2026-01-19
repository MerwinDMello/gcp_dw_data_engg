CREATE TABLE IF NOT EXISTS {{ params.param_pf_core_dataset_name }}.ref_psat_facility_client_map (
company_code STRING NOT NULL OPTIONS(description="Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.")
, coid STRING NOT NULL OPTIONS(description="The company identifier which uniquely identifies a facility to corporate and all other facilities.")
, client_id STRING NOT NULL OPTIONS(description="The unique identifier for a unit that rolls up to a facility. Registered in the Company Master as an inactive COID.")
, location_mnemonic_cs STRING NOT NULL OPTIONS(description="A 10 character code to uniquely identify the dictionary or reference item within a network.  All networks are setup as with the MIS as shared dictionaries.  The mnemonic is unique at this level.")
, sub_unit_num STRING NOT NULL OPTIONS(description="The sub unit number within a facility.")
, service_code STRING NOT NULL OPTIONS(description="Identifies the principal clinical service or location where the patient receives service-assigned in Admitting")
, facility_name STRING OPTIONS(description="Facility Name from Company Master")
, survey_form_text STRING OPTIONS(description="The default survey that a location should receive. If left blank, Press Ganey uses a formula to determine which survey to give.")
, clinic_flag STRING OPTIONS(description="Indicates if a facility is a clinic and not a hospital or other type of unit.")
, location_exclusion_flag STRING OPTIONS(description="Indicates if a location should be excluded from being surveyed. Y indicates that yes, the location should be excluded.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
 CLUSTER BY Company_Code, Coid, Client_Id, Location_Mnemonic_CS
OPTIONS(description="Press Ganey requires each unit that rolls up to a facility to have a unique identifier. This table maps those identifiers with the facility they roll up to and has additional location specific information.");
