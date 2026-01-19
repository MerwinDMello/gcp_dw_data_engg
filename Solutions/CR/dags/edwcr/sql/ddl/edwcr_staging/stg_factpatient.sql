CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_factpatient (
dimsiteid INT64
, factpatientid INT64
, dimpatientid INT64
, dimnationalityid INT64
, dimlookupid_race INT64
, dimlookupid_gender INT64
, dimlocationid INT64
, dimhospitaldepartmentid INT64
, dimprimaryoncologistid INT64
, dimdateid_patientcreation INT64
, dimdateid_patientdischarge INT64
, dimdateid_patientadmission INT64
, dimprimaryreferringphysicianid INT64
, dimlookupid_patientstatus INT64
, patientcreationdate STRING
, patientadmissiondate STRING
, patientdischargedate STRING
, logid INT64
, dimmedonchospitaldepartmentid INT64
, dimmedoncprimaryoncologistid INT64
, dimmedoncprimaryrfringphyscnid INT64
, runid INT64
, moroindicator STRING
, dw_last_update_date_time DATETIME
)
  ;
