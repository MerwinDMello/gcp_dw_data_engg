CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_dimhospitaldepartment (
dimsiteid STRING
, dimhospitaldepartmentid STRING
, hospitalname STRING
, hospitallocation STRING
, hospitalcompleteaddress STRING
, hospitalwebaddress STRING
, departmentid STRING
, departmentname STRING
, departmentcomment STRING
, departmenthstrydatetime STRING
, departmenthstryusername STRING
, hospitalhstrydatetime STRING
, hospitalhstryusername STRING
, ctrhospitalser STRING
, ctrdepartmentser STRING
, logid STRING
, runid STRING
, ctrinst_id STRING
, hospitaltelephoneextension STRING
, dw_last_update_date_time DATETIME
)
  ;
