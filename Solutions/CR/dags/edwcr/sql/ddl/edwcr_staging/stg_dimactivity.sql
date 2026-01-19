CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_dimactivity (
dimsiteid STRING
, dimactivityid STRING
, activitycategorycode STRING
, activitycategoryenu STRING
, activitycategoryfra STRING
, activitycategoryesn STRING
, activitycategorychs STRING
, activitycategorydeu STRING
, activitycategoryita STRING
, activitycategoryjpn STRING
, activitycategoryptb STRING
, activitycategorysve STRING
, activitycode STRING
, activitynameenu STRING
, activitynamefra STRING
, activitynameesn STRING
, activitynamechs STRING
, activitynamedeu STRING
, activitynameita STRING
, activitynamejpn STRING
, activitynameptb STRING
, activitynamesve STRING
, activitytype STRING
, lastmodifiedon STRING
, activityrevcount STRING
, defaultduration STRING
, ctractivityser STRING
, ctractivitycategoryser STRING
, logid STRING
, runid STRING
, dimlookupid_activityobjectstat STRING
, effectivestartdate STRING
, effectiveenddate STRING
, dw_last_update_date_time DATETIME
)
  ;
