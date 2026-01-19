CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_heme_clinical_trial_stg (
patientclinicaltrialfactid INT64
, patientdimid INT64
, tumortypedimid INT64
, diagnosisdimid INT64
, facilitydimid INT64
, navigatordimid INT64
, diagnosisresultid INT64
, coid STRING
, hemeevaluated_screenedind STRING
, hemeevaluated_screeneddate STRING
, hemeofferedclinicaltrialind STRING
, hemeofferedclinicaltrialdate STRING
, hemepatientenrolledinclinicaltrial STRING
, hemeenrolledinclinicaltrialdate STRING
, hemeclinicaltrialname STRING
, hemeclinicaltrialothername STRING
, hemetrialnotoffered STRING
, hemetrialnotofferedother STRING
, hemenotscreened STRING
, hemenotscreenedother STRING
, hbsource STRING
, dw_last_update_date_time DATETIME
)
  ;
