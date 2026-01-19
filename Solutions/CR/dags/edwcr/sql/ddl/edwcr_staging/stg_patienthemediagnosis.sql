CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_patienthemediagnosis (
patienthemediagnosisfactid INT64
, patientdimid INT64
, diseasestatus STRING
, tumortypedimid INT64
, diagnosisresultid INT64
, diagnosisdimid INT64
, navigatordimid INT64
, facilitydimid INT64
, coid STRING
, specimandate STRING
, diseasediagnosis STRING
, therapyrelated STRING
, transformedfrommds STRING
, mipi STRING
, ipi STRING
, flipi STRING
, aidsrelated STRING
, comments STRING
, classification STRING
, subclassification STRING
, nhltype STRING
, othernhltype STRING
, transformeddisease STRING
, nonmalignanttype STRING
, features STRING
, risk STRING
, riskripss STRING
, stagingfield1 STRING
, stagingfield2 STRING
, hbsource STRING
)
  ;
