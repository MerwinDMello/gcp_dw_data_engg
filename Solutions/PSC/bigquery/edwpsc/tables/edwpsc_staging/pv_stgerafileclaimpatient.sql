CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_stgerafileclaimpatient
(
  claim_patient_id INT64 NOT NULL,
  claim_id INT64,
  payer_id INT64,
  fileid INT64,
  bpr_id INT64,
  trn_id INT64,
  claimpatient01 STRING,
  claimpatient02 STRING,
  claimpatient03 STRING,
  claimpatient04 STRING,
  claimpatient05 STRING,
  claimpatient06 STRING,
  claimpatient07 STRING,
  claimpatient08 STRING,
  claimpatient09 STRING,
  claimpatientsegment STRING,
  inserteddtm DATETIME NOT NULL,
  gs_id INT64,
  dwlastupdatedatetime DATETIME NOT NULL
)
;