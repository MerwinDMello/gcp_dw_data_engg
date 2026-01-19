CREATE OR REPLACE VIEW edwpsc_base_views.`pv_stgerafileclaimpatient`
AS SELECT
  `pv_stgerafileclaimpatient`.claim_patient_id,
  `pv_stgerafileclaimpatient`.claim_id,
  `pv_stgerafileclaimpatient`.payer_id,
  `pv_stgerafileclaimpatient`.fileid,
  `pv_stgerafileclaimpatient`.bpr_id,
  `pv_stgerafileclaimpatient`.trn_id,
  `pv_stgerafileclaimpatient`.claimpatient01,
  `pv_stgerafileclaimpatient`.claimpatient02,
  `pv_stgerafileclaimpatient`.claimpatient03,
  `pv_stgerafileclaimpatient`.claimpatient04,
  `pv_stgerafileclaimpatient`.claimpatient05,
  `pv_stgerafileclaimpatient`.claimpatient06,
  `pv_stgerafileclaimpatient`.claimpatient07,
  `pv_stgerafileclaimpatient`.claimpatient08,
  `pv_stgerafileclaimpatient`.claimpatient09,
  `pv_stgerafileclaimpatient`.claimpatientsegment,
  `pv_stgerafileclaimpatient`.inserteddtm,
  `pv_stgerafileclaimpatient`.gs_id,
  `pv_stgerafileclaimpatient`.dwlastupdatedatetime
  FROM
    edwpsc.`pv_stgerafileclaimpatient`
;