CREATE OR REPLACE VIEW edwpsc_base_views.`pv_stgerafileclaimlinerarc`
AS SELECT
  `pv_stgerafileclaimlinerarc`.rarc_id,
  `pv_stgerafileclaimlinerarc`.claimline_id,
  `pv_stgerafileclaimlinerarc`.claim_id,
  `pv_stgerafileclaimlinerarc`.payer_id,
  `pv_stgerafileclaimlinerarc`.fileid,
  `pv_stgerafileclaimlinerarc`.bpr_id,
  `pv_stgerafileclaimlinerarc`.trn_id,
  `pv_stgerafileclaimlinerarc`.rarc01,
  `pv_stgerafileclaimlinerarc`.rarc02_code,
  `pv_stgerafileclaimlinerarc`.rarcsegment,
  `pv_stgerafileclaimlinerarc`.inserteddtm,
  `pv_stgerafileclaimlinerarc`.gs_id,
  `pv_stgerafileclaimlinerarc`.dwlastupdatedatetime
  FROM
    edwpsc.`pv_stgerafileclaimlinerarc`
;