CREATE OR REPLACE VIEW edwpsc_views.`pv_stgerafileclaimlineallowed`
AS SELECT
  `pv_stgerafileclaimlineallowed`.allowed_id,
  `pv_stgerafileclaimlineallowed`.claimline_id,
  `pv_stgerafileclaimlineallowed`.claim_id,
  `pv_stgerafileclaimlineallowed`.payer_id,
  `pv_stgerafileclaimlineallowed`.fileid,
  `pv_stgerafileclaimlineallowed`.bpr_id,
  `pv_stgerafileclaimlineallowed`.trn_id,
  `pv_stgerafileclaimlineallowed`.allowed01,
  `pv_stgerafileclaimlineallowed`.allowedsegment,
  `pv_stgerafileclaimlineallowed`.inserteddtm,
  `pv_stgerafileclaimlineallowed`.gs_id,
  `pv_stgerafileclaimlineallowed`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`pv_stgerafileclaimlineallowed`
;