CREATE OR REPLACE VIEW edwpsc_base_views.`pv_stgerafileclaimline`
AS SELECT
  `pv_stgerafileclaimline`.claimline_id,
  `pv_stgerafileclaimline`.claim_id,
  `pv_stgerafileclaimline`.payer_id,
  `pv_stgerafileclaimline`.fileid,
  `pv_stgerafileclaimline`.bpr_id,
  `pv_stgerafileclaimline`.trn_id,
  `pv_stgerafileclaimline`.claimlinecpt,
  `pv_stgerafileclaimline`.claimlinemod1,
  `pv_stgerafileclaimline`.claimlinemod2,
  `pv_stgerafileclaimline`.claimlinemod3,
  `pv_stgerafileclaimline`.claimlinemod4,
  `pv_stgerafileclaimline`.claimline02,
  `pv_stgerafileclaimline`.claimline03,
  `pv_stgerafileclaimline`.claimline04,
  `pv_stgerafileclaimline`.claimline05,
  `pv_stgerafileclaimline`.claimline06,
  `pv_stgerafileclaimline`.claimline07,
  `pv_stgerafileclaimline`.claimlinesegment,
  `pv_stgerafileclaimline`.inserteddtm,
  `pv_stgerafileclaimline`.gs_id,
  `pv_stgerafileclaimline`.dwlastupdatedatetime
  FROM
    edwpsc.`pv_stgerafileclaimline`
;