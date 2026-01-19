CREATE OR REPLACE VIEW edwpsc_base_views.`pv_stgerafileclaim`
AS SELECT
  `pv_stgerafileclaim`.claim_id,
  `pv_stgerafileclaim`.payer_id,
  `pv_stgerafileclaim`.fileid,
  `pv_stgerafileclaim`.bpr_id,
  `pv_stgerafileclaim`.trn_id,
  `pv_stgerafileclaim`.claim01,
  `pv_stgerafileclaim`.claim02,
  `pv_stgerafileclaim`.claim03,
  `pv_stgerafileclaim`.claim04,
  `pv_stgerafileclaim`.claim05,
  `pv_stgerafileclaim`.claim06,
  `pv_stgerafileclaim`.claim07,
  `pv_stgerafileclaim`.claim08,
  `pv_stgerafileclaim`.claim09,
  `pv_stgerafileclaim`.claim10,
  `pv_stgerafileclaim`.claim11,
  `pv_stgerafileclaim`.claim12,
  `pv_stgerafileclaim`.claim13,
  `pv_stgerafileclaim`.claim14,
  `pv_stgerafileclaim`.claimsegment,
  `pv_stgerafileclaim`.inserteddtm,
  `pv_stgerafileclaim`.gs_id,
  `pv_stgerafileclaim`.dwlastupdatedatetime
  FROM
    edwpsc.`pv_stgerafileclaim`
;