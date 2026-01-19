CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_cashvaluecurrentclaim`
AS SELECT
  `ecw_cashvaluecurrentclaim`.claimkey,
  `ecw_cashvaluecurrentclaim`.claimnumber,
  `ecw_cashvaluecurrentclaim`.regionkey,
  `ecw_cashvaluecurrentclaim`.claimbalancecashvalueamt,
  `ecw_cashvaluecurrentclaim`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_cashvaluecurrentclaim`
;