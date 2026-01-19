CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_experityclaimdata`
AS SELECT
  `ecw_experityclaimdata`.id,
  `ecw_experityclaimdata`.filename,
  `ecw_experityclaimdata`.claimnumber,
  `ecw_experityclaimdata`.claimamount,
  `ecw_experityclaimdata`.batchid,
  `ecw_experityclaimdata`.claimsource,
  `ecw_experityclaimdata`.appenddate,
  `ecw_experityclaimdata`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_experityclaimdata`
;