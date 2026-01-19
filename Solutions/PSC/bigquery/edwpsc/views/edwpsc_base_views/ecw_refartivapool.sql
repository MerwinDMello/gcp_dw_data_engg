CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refartivapool`
AS SELECT
  `ecw_refartivapool`.artivapoolkey,
  `ecw_refartivapool`.artivapooldesc
  FROM
    edwpsc.`ecw_refartivapool`
;