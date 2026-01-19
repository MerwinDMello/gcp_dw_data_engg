CREATE OR REPLACE VIEW edwpsc_views.`ecw_refartivapool`
AS SELECT
  `ecw_refartivapool`.artivapoolkey,
  `ecw_refartivapool`.artivapooldesc
  FROM
    edwpsc_base_views.`ecw_refartivapool`
;