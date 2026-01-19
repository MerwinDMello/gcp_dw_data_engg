CREATE OR REPLACE VIEW edwpsc_views.`ecw_refrhstatusctgy`
AS SELECT
  `ecw_refrhstatusctgy`.statusctgycode,
  `ecw_refrhstatusctgy`.statusctgydesc
  FROM
    edwpsc_base_views.`ecw_refrhstatusctgy`
;