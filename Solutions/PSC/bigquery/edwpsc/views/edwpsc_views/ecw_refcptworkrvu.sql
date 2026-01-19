CREATE OR REPLACE VIEW edwpsc_views.`ecw_refcptworkrvu`
AS SELECT
  `ecw_refcptworkrvu`.cptrvukey,
  `ecw_refcptworkrvu`.cptcode,
  `ecw_refcptworkrvu`.year,
  `ecw_refcptworkrvu`.cptmodifier,
  `ecw_refcptworkrvu`.cptdescription,
  `ecw_refcptworkrvu`.workrvustat,
  `ecw_refcptworkrvu`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_refcptworkrvu`
;