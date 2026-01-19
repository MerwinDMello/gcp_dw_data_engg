CREATE OR REPLACE VIEW edwpsc_views.`ecw_refietv2subcategoryrank`
AS SELECT
  `ecw_refietv2subcategoryrank`.subcategoryrankkey,
  `ecw_refietv2subcategoryrank`.subcategoryid,
  `ecw_refietv2subcategoryrank`.errortype,
  `ecw_refietv2subcategoryrank`.errortypedescription,
  `ecw_refietv2subcategoryrank`.subcategoryname,
  `ecw_refietv2subcategoryrank`.ietv2subcategoryrank,
  `ecw_refietv2subcategoryrank`.insertedby,
  `ecw_refietv2subcategoryrank`.inserteddtm,
  `ecw_refietv2subcategoryrank`.modifiedby,
  `ecw_refietv2subcategoryrank`.modifieddtm,
  `ecw_refietv2subcategoryrank`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_refietv2subcategoryrank`
;