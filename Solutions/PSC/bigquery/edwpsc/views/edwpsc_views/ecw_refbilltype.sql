CREATE OR REPLACE VIEW edwpsc_views.`ecw_refbilltype`
AS SELECT
  `ecw_refbilltype`.billtypekey,
  `ecw_refbilltype`.billtypename,
  `ecw_refbilltype`.dwlastupdatedatetime,
  `ecw_refbilltype`.sourcesystemcode,
  `ecw_refbilltype`.insertedby,
  `ecw_refbilltype`.inserteddtm,
  `ecw_refbilltype`.modifiedby,
  `ecw_refbilltype`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_refbilltype`
;