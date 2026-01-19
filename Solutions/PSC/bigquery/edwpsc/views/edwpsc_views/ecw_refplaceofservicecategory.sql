CREATE OR REPLACE VIEW edwpsc_views.`ecw_refplaceofservicecategory`
AS SELECT
  `ecw_refplaceofservicecategory`.poscategorykey,
  `ecw_refplaceofservicecategory`.poscategoryname,
  `ecw_refplaceofservicecategory`.dwlastupdatedatetime,
  `ecw_refplaceofservicecategory`.sourcesystemcode,
  `ecw_refplaceofservicecategory`.insertedby,
  `ecw_refplaceofservicecategory`.inserteddtm,
  `ecw_refplaceofservicecategory`.modifiedby,
  `ecw_refplaceofservicecategory`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_refplaceofservicecategory`
;