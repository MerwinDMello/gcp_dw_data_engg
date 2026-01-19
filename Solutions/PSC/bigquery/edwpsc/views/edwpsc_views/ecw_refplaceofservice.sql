CREATE OR REPLACE VIEW edwpsc_views.`ecw_refplaceofservice`
AS SELECT
  `ecw_refplaceofservice`.poskey,
  `ecw_refplaceofservice`.poscategorykey,
  `ecw_refplaceofservice`.posname,
  `ecw_refplaceofservice`.dwlastupdatedatetime,
  `ecw_refplaceofservice`.sourcesystemcode,
  `ecw_refplaceofservice`.insertedby,
  `ecw_refplaceofservice`.inserteddtm,
  `ecw_refplaceofservice`.modifiedby,
  `ecw_refplaceofservice`.modifieddtm,
  `ecw_refplaceofservice`.paymentrate
  FROM
    edwpsc_base_views.`ecw_refplaceofservice`
;