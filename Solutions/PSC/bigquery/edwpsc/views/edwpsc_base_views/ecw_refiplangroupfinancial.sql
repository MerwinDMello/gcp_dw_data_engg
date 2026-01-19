CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refiplangroupfinancial`
AS SELECT
  `ecw_refiplangroupfinancial`.iplangroupfinancialkey,
  `ecw_refiplangroupfinancial`.iplangroupfinancialname,
  `ecw_refiplangroupfinancial`.sourceprimarykeyvalue,
  `ecw_refiplangroupfinancial`.sourcerecordlastupdated,
  `ecw_refiplangroupfinancial`.dwlastupdatedatetime,
  `ecw_refiplangroupfinancial`.sourcesystemcode,
  `ecw_refiplangroupfinancial`.insertedby,
  `ecw_refiplangroupfinancial`.inserteddtm,
  `ecw_refiplangroupfinancial`.modifiedby,
  `ecw_refiplangroupfinancial`.modifieddtm,
  `ecw_refiplangroupfinancial`.deleteflag,
  `ecw_refiplangroupfinancial`.sysstarttime,
  `ecw_refiplangroupfinancial`.sysendtime
  FROM
    edwpsc.`ecw_refiplangroupfinancial`
;