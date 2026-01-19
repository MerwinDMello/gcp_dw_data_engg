CREATE OR REPLACE VIEW edwpsc_views.`ecw_refiplangroupfinancial_history`
AS SELECT
  `ecw_refiplangroupfinancial_history`.iplangroupfinancialkey,
  `ecw_refiplangroupfinancial_history`.iplangroupfinancialname,
  `ecw_refiplangroupfinancial_history`.sourceprimarykeyvalue,
  `ecw_refiplangroupfinancial_history`.sourcerecordlastupdated,
  `ecw_refiplangroupfinancial_history`.dwlastupdatedatetime,
  `ecw_refiplangroupfinancial_history`.sourcesystemcode,
  `ecw_refiplangroupfinancial_history`.insertedby,
  `ecw_refiplangroupfinancial_history`.inserteddtm,
  `ecw_refiplangroupfinancial_history`.modifiedby,
  `ecw_refiplangroupfinancial_history`.modifieddtm,
  `ecw_refiplangroupfinancial_history`.deleteflag,
  `ecw_refiplangroupfinancial_history`.sysstarttime,
  `ecw_refiplangroupfinancial_history`.sysendtime
  FROM
    edwpsc_base_views.`ecw_refiplangroupfinancial_history`
;