CREATE OR REPLACE VIEW edwpsc_views.`pv_refiplangroupfinancial`
AS SELECT
  `pv_refiplangroupfinancial`.iplangroupfinancialkey,
  `pv_refiplangroupfinancial`.iplangroupfinancialname,
  `pv_refiplangroupfinancial`.sourceprimarykeyvalue,
  `pv_refiplangroupfinancial`.sourcerecordlastupdated,
  `pv_refiplangroupfinancial`.dwlastupdatedatetime,
  `pv_refiplangroupfinancial`.sourcesystemcode,
  `pv_refiplangroupfinancial`.insertedby,
  `pv_refiplangroupfinancial`.inserteddtm,
  `pv_refiplangroupfinancial`.modifiedby,
  `pv_refiplangroupfinancial`.modifieddtm,
  `pv_refiplangroupfinancial`.deleteflag
  FROM
    edwpsc_base_views.`pv_refiplangroupfinancial`
;