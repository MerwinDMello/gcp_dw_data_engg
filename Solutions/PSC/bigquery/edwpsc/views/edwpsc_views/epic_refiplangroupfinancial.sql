CREATE OR REPLACE VIEW edwpsc_views.`epic_refiplangroupfinancial`
AS SELECT
  `epic_refiplangroupfinancial`.iplangroupfinancialkey,
  `epic_refiplangroupfinancial`.iplangroupfinancialname,
  `epic_refiplangroupfinancial`.sourceprimarykeyvalue,
  `epic_refiplangroupfinancial`.sourcerecordlastupdated,
  `epic_refiplangroupfinancial`.dwlastupdatedatetime,
  `epic_refiplangroupfinancial`.sourcesystemcode,
  `epic_refiplangroupfinancial`.insertedby,
  `epic_refiplangroupfinancial`.inserteddtm,
  `epic_refiplangroupfinancial`.modifiedby,
  `epic_refiplangroupfinancial`.modifieddtm,
  `epic_refiplangroupfinancial`.deleteflag
  FROM
    edwpsc_base_views.`epic_refiplangroupfinancial`
;