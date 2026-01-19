CREATE OR REPLACE VIEW edwpsc_views.`epic_refaccountcrosswalk`
AS SELECT
  `epic_refaccountcrosswalk`.accountkey,
  `epic_refaccountcrosswalk`.accountid,
  `epic_refaccountcrosswalk`.accountinternalid,
  `epic_refaccountcrosswalk`.regionkey,
  `epic_refaccountcrosswalk`.insertedby,
  `epic_refaccountcrosswalk`.inserteddtm,
  `epic_refaccountcrosswalk`.modifiedby,
  `epic_refaccountcrosswalk`.modifieddtm,
  `epic_refaccountcrosswalk`.accountname,
  `epic_refaccountcrosswalk`.accountzcname,
  `epic_refaccountcrosswalk`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`epic_refaccountcrosswalk`
;