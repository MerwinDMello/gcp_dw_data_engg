CREATE OR REPLACE VIEW edwpsc_views.`epic_refiplanfinancialgroupcrosswalk`
AS SELECT
  `epic_refiplanfinancialgroupcrosswalk`.payor_name,
  `epic_refiplanfinancialgroupcrosswalk`.iplangroupfinancialname
  FROM
    edwpsc_base_views.`epic_refiplanfinancialgroupcrosswalk`
;