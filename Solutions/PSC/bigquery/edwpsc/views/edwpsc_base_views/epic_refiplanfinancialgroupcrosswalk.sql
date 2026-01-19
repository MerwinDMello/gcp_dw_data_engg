CREATE OR REPLACE VIEW edwpsc_base_views.`epic_refiplanfinancialgroupcrosswalk`
AS SELECT
  `epic_refiplanfinancialgroupcrosswalk`.payor_name,
  `epic_refiplanfinancialgroupcrosswalk`.iplangroupfinancialname
  FROM
    edwpsc.`epic_refiplanfinancialgroupcrosswalk`
;