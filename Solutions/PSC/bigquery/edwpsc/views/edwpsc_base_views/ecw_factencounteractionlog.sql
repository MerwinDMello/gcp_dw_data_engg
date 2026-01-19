CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_factencounteractionlog`
AS SELECT
  `ecw_factencounteractionlog`.encounteractionlogid,
  `ecw_factencounteractionlog`.encounterkey,
  `ecw_factencounteractionlog`.encounteractionlogtypekey,
  `ecw_factencounteractionlog`.encounteractionlognote,
  `ecw_factencounteractionlog`.encounteractionlogdatekey,
  `ecw_factencounteractionlog`.encounteractionlogtime,
  `ecw_factencounteractionlog`.encounteractionlogcreatedbyuserkey,
  `ecw_factencounteractionlog`.regionkey,
  `ecw_factencounteractionlog`.sourceprimarykeyvalue,
  `ecw_factencounteractionlog`.dwlastupdatedatetime,
  `ecw_factencounteractionlog`.sourcesystemcode,
  `ecw_factencounteractionlog`.insertedby,
  `ecw_factencounteractionlog`.inserteddtm,
  `ecw_factencounteractionlog`.modifiedby,
  `ecw_factencounteractionlog`.modifieddtm,
  `ecw_factencounteractionlog`.archivedrecord
  FROM
    edwpsc.`ecw_factencounteractionlog`
;