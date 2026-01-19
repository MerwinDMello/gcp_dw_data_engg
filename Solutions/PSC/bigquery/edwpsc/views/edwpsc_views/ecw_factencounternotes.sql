CREATE OR REPLACE VIEW edwpsc_views.`ecw_factencounternotes`
AS SELECT
  `ecw_factencounternotes`.encounternoteskey,
  `ecw_factencounternotes`.regionkey,
  `ecw_factencounternotes`.encounterkey,
  `ecw_factencounternotes`.encounterid,
  `ecw_factencounternotes`.encounternotetype,
  `ecw_factencounternotes`.encounternote,
  `ecw_factencounternotes`.sourceprimarykeyvalue,
  `ecw_factencounternotes`.sourcerecordlastupdated,
  `ecw_factencounternotes`.dwlastupdatedatetime,
  `ecw_factencounternotes`.sourcesystemcode,
  `ecw_factencounternotes`.insertedby,
  `ecw_factencounternotes`.inserteddtm,
  `ecw_factencounternotes`.modifiedby,
  `ecw_factencounternotes`.modifieddtm,
  `ecw_factencounternotes`.archivedrecord
  FROM
    edwpsc_base_views.`ecw_factencounternotes`
;