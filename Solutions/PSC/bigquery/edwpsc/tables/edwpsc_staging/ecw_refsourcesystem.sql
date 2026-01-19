CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refsourcesystem
(
  sourcesystemkey INT64 NOT NULL,
  sourcesystemcode STRING NOT NULL,
  sourcesystemshortname STRING,
  sourcesystemlongname STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;