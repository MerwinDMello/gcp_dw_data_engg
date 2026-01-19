CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_refvirtualvisit
(
  virtualvisittype INT64 NOT NULL,
  virtualvisitdesc STRING NOT NULL,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;