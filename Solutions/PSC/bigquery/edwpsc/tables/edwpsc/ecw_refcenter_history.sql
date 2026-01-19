CREATE TABLE IF NOT EXISTS edwpsc.ecw_refcenter_history
(
  centerkey INT64 NOT NULL,
  centerdescription STRING NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  sysstarttime DATETIME NOT NULL,
  sysendtime DATETIME NOT NULL
)
;