CREATE TABLE IF NOT EXISTS edwpsc.ecw_refsrtreportingdefinitionsandsource
(
  srtreportingdefinitionsandsoucekey INT64 NOT NULL,
  metric STRING,
  metricdefinition STRING,
  criteria STRING,
  report STRING,
  sort INT64,
  sourceaprimarykeyvalue INT64,
  deleteflag INT64 NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME NOT NULL,
  modifiedby STRING,
  modifieddtm DATETIME
)
;