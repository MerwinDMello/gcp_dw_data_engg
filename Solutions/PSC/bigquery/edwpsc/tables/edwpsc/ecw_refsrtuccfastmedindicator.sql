CREATE TABLE IF NOT EXISTS edwpsc.ecw_refsrtuccfastmedindicator
(
  uccfastmedindicatorkey INT64 NOT NULL,
  id INT64,
  practice STRING,
  clinic STRING,
  fastmedindicator STRING,
  effectivedate DATE,
  termeddate DATE,
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