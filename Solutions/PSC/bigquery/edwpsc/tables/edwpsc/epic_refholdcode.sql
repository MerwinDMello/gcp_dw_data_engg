CREATE TABLE IF NOT EXISTS edwpsc.epic_refholdcode
(
  holdcodekey INT64 NOT NULL,
  holdcode INT64,
  holdcodename STRING,
  holdcodetype STRING,
  deleteflag INT64,
  holdcodedescription STRING,
  regionkey INT64,
  sourceaprimarykeyvalue STRING,
  sourcearecordlastupdated DATETIME,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (holdcodekey) NOT ENFORCED
)
;