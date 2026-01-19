CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_refcptcode
(
  cptcodekey INT64 NOT NULL,
  cptcode STRING NOT NULL,
  cptname STRING NOT NULL,
  sourceaprimarykeyvalue INT64,
  sourcearecordlastupdated DATETIME,
  sourcebprimarykeyvalue INT64,
  sourcebrecordlastupdated DATETIME,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  deleteflag INT64,
  category1code STRING,
  category2code STRING,
  PRIMARY KEY (cptcodekey) NOT ENFORCED
)
;