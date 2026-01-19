CREATE TABLE IF NOT EXISTS edwpsc.ecw_refcptcode
(
  cptcodekey INT64 NOT NULL,
  cptcode STRING,
  cptname STRING,
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
  cpttier1description STRING,
  cpttier2description STRING,
  cpttier3description STRING,
  cpttier4description STRING,
  cpttier5description STRING,
  PRIMARY KEY (cptcodekey) NOT ENFORCED
)
;