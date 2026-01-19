CREATE TABLE IF NOT EXISTS edwpsc.epic_refiplangroupfinancial
(
  iplangroupfinancialkey INT64 NOT NULL,
  iplangroupfinancialname STRING NOT NULL,
  sourceprimarykeyvalue STRING NOT NULL,
  sourcerecordlastupdated DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  deleteflag INT64,
  PRIMARY KEY (iplangroupfinancialkey) NOT ENFORCED
)
;