CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_factclaimpayercpid
(
  claimpayercpidkey INT64 NOT NULL,
  claimkey INT64,
  iplankey INT64,
  cpid STRING,
  regionkey INT64,
  sourceaprimarykeyvalue STRING,
  sourcebprimarykeyvalue STRING,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (claimpayercpidkey) NOT ENFORCED
)
;