CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_refpos
(
  poskey INT64 NOT NULL,
  posname STRING,
  posabbr STRING,
  posglprefix STRING,
  posservareaid STRING,
  serviceareakey INT64,
  posaddr1 STRING,
  posaddr2 STRING,
  poscity STRING,
  posstate STRING,
  poszip STRING,
  postype STRING,
  poscode STRING,
  deleteflag INT64,
  posid STRING,
  regionkey INT64,
  sourceaprimarykey STRING,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  postypec INT64,
  PRIMARY KEY (poskey) NOT ENFORCED
)
;