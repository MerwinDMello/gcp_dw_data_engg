CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_refservicearea
(
  serviceareakey INT64 NOT NULL,
  serviceareaname STRING,
  serviceareaabbr STRING,
  serviceareatype STRING,
  serviceareagroup STRING,
  serviceareaglprefix STRING,
  servareaid STRING,
  regionkey INT64,
  sourceaprimarykey STRING,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (serviceareakey) NOT ENFORCED
)
;