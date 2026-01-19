CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stghctimeacct
(
  hctimekey STRING,
  hctimeid STRING,
  hctimeencntrid STRING,
  hctimeuser STRING,
  hctimepyr STRING,
  hctimedate STRING NOT NULL,
  hctimetime TIME,
  hctimestime TIME,
  hctimeetime TIME,
  hctimettime STRING,
  userid STRING,
  dwlastupdatedatetime DATETIME
)
;