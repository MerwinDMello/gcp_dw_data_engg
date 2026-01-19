CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refcoidsmrymonth
(
  smrymonthkey INT64 NOT NULL,
  snapshotdate DATE,
  displayorder INT64,
  displayname STRING,
  linksnapshotdate DATE,
  linkdatetype STRING,
  dwlastupdatedatetime DATETIME
)
;