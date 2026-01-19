CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_juncenterprisepermissionsbyresource
(
  ecwenterprisepermissionsbyresourcekey INT64 NOT NULL,
  regionkey INT64 NOT NULL,
  resourcetype STRING,
  resourceid INT64 NOT NULL,
  enterpriseid INT64 NOT NULL,
  sourceprimarykeyvalue INT64,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;