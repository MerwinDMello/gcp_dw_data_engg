CREATE TABLE IF NOT EXISTS edwpsc_staging.openconnect_factedrpsc
(
  openconnectedrkey INT64 NOT NULL,
  sendingapplication STRING,
  messageid STRING,
  actionid STRING,
  actionname STRING,
  errorcategory STRING,
  crosswalkerror STRING,
  crosswalkerrorrollup STRING,
  dateerrorreceived DATETIME,
  errorid STRING,
  errormessage STRING,
  artivaloaddate DATETIME,
  notes STRING,
  originalartivaloaddate DATETIME,
  routestepstatusreasonid STRING,
  routestepstatusreason STRING,
  routestepid STRING,
  routestepname STRING,
  psoceocid STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;