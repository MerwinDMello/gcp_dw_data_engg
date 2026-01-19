CREATE TABLE IF NOT EXISTS edwpsc.`3m_factuseractivity`
(
  useractivitykey INT64 NOT NULL,
  cacaccountnumber STRING,
  visitnumber STRING,
  facilityid STRING,
  activitydate DATETIME,
  holdreason STRING,
  codingstatus STRING,
  username STRING,
  userid STRING,
  comment STRING,
  worklistname STRING,
  filename STRING,
  filedate STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;