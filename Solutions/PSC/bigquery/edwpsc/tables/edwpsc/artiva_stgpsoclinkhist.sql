CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpsoclinkhist
(
  psoclhkey INT64,
  psoclhaction STRING,
  psoclhdate DATETIME,
  psoclhdesc STRING,
  psoclhlead STRING,
  psoclhlinkid INT64,
  psoclhmsgid INT64,
  psoclhtime DATETIME,
  psoclhuser STRING,
  dwlastupdatedatetime DATETIME
)
;