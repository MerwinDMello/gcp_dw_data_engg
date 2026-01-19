CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpsoctransactions
(
  psoctrkey INT64,
  psoctrcat STRING,
  psoctrdate DATETIME,
  psoctrloaddte DATETIME,
  psoctrmod1 STRING,
  psoctrmod2 STRING,
  psoctrmod3 STRING,
  psoctrmod4 STRING,
  psoctrocid INT64,
  psoctrorigdte DATETIME,
  psoctrproccd STRING,
  psoctrproccdid STRING,
  dwlastupdatedatetime DATETIME
)
;