CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpscdrevrequests
(
  pscdracctid INT64,
  pscdrcrtdte DATETIME,
  pscdrcrttime TIME,
  pscdrcrtuser STRING,
  pscdrencntrid INT64,
  pscdrkey INT64,
  pscdrstatus STRING,
  pscdrutilityid INT64
)
;