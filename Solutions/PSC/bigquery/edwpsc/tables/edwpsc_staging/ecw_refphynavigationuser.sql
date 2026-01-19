CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refphynavigationuser
(
  refphynavigationuserkey INT64 NOT NULL,
  user_id STRING,
  lastname STRING,
  firstname STRING,
  role STRING,
  defaultaccess STRING,
  security_filter STRING,
  device STRING,
  PRIMARY KEY (refphynavigationuserkey) NOT ENFORCED
)
;