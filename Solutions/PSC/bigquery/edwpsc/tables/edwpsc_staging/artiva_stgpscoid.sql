CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpscoid
(
  pscoidkey STRING NOT NULL,
  pscoidsiteid STRING,
  pscoidcompany STRING,
  pscoiddivision STRING,
  pscoidgroup STRING,
  pscoidmarket STRING,
  pscoidnumber STRING,
  pscoidregion STRING,
  pscoidlocationcode STRING,
  pscoiddeptcode STRING,
  pscoidpractice STRING,
  pscoidprovider34 STRING,
  pscoidproviderid STRING,
  pscoidprovtrmdte STRING,
  pscoidprovtrmtype STRING,
  pscoidpracclsdte STRING,
  pscoidpracclstype STRING,
  pscoidietreroute STRING,
  pscoidietsrcedept STRING,
  PRIMARY KEY (pscoidkey) NOT ENFORCED
)
;