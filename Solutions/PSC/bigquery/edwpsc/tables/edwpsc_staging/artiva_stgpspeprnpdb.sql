CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspeprnpdb
(
  pspeprnpentity STRING,
  pspeprnpgafid STRING,
  pspeprnpkey STRING NOT NULL,
  pspeprnpperfid STRING,
  pspeprnpqrytype STRING,
  pspeprnprcvdte DATETIME,
  pspeprnpsubdte DATETIME,
  PRIMARY KEY (pspeprnpkey) NOT ENFORCED
)
;