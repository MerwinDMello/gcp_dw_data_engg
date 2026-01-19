CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspeecmsgs
(
  pspeecmsgcnt STRING,
  pspeecmsgdate DATETIME,
  pspeecmsgectid STRING,
  pspeecmsgeditor STRING,
  pspeecmsgexportdte DATETIME,
  pspeecmsgid NUMERIC(29) NOT NULL,
  pspeecmsgnew STRING,
  pspeecmsgportalid STRING,
  pspeecmsgsource STRING,
  pspeecmsgtext STRING,
  pspeecmsgtime DATETIME,
  pspeecmsgtype STRING,
  pspeecmsguser STRING,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (pspeecmsgid) NOT ENFORCED
)
;