CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspeavi
(
  pspeaviapprecvdte DATETIME,
  pspeaviappsentdte DATETIME,
  pspeaviattestdte DATETIME,
  pspeavicrcompdte DATETIME,
  pspeavicredcomp STRING,
  pspeavicrgrpkey STRING,
  pspeavicrstartdte DATETIME,
  pspeavicrstasofdte DATETIME,
  pspeavicrstatus STRING,
  pspeavidesc STRING,
  pspeavientdesc STRING,
  pspeavifacid STRING,
  pspeavigafid STRING,
  pspeavikey NUMERIC(29) NOT NULL,
  pspeaviorigdate DATETIME,
  pspeaviperfid STRING,
  pspeavirfccompdte DATETIME,
  pspeavistchgdte DATETIME,
  pspeavitaxid STRING,
  pspeavitype STRING,
  PRIMARY KEY (pspeavikey) NOT ENFORCED
)
;