CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspeprlicense
(
  pspeprliawarddte STRING,
  pspeprlienrollstat STRING,
  pspeprlienstasdte DATETIME,
  pspeprliexpdte DATETIME,
  pspeprligafid STRING,
  pspeprliinid STRING,
  pspeprlikey STRING NOT NULL,
  pspeprlinumber STRING,
  pspeprliperfid STRING,
  pspeprlipubstasdte DATETIME,
  pspeprlipubstat STRING,
  pspeprlipubstliteral STRING,
  pspeprlistate STRING,
  pspeprlistatus STRING,
  pspeprlitype STRING,
  PRIMARY KEY (pspeprlikey) NOT ENFORCED
)
;