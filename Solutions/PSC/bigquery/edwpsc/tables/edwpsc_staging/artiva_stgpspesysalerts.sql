CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspesysalerts
(
  pspesysalractdte DATETIME,
  pspesysalractive STRING,
  pspesysalrcrtdte DATETIME,
  pspesysalrcrttim DATETIME,
  pspesysalrcrtuser STRING,
  pspesysalrexpdte DATETIME,
  pspesysalrgrpaviflg STRING,
  pspesysalrgrpid STRING,
  pspesysalrkey NUMERIC(29) NOT NULL,
  pspesysalrprovid STRING,
  pspesysalrsdesc STRING,
  pspesysalrtext STRING,
  PRIMARY KEY (pspesysalrkey) NOT ENFORCED
)
;