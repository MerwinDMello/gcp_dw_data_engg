CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspespatmppayovr
(
  pspespayovrkey STRING NOT NULL,
  pspespayovrlocflg STRING,
  pspespayovrperfid STRING,
  pspespayovrpid STRING,
  PRIMARY KEY (pspespayovrkey) NOT ENFORCED
)
;