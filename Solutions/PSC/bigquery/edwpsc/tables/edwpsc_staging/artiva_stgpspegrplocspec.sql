CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspegrplocspec
(
  pspeglsglid STRING,
  pspeglskey STRING NOT NULL,
  pspeglsspecname STRING,
  pspeglstaxonomy STRING,
  PRIMARY KEY (pspeglskey) NOT ENFORCED
)
;