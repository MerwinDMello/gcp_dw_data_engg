CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspeprspecialty
(
  pspeprspactiveind STRING,
  pspeprspgafid STRING,
  pspeprspkey STRING NOT NULL,
  pspeprspname STRING,
  pspeprspperfid STRING,
  pspeprspstatus STRING,
  pspeprsptaxonomy STRING,
  pspeprsptype STRING,
  PRIMARY KEY (pspeprspkey) NOT ENFORCED
)
;