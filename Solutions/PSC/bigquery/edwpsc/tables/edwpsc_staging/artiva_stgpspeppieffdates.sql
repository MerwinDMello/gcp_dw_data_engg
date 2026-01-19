CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspeppieffdates
(
  pspeppieffdte DATETIME,
  pspeppieffkey NUMERIC(29) NOT NULL,
  pspeppieffppiid NUMERIC(29),
  pspeppieffprodid NUMERIC(29),
  pspeppieffprovlocid STRING,
  PRIMARY KEY (pspeppieffkey) NOT ENFORCED
)
;