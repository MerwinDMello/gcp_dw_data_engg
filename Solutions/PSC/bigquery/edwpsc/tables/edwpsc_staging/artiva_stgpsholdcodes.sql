CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpsholdcodes
(
  pshcdesc STRING,
  pshcid STRING NOT NULL,
  pshcmtype STRING,
  pshcpriority NUMERIC(29),
  pshctype STRING,
  pshcriskreport STRING,
  PRIMARY KEY (pshcid) NOT ENFORCED
)
;