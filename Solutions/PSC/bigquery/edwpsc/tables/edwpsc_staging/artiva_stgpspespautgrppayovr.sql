CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspespautgrppayovr
(
  pspespautgpovkey STRING NOT NULL,
  pspespautgpovpayid STRING,
  pspespautgpovspaid NUMERIC(29),
  PRIMARY KEY (pspespautgpovkey) NOT ENFORCED
)
;