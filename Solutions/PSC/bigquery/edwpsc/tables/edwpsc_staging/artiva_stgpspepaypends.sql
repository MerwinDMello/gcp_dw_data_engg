CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspepaypends
(
  pspepaypdkey NUMERIC(29) NOT NULL,
  pspepaypdpayid STRING,
  pspepaypdrefpayid STRING,
  PRIMARY KEY (pspepaypdkey) NOT ENFORCED
)
;