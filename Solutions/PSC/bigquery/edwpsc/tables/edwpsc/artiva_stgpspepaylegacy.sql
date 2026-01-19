CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspepaylegacy
(
  pspepaylpkey NUMERIC(29) NOT NULL,
  pspepaylppayid STRING,
  pspepaylprefpayid STRING,
  PRIMARY KEY (pspepaylpkey) NOT ENFORCED
)
;