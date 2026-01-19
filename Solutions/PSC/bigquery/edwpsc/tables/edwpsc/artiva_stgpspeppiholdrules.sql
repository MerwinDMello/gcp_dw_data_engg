CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspeppiholdrules
(
  pspeppihrkey NUMERIC(29) NOT NULL,
  pspeppihrppiid STRING,
  pspeppihruid NUMERIC(29),
  PRIMARY KEY (pspeppihrkey) NOT ENFORCED
)
;