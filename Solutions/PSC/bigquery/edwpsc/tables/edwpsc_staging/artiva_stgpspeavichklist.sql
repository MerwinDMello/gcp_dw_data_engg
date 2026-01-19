CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspeavichklist
(
  pspeavichkaviid STRING,
  pspeavichkcomp STRING,
  pspeavichkcompdte DATETIME,
  pspeavichkgafid STRING,
  pspeavichkkey NUMERIC(29) NOT NULL,
  pspeavichkorigqkey STRING,
  pspeavichkperfid STRING,
  pspeavichkresponse STRING,
  pspeavichktask STRING,
  PRIMARY KEY (pspeavichkkey) NOT ENFORCED
)
;