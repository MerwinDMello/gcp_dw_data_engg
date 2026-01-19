CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspeperfrelgrp
(
  psperelgrpid STRING,
  psperelgrpkey NUMERIC(29) NOT NULL,
  psperelgrpperfid STRING,
  PRIMARY KEY (psperelgrpkey) NOT ENFORCED
)
;