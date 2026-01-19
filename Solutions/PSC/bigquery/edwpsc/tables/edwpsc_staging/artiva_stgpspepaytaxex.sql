CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspepaytaxex
(
  pspepaytxkey NUMERIC(29) NOT NULL,
  pspepaytxpayid STRING,
  pspepaytxtax STRING,
  pspepaytxtaxop STRING,
  pspepaytxtyp STRING,
  PRIMARY KEY (pspepaytxkey) NOT ENFORCED
)
;