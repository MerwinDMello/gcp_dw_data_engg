CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspepayprod
(
  pspepayprdesc STRING,
  pspepayprfinclass STRING,
  pspepayprkey NUMERIC(29) NOT NULL,
  pspepayprname STRING,
  pspepayprpayid STRING,
  PRIMARY KEY (pspepayprkey) NOT ENFORCED
)
;