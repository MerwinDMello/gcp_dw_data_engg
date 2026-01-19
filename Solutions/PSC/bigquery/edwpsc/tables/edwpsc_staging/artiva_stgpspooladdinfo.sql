CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspooladdinfo
(
  pool STRING NOT NULL,
  pspooldept STRING,
  pspooltype STRING,
  poolsubgroup STRING,
  poolfunction STRING,
  poolsla INT64,
  pspoolvendor STRING,
  pspoolitmprhr NUMERIC(31, 2),
  pspoolvndesc STRING,
  pspoollob STRING,
  pspoolmanager STRING,
  pspoolpaygrp STRING,
  pspoolresdept STRING,
  pspooltier STRING,
  pspoolescpoolflg STRING,
  pspoolconctrlnum STRING,
  pspooleffectdte DATETIME,
  pspoollaborstdname STRING,
  PRIMARY KEY (pool) NOT ENFORCED
)
;