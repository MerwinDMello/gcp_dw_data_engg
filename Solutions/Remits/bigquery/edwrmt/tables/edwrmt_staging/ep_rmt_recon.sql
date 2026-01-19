CREATE TABLE IF NOT EXISTS {{ params.param_rmt_stage_dataset_name }}.ep_rmt_recon
(
  hcaremitdt DATE,
  remitprovnpi STRING,
  coid STRING,
  unitnum STRING,
  patientname STRING,
  claimpaymentamt NUMERIC(31, 2),
  patacctnum STRING,
  paymentplbamt NUMERIC(31, 2),
  paymentchknum STRING,
  paymentchkamt NUMERIC(31, 2),
  payerbatchlabel STRING,
  claimbilldt DATE,
  paymentchkdt DATE,
  payericn STRING,
  claimcontrolmsg STRING,
  iplan STRING,
  eppayernum STRING,
  overflow STRING
)
PARTITION BY hcaremitdt
CLUSTER BY unitnum, patacctnum;
