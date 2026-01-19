CREATE OR REPLACE VIEW {{ params.param_rmt_views_dataset_name }}.ep_rmt_recon
AS SELECT
    ep_rmt_recon.hcaremitdt,
    ep_rmt_recon.remitprovnpi,
    ep_rmt_recon.coid,
    ep_rmt_recon.unitnum,
    ep_rmt_recon.patientname,
    ep_rmt_recon.claimpaymentamt,
    ep_rmt_recon.patacctnum,
    ep_rmt_recon.paymentplbamt,
    ep_rmt_recon.paymentchknum,
    ep_rmt_recon.paymentchkamt,
    ep_rmt_recon.payerbatchlabel,
    ep_rmt_recon.claimbilldt,
    ep_rmt_recon.paymentchkdt,
    ep_rmt_recon.payericn,
    ep_rmt_recon.claimcontrolmsg,
    ep_rmt_recon.iplan,
    ep_rmt_recon.eppayernum,
    ep_rmt_recon.overflow
  FROM
    {{ params.param_rmt_base_views_dataset_name }}.ep_rmt_recon
;
