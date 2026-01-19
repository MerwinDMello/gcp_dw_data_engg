CREATE OR REPLACE VIEW edwpsc_views.`pv_factclaimpaymentcreditcardtransaction`
AS SELECT
  `pv_factclaimpaymentcreditcardtransaction`.claimkey,
  `pv_factclaimpaymentcreditcardtransaction`.logdetailpk,
  `pv_factclaimpaymentcreditcardtransaction`.creditcardtype,
  `pv_factclaimpaymentcreditcardtransaction`.ccsaletype,
  `pv_factclaimpaymentcreditcardtransaction`.createdcarddate,
  `pv_factclaimpaymentcreditcardtransaction`.reserveamt,
  `pv_factclaimpaymentcreditcardtransaction`.reserveamtused,
  `pv_factclaimpaymentcreditcardtransaction`.reserveamtremaining,
  `pv_factclaimpaymentcreditcardtransaction`.deletedflag,
  `pv_factclaimpaymentcreditcardtransaction`.declinedorcancelledflag,
  `pv_factclaimpaymentcreditcardtransaction`.declinedamt,
  `pv_factclaimpaymentcreditcardtransaction`.regionkey,
  `pv_factclaimpaymentcreditcardtransaction`.dwlastupdatedatetime,
  `pv_factclaimpaymentcreditcardtransaction`.sourcesystemcode,
  `pv_factclaimpaymentcreditcardtransaction`.insertedby,
  `pv_factclaimpaymentcreditcardtransaction`.inserteddtm,
  `pv_factclaimpaymentcreditcardtransaction`.modifiedby,
  `pv_factclaimpaymentcreditcardtransaction`.modifieddtm,
  `pv_factclaimpaymentcreditcardtransaction`.loadkey
  FROM
    edwpsc_base_views.`pv_factclaimpaymentcreditcardtransaction`
;