CREATE OR REPLACE VIEW edwpsc_base_views.`pv_juncclaimpayerfirstlastkeymeasure`
AS SELECT
  `pv_juncclaimpayerfirstlastkeymeasure`.claimpayerkey,
  `pv_juncclaimpayerfirstlastkeymeasure`.claimkey,
  `pv_juncclaimpayerfirstlastkeymeasure`.coid,
  `pv_juncclaimpayerfirstlastkeymeasure`.payerfirstbillkey,
  `pv_juncclaimpayerfirstlastkeymeasure`.payerfirstbilldatekey,
  `pv_juncclaimpayerfirstlastkeymeasure`.payerlastbillkey,
  `pv_juncclaimpayerfirstlastkeymeasure`.payerlastbilldatekey,
  `pv_juncclaimpayerfirstlastkeymeasure`.payernumberofbills,
  `pv_juncclaimpayerfirstlastkeymeasure`.payerfirstclaimpaymentkey,
  `pv_juncclaimpayerfirstlastkeymeasure`.payerfirstclaimpaymentdatekey,
  `pv_juncclaimpayerfirstlastkeymeasure`.payerlastclaimpaymentkey,
  `pv_juncclaimpayerfirstlastkeymeasure`.payerlastclaimpaymentdatekey,
  `pv_juncclaimpayerfirstlastkeymeasure`.payertotalpayments,
  `pv_juncclaimpayerfirstlastkeymeasure`.dwlastupdatedatetime,
  `pv_juncclaimpayerfirstlastkeymeasure`.sourcesystemcode,
  `pv_juncclaimpayerfirstlastkeymeasure`.insertedby,
  `pv_juncclaimpayerfirstlastkeymeasure`.inserteddtm,
  `pv_juncclaimpayerfirstlastkeymeasure`.modifiedby,
  `pv_juncclaimpayerfirstlastkeymeasure`.modifieddtm
  FROM
    edwpsc.`pv_juncclaimpayerfirstlastkeymeasure`
;