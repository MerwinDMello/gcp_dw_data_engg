CREATE OR REPLACE VIEW edwpsc_base_views.`epic_juncclaimpayerfirstlastkeymeasure`
AS SELECT
  `epic_juncclaimpayerfirstlastkeymeasure`.claimpayerkey,
  `epic_juncclaimpayerfirstlastkeymeasure`.claimkey,
  `epic_juncclaimpayerfirstlastkeymeasure`.coid,
  `epic_juncclaimpayerfirstlastkeymeasure`.payerfirstbillkey,
  `epic_juncclaimpayerfirstlastkeymeasure`.payerfirstbilldatekey,
  `epic_juncclaimpayerfirstlastkeymeasure`.payerlastbillkey,
  `epic_juncclaimpayerfirstlastkeymeasure`.payerlastbilldatekey,
  `epic_juncclaimpayerfirstlastkeymeasure`.payernumberofbills,
  `epic_juncclaimpayerfirstlastkeymeasure`.payerfirstclaimpaymentkey,
  `epic_juncclaimpayerfirstlastkeymeasure`.payerfirstclaimpaymentdatekey,
  `epic_juncclaimpayerfirstlastkeymeasure`.payerlastclaimpaymentkey,
  `epic_juncclaimpayerfirstlastkeymeasure`.payerlastclaimpaymentdatekey,
  `epic_juncclaimpayerfirstlastkeymeasure`.payertotalpayments,
  `epic_juncclaimpayerfirstlastkeymeasure`.dwlastupdatedatetime,
  `epic_juncclaimpayerfirstlastkeymeasure`.sourcesystemcode,
  `epic_juncclaimpayerfirstlastkeymeasure`.insertedby,
  `epic_juncclaimpayerfirstlastkeymeasure`.inserteddtm,
  `epic_juncclaimpayerfirstlastkeymeasure`.modifiedby,
  `epic_juncclaimpayerfirstlastkeymeasure`.modifieddtm
  FROM
    edwpsc.`epic_juncclaimpayerfirstlastkeymeasure`
;