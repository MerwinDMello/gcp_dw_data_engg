CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncclaimpayerfirstlastkeymeasure`
AS SELECT
  `ecw_juncclaimpayerfirstlastkeymeasure`.claimpayerkey,
  `ecw_juncclaimpayerfirstlastkeymeasure`.claimkey,
  `ecw_juncclaimpayerfirstlastkeymeasure`.coid,
  `ecw_juncclaimpayerfirstlastkeymeasure`.payerfirstbillkey,
  `ecw_juncclaimpayerfirstlastkeymeasure`.payerfirstbilldatekey,
  `ecw_juncclaimpayerfirstlastkeymeasure`.payerlastbillkey,
  `ecw_juncclaimpayerfirstlastkeymeasure`.payerlastbilldatekey,
  `ecw_juncclaimpayerfirstlastkeymeasure`.payernumberofbills,
  `ecw_juncclaimpayerfirstlastkeymeasure`.payerfirstclaimpaymentkey,
  `ecw_juncclaimpayerfirstlastkeymeasure`.payerfirstclaimpaymentdatekey,
  `ecw_juncclaimpayerfirstlastkeymeasure`.payerlastclaimpaymentkey,
  `ecw_juncclaimpayerfirstlastkeymeasure`.payerlastclaimpaymentdatekey,
  `ecw_juncclaimpayerfirstlastkeymeasure`.payertotalpayments,
  `ecw_juncclaimpayerfirstlastkeymeasure`.dwlastupdatedatetime,
  `ecw_juncclaimpayerfirstlastkeymeasure`.sourcesystemcode,
  `ecw_juncclaimpayerfirstlastkeymeasure`.insertedby,
  `ecw_juncclaimpayerfirstlastkeymeasure`.inserteddtm,
  `ecw_juncclaimpayerfirstlastkeymeasure`.modifiedby,
  `ecw_juncclaimpayerfirstlastkeymeasure`.modifieddtm,
  `ecw_juncclaimpayerfirstlastkeymeasure`.archivedrecord
  FROM
    edwpsc.`ecw_juncclaimpayerfirstlastkeymeasure`
;