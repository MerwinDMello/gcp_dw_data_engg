CREATE OR REPLACE VIEW edwpsc_views.`epic_factbilldeniedcharge`
AS SELECT
  `epic_factbilldeniedcharge`.sourcepaymenttransactionid,
  `epic_factbilldeniedcharge`.billdeniedchargereceiveddate,
  `epic_factbilldeniedcharge`.billdeniedchargeamount,
  `epic_factbilldeniedcharge`.billdeniedchargeid,
  `epic_factbilldeniedcharge`.billdeniedchargename,
  `epic_factbilldeniedcharge`.invoicenumber,
  `epic_factbilldeniedcharge`.denialcode,
  `epic_factbilldeniedcharge`.denialcodedesc,
  `epic_factbilldeniedcharge`.denialcoderanking,
  `epic_factbilldeniedcharge`.regionkey,
  `epic_factbilldeniedcharge`.sourceaprimarykeyvalue,
  `epic_factbilldeniedcharge`.dwlastupdatedatetime,
  `epic_factbilldeniedcharge`.sourcesystemcode,
  `epic_factbilldeniedcharge`.insertedby,
  `epic_factbilldeniedcharge`.inserteddtm,
  `epic_factbilldeniedcharge`.modifiedby,
  `epic_factbilldeniedcharge`.modifieddtm,
  `epic_factbilldeniedcharge`.billdeniedchargekey
  FROM
    edwpsc_base_views.`epic_factbilldeniedcharge`
;