CREATE OR REPLACE VIEW edwpsc_views.`epic_refpbbillingstat`
AS SELECT
  `epic_refpbbillingstat`.pbbillingstatkey,
  `epic_refpbbillingstat`.pbbillingstatname,
  `epic_refpbbillingstat`.pbbillingstattitle,
  `epic_refpbbillingstat`.pbbillingstatabbr,
  `epic_refpbbillingstat`.pbbillingstatinternalid,
  `epic_refpbbillingstat`.pbbillingstatusc,
  `epic_refpbbillingstat`.regionkey,
  `epic_refpbbillingstat`.sourceaprimarykey,
  `epic_refpbbillingstat`.dwlastupdatedatetime,
  `epic_refpbbillingstat`.sourcesystemcode,
  `epic_refpbbillingstat`.insertedby,
  `epic_refpbbillingstat`.inserteddtm,
  `epic_refpbbillingstat`.modifiedby,
  `epic_refpbbillingstat`.modifieddtm
  FROM
    edwpsc_base_views.`epic_refpbbillingstat`
;