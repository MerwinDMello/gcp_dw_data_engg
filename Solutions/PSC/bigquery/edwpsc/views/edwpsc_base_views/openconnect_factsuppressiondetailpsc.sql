CREATE OR REPLACE VIEW edwpsc_base_views.`openconnect_factsuppressiondetailpsc`
AS SELECT
  `openconnect_factsuppressiondetailpsc`.openconnectsuppressiondetailkey,
  `openconnect_factsuppressiondetailpsc`.messagecreateddate,
  `openconnect_factsuppressiondetailpsc`.routestep,
  `openconnect_factsuppressiondetailpsc`.statusreason,
  `openconnect_factsuppressiondetailpsc`.statusmessage,
  `openconnect_factsuppressiondetailpsc`.deleteflag,
  `openconnect_factsuppressiondetailpsc`.dwlastupdateddate,
  `openconnect_factsuppressiondetailpsc`.sourceaprimarykeyvalue,
  `openconnect_factsuppressiondetailpsc`.sendingapplication,
  `openconnect_factsuppressiondetailpsc`.insertedby,
  `openconnect_factsuppressiondetailpsc`.inserteddtm,
  `openconnect_factsuppressiondetailpsc`.modifiedby,
  `openconnect_factsuppressiondetailpsc`.modifieddtm,
  `openconnect_factsuppressiondetailpsc`.dwlastupdatedatetime
  FROM
    edwpsc.`openconnect_factsuppressiondetailpsc`
;