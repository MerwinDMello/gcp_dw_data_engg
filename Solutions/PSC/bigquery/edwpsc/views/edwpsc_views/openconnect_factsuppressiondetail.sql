CREATE OR REPLACE VIEW edwpsc_views.`openconnect_factsuppressiondetail`
AS SELECT
  `openconnect_factsuppressiondetail`.openconnectsuppressiondetailkey,
  `openconnect_factsuppressiondetail`.messagecreateddate,
  `openconnect_factsuppressiondetail`.routestep,
  `openconnect_factsuppressiondetail`.statusreason,
  `openconnect_factsuppressiondetail`.statusmessage,
  `openconnect_factsuppressiondetail`.deleteflag,
  `openconnect_factsuppressiondetail`.dwlastupdateddate,
  `openconnect_factsuppressiondetail`.sourceaprimarykeyvalue,
  `openconnect_factsuppressiondetail`.insertedby,
  `openconnect_factsuppressiondetail`.inserteddtm,
  `openconnect_factsuppressiondetail`.modifiedby,
  `openconnect_factsuppressiondetail`.modifieddtm,
  `openconnect_factsuppressiondetail`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`openconnect_factsuppressiondetail`
;