CREATE OR REPLACE VIEW edwpsc_views.`eboc_facttransactions`
AS SELECT
  `eboc_facttransactions`.keyid,
  `eboc_facttransactions`.username,
  `eboc_facttransactions`.userid,
  `eboc_facttransactions`.gldate,
  `eboc_facttransactions`.linecount,
  `eboc_facttransactions`.dollars,
  `eboc_facttransactions`.treasurybatchnumber,
  `eboc_facttransactions`.dwlastupdatedatetime,
  `eboc_facttransactions`.insertedby,
  `eboc_facttransactions`.inserteddtm,
  `eboc_facttransactions`.modifiedby,
  `eboc_facttransactions`.modifieddtm
  FROM
    edwpsc_base_views.`eboc_facttransactions`
;