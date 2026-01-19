CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refsrtproductivityresultcode`
AS SELECT
  `ecw_refsrtproductivityresultcode`.productivityresultcodekey,
  `ecw_refsrtproductivityresultcode`.coid,
  `ecw_refsrtproductivityresultcode`.controlnumber,
  `ecw_refsrtproductivityresultcode`.invoicename,
  `ecw_refsrtproductivityresultcode`.resultcode,
  `ecw_refsrtproductivityresultcode`.sourceaprimarykeyvalue,
  `ecw_refsrtproductivityresultcode`.deleteflag,
  `ecw_refsrtproductivityresultcode`.dwlastupdatedatetime,
  `ecw_refsrtproductivityresultcode`.sourcesystemcode,
  `ecw_refsrtproductivityresultcode`.insertedby,
  `ecw_refsrtproductivityresultcode`.inserteddtm,
  `ecw_refsrtproductivityresultcode`.modifiedby,
  `ecw_refsrtproductivityresultcode`.modifieddtm
  FROM
    edwpsc.`ecw_refsrtproductivityresultcode`
;