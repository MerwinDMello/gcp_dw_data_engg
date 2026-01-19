CREATE OR REPLACE VIEW edwpsc_views.`ecw_refsrtproductivityresultcode`
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
    edwpsc_base_views.`ecw_refsrtproductivityresultcode`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(CAST(`ecw_refsrtproductivityresultcode`.coid AS STRING), ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;