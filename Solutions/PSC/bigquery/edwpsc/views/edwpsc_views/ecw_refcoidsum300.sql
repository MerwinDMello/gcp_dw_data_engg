CREATE OR REPLACE VIEW edwpsc_views.`ecw_refcoidsum300`
AS SELECT
  `ecw_refcoidsum300`.coid,
  `ecw_refcoidsum300`.fein,
  `ecw_refcoidsum300`.hcaps300,
  `ecw_refcoidsum300`.conscoid,
  `ecw_refcoidsum300`.flevel,
  `ecw_refcoidsum300`.legalname,
  `ecw_refcoidsum300`.dwlastupdatedatetime,
  `ecw_refcoidsum300`.sourcesystemcode,
  `ecw_refcoidsum300`.insertedby,
  `ecw_refcoidsum300`.inserteddtm,
  `ecw_refcoidsum300`.modifiedby,
  `ecw_refcoidsum300`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_refcoidsum300`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_refcoidsum300`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;