CREATE OR REPLACE VIEW edwpsc_views.`ecw_rprthistoricalatriskmor`
AS SELECT
  `ecw_rprthistoricalatriskmor`.snapshotdate,
  `ecw_rprthistoricalatriskmor`.atrisksnapshotdate,
  `ecw_rprthistoricalatriskmor`.centerdescription,
  `ecw_rprthistoricalatriskmor`.group_name,
  `ecw_rprthistoricalatriskmor`.division_name,
  `ecw_rprthistoricalatriskmor`.lob_code,
  `ecw_rprthistoricalatriskmor`.coid,
  `ecw_rprthistoricalatriskmor`.coid_name,
  `ecw_rprthistoricalatriskmor`.coidandname,
  `ecw_rprthistoricalatriskmor`.currentmonthatrisk,
  `ecw_rprthistoricalatriskmor`.priormonthatrisk,
  `ecw_rprthistoricalatriskmor`.nonparadj,
  `ecw_rprthistoricalatriskmor`.cmdosatrisk,
  `ecw_rprthistoricalatriskmor`.total_balance_2mp,
  `ecw_rprthistoricalatriskmor`.total_balance_3mp,
  `ecw_rprthistoricalatriskmor`.total_balance_4mp,
  `ecw_rprthistoricalatriskmor`.adjamt_pm,
  `ecw_rprthistoricalatriskmor`.adjamt_2mp,
  `ecw_rprthistoricalatriskmor`.adjamt_3mp,
  `ecw_rprthistoricalatriskmor`.insertedby,
  `ecw_rprthistoricalatriskmor`.inserteddtm,
  `ecw_rprthistoricalatriskmor`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_rprthistoricalatriskmor`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_rprthistoricalatriskmor`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;