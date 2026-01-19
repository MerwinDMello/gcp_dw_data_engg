CREATE OR REPLACE VIEW edwpsc_views.`ecw_rprtopenietsummarybycoid`
AS SELECT
  `ecw_rprtopenietsummarybycoid`.snapshotdate,
  `ecw_rprtopenietsummarybycoid`.groupname,
  `ecw_rprtopenietsummarybycoid`.divisionname,
  `ecw_rprtopenietsummarybycoid`.marketname,
  `ecw_rprtopenietsummarybycoid`.coid,
  `ecw_rprtopenietsummarybycoid`.coidname,
  `ecw_rprtopenietsummarybycoid`.claimnumber,
  `ecw_rprtopenietsummarybycoid`.claimbalance,
  `ecw_rprtopenietsummarybycoid`.errorcount,
  `ecw_rprtopenietsummarybycoid`.claimkey
  FROM
    edwpsc_base_views.`ecw_rprtopenietsummarybycoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_rprtopenietsummarybycoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;