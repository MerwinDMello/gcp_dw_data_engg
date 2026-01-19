CREATE OR REPLACE VIEW edwpsc_views.`ecw_rprtcashmgmtarunbalanced`
AS SELECT
  `ecw_rprtcashmgmtarunbalanced`.coid,
  `ecw_rprtcashmgmtarunbalanced`.posteddate,
  `ecw_rprtcashmgmtarunbalanced`.arsystemname,
  `ecw_rprtcashmgmtarunbalanced`.arsystemtypeid,
  `ecw_rprtcashmgmtarunbalanced`.arpostedamount,
  `ecw_rprtcashmgmtarunbalanced`.cmtpostedamount,
  `ecw_rprtcashmgmtarunbalanced`.siteid,
  `ecw_rprtcashmgmtarunbalanced`.lastnote,
  `ecw_rprtcashmgmtarunbalanced`.notedate,
  `ecw_rprtcashmgmtarunbalanced`.userid,
  `ecw_rprtcashmgmtarunbalanced`.fullname,
  `ecw_rprtcashmgmtarunbalanced`.arunbalancedkey,
  `ecw_rprtcashmgmtarunbalanced`.dwlastupdatedatetime,
  `ecw_rprtcashmgmtarunbalanced`.reasons,
  `ecw_rprtcashmgmtarunbalanced`.selectedcoiduser
  FROM
    edwpsc_base_views.`ecw_rprtcashmgmtarunbalanced`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(CAST(`ecw_rprtcashmgmtarunbalanced`.coid AS STRING), ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;