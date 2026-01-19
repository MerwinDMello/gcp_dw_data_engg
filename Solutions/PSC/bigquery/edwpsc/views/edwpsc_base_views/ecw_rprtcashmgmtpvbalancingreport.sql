CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_rprtcashmgmtpvbalancingreport`
AS SELECT
  `ecw_rprtcashmgmtpvbalancingreport`.practice,
  `ecw_rprtcashmgmtpvbalancingreport`.coid,
  `ecw_rprtcashmgmtpvbalancingreport`.posteddate,
  `ecw_rprtcashmgmtpvbalancingreport`.pvamount,
  `ecw_rprtcashmgmtpvbalancingreport`.cmtpostedamount,
  `ecw_rprtcashmgmtpvbalancingreport`.variance,
  `ecw_rprtcashmgmtpvbalancingreport`.pvtype,
  `ecw_rprtcashmgmtpvbalancingreport`.pvtypeid,
  `ecw_rprtcashmgmtpvbalancingreport`.specialistfullname,
  `ecw_rprtcashmgmtpvbalancingreport`.lastnote,
  `ecw_rprtcashmgmtpvbalancingreport`.notedate,
  `ecw_rprtcashmgmtpvbalancingreport`.userid,
  `ecw_rprtcashmgmtpvbalancingreport`.pvbalancingreportkey,
  `ecw_rprtcashmgmtpvbalancingreport`.dwlastupdatedatetime,
  `ecw_rprtcashmgmtpvbalancingreport`.reasonnames,
  `ecw_rprtcashmgmtpvbalancingreport`.selectedcoiduser
  FROM
    edwpsc.`ecw_rprtcashmgmtpvbalancingreport`
;