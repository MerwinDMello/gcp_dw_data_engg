CREATE OR REPLACE VIEW edwpsc_views.`ccu_rprtcoidprovidervariance`
AS SELECT
  `ccu_rprtcoidprovidervariance`.ccucoidprovidervariancehistorykey,
  `ccu_rprtcoidprovidervariance`.priorsnapshotdatekey,
  `ccu_rprtcoidprovidervariance`.snapshotdate,
  `ccu_rprtcoidprovidervariance`.recordtype,
  `ccu_rprtcoidprovidervariance`.coid,
  `ccu_rprtcoidprovidervariance`.providernpi,
  `ccu_rprtcoidprovidervariance`.providername,
  `ccu_rprtcoidprovidervariance`.priorstatus,
  `ccu_rprtcoidprovidervariance`.priorcentralizedstatus,
  `ccu_rprtcoidprovidervariance`.currstatus,
  `ccu_rprtcoidprovidervariance`.currcentralizedstatus,
  `ccu_rprtcoidprovidervariance`.result,
  `ccu_rprtcoidprovidervariance`.resultcount,
  `ccu_rprtcoidprovidervariance`.loaddate,
  `ccu_rprtcoidprovidervariance`.dwlastupdatedatetime,
  `ccu_rprtcoidprovidervariance`.insertedby,
  `ccu_rprtcoidprovidervariance`.inserteddtm,
  `ccu_rprtcoidprovidervariance`.modifiedby,
  `ccu_rprtcoidprovidervariance`.modifieddtm
  FROM
    edwpsc_base_views.`ccu_rprtcoidprovidervariance`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ccu_rprtcoidprovidervariance`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;