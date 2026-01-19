CREATE OR REPLACE VIEW edwpsc_views.`pv_factsnapshotchargeline`
AS SELECT
  `pv_factsnapshotchargeline`.chargelinekey,
  `pv_factsnapshotchargeline`.monthid,
  `pv_factsnapshotchargeline`.snapshotdate,
  `pv_factsnapshotchargeline`.coid,
  `pv_factsnapshotchargeline`.regionkey,
  `pv_factsnapshotchargeline`.practicekey,
  `pv_factsnapshotchargeline`.practiceid,
  `pv_factsnapshotchargeline`.claimkey,
  `pv_factsnapshotchargeline`.claimnumber,
  `pv_factsnapshotchargeline`.gldepartment,
  `pv_factsnapshotchargeline`.patientid,
  `pv_factsnapshotchargeline`.servicingproviderkey,
  `pv_factsnapshotchargeline`.servicingproviderid,
  `pv_factsnapshotchargeline`.renderingproviderkey,
  `pv_factsnapshotchargeline`.renderingproviderid,
  `pv_factsnapshotchargeline`.facilitykey,
  `pv_factsnapshotchargeline`.facilityid,
  `pv_factsnapshotchargeline`.claimdatekey,
  `pv_factsnapshotchargeline`.servicedatekey,
  `pv_factsnapshotchargeline`.iplan1iplankey,
  `pv_factsnapshotchargeline`.iplan1id,
  `pv_factsnapshotchargeline`.financialclasskey,
  `pv_factsnapshotchargeline`.cptid,
  `pv_factsnapshotchargeline`.cptorder,
  `pv_factsnapshotchargeline`.cptcode,
  `pv_factsnapshotchargeline`.cptcodekey,
  `pv_factsnapshotchargeline`.cptstartservicedatekey,
  `pv_factsnapshotchargeline`.cptendservicedatekey,
  `pv_factsnapshotchargeline`.cpttypeofservice,
  `pv_factsnapshotchargeline`.cptchargesamt,
  `pv_factsnapshotchargeline`.cptchargesperunitamt,
  `pv_factsnapshotchargeline`.cptunits,
  `pv_factsnapshotchargeline`.cptmodifier1,
  `pv_factsnapshotchargeline`.cptmodifier2,
  `pv_factsnapshotchargeline`.cptmodifier3,
  `pv_factsnapshotchargeline`.cptmodifier4,
  `pv_factsnapshotchargeline`.cptdeleteflag,
  `pv_factsnapshotchargeline`.dwlastupdatedatetime,
  `pv_factsnapshotchargeline`.cptposkey,
  `pv_factsnapshotchargeline`.claimlinechargekey,
  `pv_factsnapshotchargeline`.cptbalance
  FROM
    edwpsc_base_views.`pv_factsnapshotchargeline`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factsnapshotchargeline`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;