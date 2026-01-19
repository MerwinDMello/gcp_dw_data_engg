CREATE OR REPLACE VIEW edwpsc_views.`ecw_factsnapshotencounterswithoutclaimswithpayments`
AS SELECT
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.encounterswithoutclaimswithpaymentskey,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.monthid,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.snapshotdate,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.coid,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.finacialclasskey,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.facilityid,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.facilitykey,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.encounterid,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.encounterkey,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.dosproviderid,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.servicingproviderkey,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.totalpaymentamt,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.totalpatientpaymentamt,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.lastpaymentdatekey,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.lastpatientpaymentdatekey,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.servicedate,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.patientid,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.patientkey,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.totalbalanceamt,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.practiceid,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.practicekey,
  `ecw_factsnapshotencounterswithoutclaimswithpayments`.regionkey
  FROM
    edwpsc_base_views.`ecw_factsnapshotencounterswithoutclaimswithpayments`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factsnapshotencounterswithoutclaimswithpayments`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;