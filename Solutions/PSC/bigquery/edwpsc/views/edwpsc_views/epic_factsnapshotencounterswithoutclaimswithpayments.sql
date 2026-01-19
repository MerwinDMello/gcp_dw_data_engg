CREATE OR REPLACE VIEW edwpsc_views.`epic_factsnapshotencounterswithoutclaimswithpayments`
AS SELECT
  `epic_factsnapshotencounterswithoutclaimswithpayments`.encounterswithoutclaimswithpaymentskey,
  `epic_factsnapshotencounterswithoutclaimswithpayments`.monthid,
  `epic_factsnapshotencounterswithoutclaimswithpayments`.snapshotdate,
  `epic_factsnapshotencounterswithoutclaimswithpayments`.coid,
  `epic_factsnapshotencounterswithoutclaimswithpayments`.finacialclasskey,
  `epic_factsnapshotencounterswithoutclaimswithpayments`.facilityid,
  `epic_factsnapshotencounterswithoutclaimswithpayments`.facilitykey,
  `epic_factsnapshotencounterswithoutclaimswithpayments`.encounterid,
  `epic_factsnapshotencounterswithoutclaimswithpayments`.encounterkey,
  `epic_factsnapshotencounterswithoutclaimswithpayments`.dosproviderid,
  `epic_factsnapshotencounterswithoutclaimswithpayments`.servicingproviderkey,
  `epic_factsnapshotencounterswithoutclaimswithpayments`.totalpaymentamt,
  `epic_factsnapshotencounterswithoutclaimswithpayments`.totalpatientpaymentamt,
  `epic_factsnapshotencounterswithoutclaimswithpayments`.lastpaymentdatekey,
  `epic_factsnapshotencounterswithoutclaimswithpayments`.lastpatientpaymentdatekey,
  `epic_factsnapshotencounterswithoutclaimswithpayments`.servicedate,
  `epic_factsnapshotencounterswithoutclaimswithpayments`.patientid,
  `epic_factsnapshotencounterswithoutclaimswithpayments`.patientkey,
  `epic_factsnapshotencounterswithoutclaimswithpayments`.totalbalanceamt,
  `epic_factsnapshotencounterswithoutclaimswithpayments`.practicekey,
  `epic_factsnapshotencounterswithoutclaimswithpayments`.regionkey
  FROM
    edwpsc_base_views.`epic_factsnapshotencounterswithoutclaimswithpayments`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_factsnapshotencounterswithoutclaimswithpayments`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;