CREATE OR REPLACE VIEW edwpsc_base_views.`pv_factsnapshotencounterswithoutclaimswithpayments`
AS SELECT
  `pv_factsnapshotencounterswithoutclaimswithpayments`.encounterswithoutclaimswithpaymentskey,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.monthid,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.snapshotdate,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.coid,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.finacialclasskey,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.facilityid,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.facilitykey,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.encounterid,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.encounterkey,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.dosproviderid,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.servicingproviderkey,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.totalpaymentamt,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.totalpatientpaymentamt,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.lastpaymentdatekey,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.lastpatientpaymentdatekey,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.servicedate,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.patientid,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.patientkey,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.totalbalanceamt,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.practiceid,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.practicekey,
  `pv_factsnapshotencounterswithoutclaimswithpayments`.regionkey
  FROM
    edwpsc.`pv_factsnapshotencounterswithoutclaimswithpayments`
;