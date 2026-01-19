-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cr_ro_rad_onc_activity_transaction.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT row_number() OVER (
                             ORDER BY upper(dp.dimsiteid),
                                      upper(dp.dimactivityid) DESC) AS fact_patient_toxicity_sk
   FROM
     (SELECT stg_dimactivitytransaction.dimsiteid,
             stg_dimactivitytransaction.dimactivityid,
             stg_dimactivitytransaction.activitypriority,
             stg_dimactivitytransaction.dimhospitaldepartmentid,
             stg_dimactivitytransaction.dimpatientid,
             stg_dimactivitytransaction.dimlookupid_appointmentstatus,
             stg_dimactivitytransaction.dimlookupid_actualresourcetype,
             stg_dimactivitytransaction.dimlookupid_cancelreasontype,
             stg_dimactivitytransaction.dimlookupid_appointmentrsrcsta,
             stg_dimactivitytransaction.dimactivitytransactionid,
             stg_dimactivitytransaction.scheduledendtime,
             stg_dimactivitytransaction.appointmentdatetime,
             stg_dimactivitytransaction.isscheduled,
             stg_dimactivitytransaction.activitystartdatetime,
             stg_dimactivitytransaction.activityenddatetime,
             trim(stg_dimactivitytransaction.activitynote) AS activitynote,
             stg_dimactivitytransaction.checkedin,
             stg_dimactivitytransaction.patientarrivaldatetime,
             stg_dimactivitytransaction.waitlistedflag,
             trim(stg_dimactivitytransaction.patientlocation) AS patientlocation,
             stg_dimactivitytransaction.appointmentinstanceflag,
             stg_dimactivitytransaction.derivedappointmenttaskdate,
             stg_dimactivitytransaction.activityownerflag,
             stg_dimactivitytransaction.isvisittypeopenchart,
             stg_dimactivitytransaction.ctrresourceser,
             stg_dimactivitytransaction.logid,
             stg_dimactivitytransaction.runid
      FROM {{ params.param_cr_stage_dataset_name }}.stg_dimactivitytransaction) AS dp
   INNER JOIN
     (SELECT ref_rad_onc_site.source_site_id,
             ref_rad_onc_site.site_sk
      FROM {{ params.param_cr_core_dataset_name }}.ref_rad_onc_site
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) AS rr ON CAST({{ params.param_cr_bqutil_fns_dataset_name }}.cw_td_normalize_number(dp.dimsiteid) AS FLOAT64) = rr.source_site_id) AS stg