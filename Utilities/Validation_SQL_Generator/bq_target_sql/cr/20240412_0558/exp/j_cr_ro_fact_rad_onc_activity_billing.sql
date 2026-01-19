-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cr_ro_fact_rad_onc_activity_billing.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          row_number() OVER (ORDER BY dp.dimsiteid, dp.factactivitybillingid DESC) AS fact_activity_billing_sk
        FROM
          (
            SELECT
                factactivitybilling_stg.dimdoctorid,
                factactivitybilling_stg.dimdoctid_attdoncologist,
                factactivitybilling_stg.dimcourseid,
                factactivitybilling_stg.dimhospitaldepartmentid,
                factactivitybilling_stg.dimactivityid,
                factactivitybilling_stg.dimactivitytransactionid,
                factactivitybilling_stg.dimprocedurecodeid,
                factactivitybilling_stg.dimpatientid,
                factactivitybilling_stg.dimlkpid_activitycategory,
                factactivitybilling_stg.dimsiteid,
                factactivitybilling_stg.factactivitybillingid,
                factactivitybilling_stg.primaryglobalcharge,
                factactivitybilling_stg.primarytechnicalcharge,
                factactivitybilling_stg.primaryprofessionalcharge,
                factactivitybilling_stg.otherprofessionalcharge,
                factactivitybilling_stg.othertechnicalcharge,
                factactivitybilling_stg.otherglobalcharge,
                factactivitybilling_stg.chargeforecast,
                factactivitybilling_stg.actualcharge,
                factactivitybilling_stg.activitycost,
                trim(factactivitybilling_stg.accountbillingcode) AS accountbillingcode,
                factactivitybilling_stg.fromdateofservice,
                factactivitybilling_stg.todateofservice,
                factactivitybilling_stg.completeddatetime,
                factactivitybilling_stg.exporteddatetime,
                factactivitybilling_stg.markedcompleteddatetime,
                factactivitybilling_stg.creditexporteddatetime,
                factactivitybilling_stg.crediteddatetime,
                trim(factactivitybilling_stg.creditnote) AS creditnote,
                trim(factactivitybilling_stg.allmodifiercodes) AS allmodifiercodes,
                factactivitybilling_stg.creditamount,
                factactivitybilling_stg.isscheduled,
                factactivitybilling_stg.objectstatus,
                factactivitybilling_stg.logid,
                factactivitybilling_stg.runid
              FROM
                `hca-hin-dev-cur-ops`.edwcr_staging.factactivitybilling_stg
          ) AS dp
          INNER JOIN (
            SELECT
                ref_rad_onc_site.source_site_id,
                ref_rad_onc_site.site_sk
              FROM
                `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_site
          ) AS rr ON dp.dimsiteid = rr.source_site_id
    ) AS stg
;
