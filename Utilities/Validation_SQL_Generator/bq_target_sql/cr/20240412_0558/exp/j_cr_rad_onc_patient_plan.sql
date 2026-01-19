-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cr_rad_onc_patient_plan.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimplan AS dp
    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_plan_purpose AS rpp ON upper(rtrim(dp.planintent)) = upper(rtrim(rpp.plan_purpose_name))
    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_site AS rr ON rr.source_site_id = CAST(bqutil.fn.cw_td_normalize_number(dp.dimsiteid) as FLOAT64)
    LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient_course AS rpc ON rpc.source_patient_course_id = CAST(bqutil.fn.cw_td_normalize_number(dp.dimcourseid) as FLOAT64)
    LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient_plan AS core ON rr.site_sk = core.site_sk
     AND CAST(bqutil.fn.cw_td_normalize_number(dp.dimplanid) as FLOAT64) = core.source_patient_plan_id
  WHERE core.patient_plan_sk IS NULL
;
