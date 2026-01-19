-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_imaging.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT stg.cn_patient_imaging_sid,
          stg.core_record_type_id,
          stg.nav_patient_id,
          stg.med_spcl_physician_id,
          stg.tumor_type_id,
          stg.diagnosis_result_id,
          stg.nav_diagnosis_id,
          stg.navigator_id,
          stg.coid,
          'H' AS company_code,
          stg.imaging_type_id,
          stg.imaging_date,
          rim.imaging_mode_id AS imaging_mode_id,
          rs.side_id AS imaging_area_side_id,
          stg.imaging_location_text,
          rf.facility_id AS imaging_facility_id,
          CASE
              WHEN upper(rtrim(stg.birad_scale_code)) = 'RESULTS NOT AVAILABLE' THEN CAST(NULL AS STRING)
              ELSE stg.birad_scale_code
          END AS birad_scale_code, -- STG.Birad_Scale_Code,
 stg.comment_text,
 ds.status_id AS disease_status_id,
 ts.status_id AS treatment_status_id,
 stg.other_image_type_text,
 CASE upper(rtrim(stg.initial_diagnosis_ind))
     WHEN 'YES' THEN 'Y'
     WHEN 'NO' THEN 'N'
     ELSE 'U'
 END AS initial_diagnosis_ind,
 CASE upper(rtrim(stg.disease_monitoring_ind))
     WHEN 'YES' THEN 'Y'
     WHEN 'NO' THEN 'N'
     ELSE 'U'
 END AS disease_monitoring_ind,
 stg.radiology_result_text,
 stg.hashbite_ssk,
 'N' AS source_system_code,
 datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_imaging_stg AS stg
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_imaging_mode AS rim ON upper(rtrim(coalesce(trim(stg.imagemode), 'X'))) = upper(rtrim(coalesce(trim(rim.imaging_mode_desc), 'X')))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_side AS rs ON upper(rtrim(coalesce(trim(stg.imagearea), 'XX'))) = upper(rtrim(coalesce(trim(rs.side_desc), 'XX')))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_facility AS rf ON upper(rtrim(coalesce(trim(stg.imagecenter), 'XXX'))) = upper(rtrim(coalesce(trim(rf.facility_name), 'XXX')))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_status AS ds ON upper(rtrim(coalesce(trim(stg.disease_status), 'XXX'))) = upper(rtrim(coalesce(trim(ds.status_desc), 'XXX')))
   AND upper(rtrim(ds.status_type_desc)) = 'DISEASE'
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_status AS ts ON upper(rtrim(coalesce(trim(stg.treatment_status), 'XXX'))) = upper(rtrim(coalesce(trim(ts.status_desc), 'XXX')))
   AND upper(rtrim(ts.status_type_desc)) = 'TREATMENT'
   WHERE upper(stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_imaging.hashbite_ssk) AS hashbite_ssk
        FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_imaging) ) AS src