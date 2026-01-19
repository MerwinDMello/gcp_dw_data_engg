-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/exp/j_mhb_ref_mhb_dynamic_role_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT
     (SELECT coalesce(max(ref_mhb_dynamic_role.dynamic_role_sid), 0)
      FROM `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_dynamic_role) + row_number() OVER (
                                                                                                  ORDER BY y.rdc_sid,
                                                                                                           upper(y.dynamicrole_name),
                                                                                                           upper(y.dynamicrole_label)) AS dynamic_role_sid,
                                                                                                 y.rdc_sid AS rdc_sid,
                                                                                                 y.dynamicrole_name AS mhb_dynamic_role_parent_name,
                                                                                                 y.dynamicrole_label AS mhb_dynamic_role_child_name,
                                                                                                 'B' AS source_system_code,
                                                                                                 timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT vwdynamicrole.dynamicrole_name,
             vwdynamicrole.dynamicrole_label,
             rdc.rdc_sid
      FROM
        (SELECT max(vwdynamicroledetachment.dynamicrole_name) AS dynamicrole_name,
                max(vwdynamicroledetachment.dynamicrole_label) AS dynamicrole_label,
                max(vwdynamicroledetachment.databasename) AS databasename
         FROM `hca-hin-dev-cur-clinical`.edwci_staging.vwdynamicroledetachment
         GROUP BY upper(vwdynamicroledetachment.dynamicrole_name),
                  upper(vwdynamicroledetachment.dynamicrole_label),
                  upper(vwdynamicroledetachment.databasename)
         UNION DISTINCT SELECT substr(max(vwdynamicroleattachment.dynamicrole_name), 1, 30) AS dynamicrole_name,
                               substr(max(vwdynamicroleattachment.dynamicrole_label), 1, 40) AS dynamicrole_label,
                               max(vwdynamicroleattachment.databasename) AS databasename
         FROM `hca-hin-dev-cur-clinical`.edwci_staging.vwdynamicroleattachment
         GROUP BY upper(substr(vwdynamicroleattachment.dynamicrole_name, 1, 30)),
                  upper(substr(vwdynamicroleattachment.dynamicrole_label, 1, 40)),
                  upper(vwdynamicroleattachment.databasename)) AS vwdynamicrole
      INNER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_regional_data_center AS rdc ON upper(substr(trim(vwdynamicrole.databasename), strpos(trim(vwdynamicrole.databasename), '_') + 1)) = upper(rdc.rdc_desc)
      LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_dynamic_role AS dr ON upper(vwdynamicrole.dynamicrole_name) = upper(dr.mhb_dynamic_role_parent_name)
      AND upper(vwdynamicrole.dynamicrole_label) = upper(dr.mhb_dynamic_role_child_name)
      AND rdc.rdc_sid = dr.rdc_sid
      WHERE dr.dynamic_role_sid IS NULL ) AS y) AS q