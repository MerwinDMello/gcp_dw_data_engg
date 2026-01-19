-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/exp/j_mhb_ref_mhb_unit_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          rdc.rdc_sid AS rdc_sid,
          vwunit.unit_id AS mhb_unit_id,
          coalesce(cf.company_code, 'H') AS company_code,
          coalesce(cf.coid, '99999') AS coid,
          vwunit.unitname AS mhb_unit_name,
          vwunit.unitcode AS mhb_unit_short_name,
          vwunit.abbreviation AS mhb_alt_unit_name,
          vwunit.subtype AS mhb_sub_type_name,
          'B' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
        FROM
          `hca-hin-dev-cur-clinical`.edwci_staging.vwunits AS vwunit
          INNER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_regional_data_center AS rdc ON upper(substr(trim(vwunit.databasename), strpos(trim(vwunit.databasename), '_') + 1)) = upper(rdc.rdc_desc)
          LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edw_pub_views.clinical_facility AS cf ON trim(cf.facility_mnemonic_cs) = trim(vwunit.facilitycode)
          LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_unit AS un ON rdc.rdc_sid = un.rdc_sid
           AND vwunit.unit_id = un.mhb_unit_id
        WHERE un.rdc_sid IS NULL
    ) AS q
;
