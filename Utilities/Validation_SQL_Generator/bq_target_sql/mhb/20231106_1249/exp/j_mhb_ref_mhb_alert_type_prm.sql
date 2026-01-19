-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/exp/j_mhb_ref_mhb_alert_type_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          x.*
        FROM
          (
            SELECT
                x_0.*
              FROM
                (
                  SELECT DISTINCT
                      vwpatientalerttracker.alert_title
                    FROM
                      `hca-hin-dev-cur-clinical`.edwci_staging.vwpatientalerttracker
                ) AS x_0
              WHERE upper(x_0.alert_title) NOT IN(
                SELECT
                    upper(ref_mhb_alert_type.alert_type_desc) AS alert_type_desc
                  FROM
                    `hca-hin-dev-cur-clinical`.edwci.ref_mhb_alert_type
              )
          ) AS x
    ) AS q
;
