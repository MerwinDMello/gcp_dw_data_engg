-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/exp/j_mhb_broadcast_message_detail_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          rdc.rdc_sid AS rdc_sid
        FROM
          `hca-hin-dev-cur-clinical`.edwci_staging.vwbroadcastmessages AS a
          INNER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_regional_data_center AS rdc ON upper(substr(trim(a.databasename), strpos(trim(a.databasename), '_') + 1)) = upper(trim(rdc.rdc_desc))
          INNER JOIN -- ON OREPLACE(TRIM(A.DATABASENAME),'HEARTBEATDW_','') = TRIM(RDC.RDC_DESC)
          `hca-hin-dev-cur-clinical`.edwci_base_views.mhb_user AS usr ON upper(a.from_username) = upper(usr.user_login_name)
           AND usr.rdc_sid = rdc.rdc_sid
           AND usr.eff_to_date = DATE '9999-12-31'
           AND upper(usr.active_dw_ind) = 'Y'
    ) AS q
;
