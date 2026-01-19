-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/exp/j_mhb_message_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT rdc.rdc_sid AS rdc_sid,
          x.messages_id AS mhb_message_id
   FROM
     (SELECT vwtextmessages.messages_id,
             max(vwtextmessages.sender_facilitycode) AS sender_facilitycode,
             max(vwtextmessages.sent_date_time) AS sent_date_time,
             max(vwtextmessages.message_content) AS message_content,
             max(vwtextmessages.urgent) AS urgent,
             max(vwtextmessages.quickpick) AS quickpick,
             max(vwtextmessages.databasename) AS databasename
      FROM `hca-hin-dev-cur-clinical`.edwci_staging.vwtextmessages
      GROUP BY 1,
               upper(vwtextmessages.sender_facilitycode),
               upper(vwtextmessages.sent_date_time),
               upper(vwtextmessages.message_content),
               upper(vwtextmessages.urgent),
               upper(vwtextmessages.quickpick),
               upper(vwtextmessages.databasename)) AS x
   INNER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_regional_data_center AS rdc ON upper(rdc.rdc_desc) = upper(replace(x.databasename, 'heartbeatDW_', ''))
   LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcl_base_views.clinical_facility AS cf ON trim(cf.facility_mnemonic_cs) = trim(x.sender_facilitycode)) AS q