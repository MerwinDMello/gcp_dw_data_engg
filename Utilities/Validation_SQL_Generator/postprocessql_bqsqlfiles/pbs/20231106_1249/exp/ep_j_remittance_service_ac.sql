-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_remittance_service_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT rs.service_guid AS service_guid,
          rs.claim_guid AS claim_guid,
          rs.audit_date AS audit_date,
          rs.delete_ind,
          rs.delete_date,
          coalesce(rc.coid, '') AS coid,
          coalesce(rc.company_code, '') AS company_code,
          rs.charge_amt AS charge_amt,
          rs.payment_amt AS payment_amt,
          rs.coinsurance_amt AS coinsurance_amt,
          rs.deductible_amt AS deductible_amt,
          rs.adjudicated_hcpcs_code AS adjudicated_hcpcs_code,
          rs.submitted_hcpcs_code AS submitted_hcpcs_code,
          rs.procedure_6_desc_7 AS submitted_hcpcs_code_desc,
          rs.revenue_code AS payor_sent_revenue_code,
          rs.adjudicated_hipps_code AS adjudicated_hipps_code,
          rs.submitted_hipps_code AS submitted_hipps_code,
          rs.apc_code AS apc_code,
          rs.apc_amt AS apc_amt,
          rs.quantity AS adjudicated_service_qty,
          rs.original_quantity_cnt AS submitted_service_qty,
          rs.hca_category AS service_category_code,
          rs.date_time_qualifier1 AS date_time_qualifier_code_1,
          parse_date('%Y/%m/%d', CASE
                                     WHEN length(rs.service_date1) > 0 THEN CASE
                                                                                WHEN length(SPLIT(rs.service_date1, '/')[ORDINAL(1)]) = 1
                                                                                     AND length(SPLIT(rs.service_date1, '/')[ORDINAL(2)]) = 1 THEN concat(SPLIT(rs.service_date1, '/')[ORDINAL(3)], '/0', SPLIT(rs.service_date1, '/')[ORDINAL(1)], '/0', SPLIT(rs.service_date1, '/')[ORDINAL(2)])
                                                                                WHEN length(SPLIT(rs.service_date1, '/')[ORDINAL(1)]) = 1 THEN concat(SPLIT(rs.service_date1, '/')[ORDINAL(3)], '/0', SPLIT(rs.service_date1, '/')[ORDINAL(1)], '/', SPLIT(rs.service_date1, '/')[ORDINAL(2)])
                                                                                WHEN length(SPLIT(rs.service_date1, '/')[ORDINAL(2)]) = 1 THEN concat(SPLIT(rs.service_date1, '/')[ORDINAL(3)], '/', SPLIT(rs.service_date1, '/')[ORDINAL(1)], '/0', SPLIT(rs.service_date1, '/')[ORDINAL(2)])
                                                                                ELSE concat(SPLIT(rs.service_date1, '/')[ORDINAL(3)], '/', SPLIT(rs.service_date1, '/')[ORDINAL(1)], '/', SPLIT(rs.service_date1, '/')[ORDINAL(2)])
                                                                            END
                                     ELSE CAST(NULL AS STRING)
                                 END) AS service_date_1, -- SERVICE_DATE1 as SERVICE_DATE_1,
 rs.date_time_qualifier2 AS date_time_qualifier_code_2,
 parse_date('%Y/%m/%d', CASE
                            WHEN length(rs.service_date2) > 0 THEN CASE
                                                                       WHEN length(SPLIT(rs.service_date2, '/')[ORDINAL(1)]) = 1
                                                                            AND length(SPLIT(rs.service_date2, '/')[ORDINAL(2)]) = 1 THEN concat(SPLIT(rs.service_date2, '/')[ORDINAL(3)], '/0', SPLIT(rs.service_date2, '/')[ORDINAL(1)], '/0', SPLIT(rs.service_date2, '/')[ORDINAL(2)])
                                                                       WHEN length(SPLIT(rs.service_date2, '/')[ORDINAL(1)]) = 1 THEN concat(SPLIT(rs.service_date2, '/')[ORDINAL(3)], '/0', SPLIT(rs.service_date2, '/')[ORDINAL(1)], '/', SPLIT(rs.service_date2, '/')[ORDINAL(2)])
                                                                       WHEN length(SPLIT(rs.service_date2, '/')[ORDINAL(2)]) = 1 THEN concat(SPLIT(rs.service_date2, '/')[ORDINAL(3)], '/', SPLIT(rs.service_date2, '/')[ORDINAL(1)], '/0', SPLIT(rs.service_date2, '/')[ORDINAL(2)])
                                                                       ELSE concat(SPLIT(rs.service_date2, '/')[ORDINAL(3)], '/', SPLIT(rs.service_date2, '/')[ORDINAL(1)], '/', SPLIT(rs.service_date2, '/')[ORDINAL(2)])
                                                                   END
                            ELSE CAST(NULL AS STRING)
                        END) AS service_date_2, -- SERVICE_DATE2 as SERVICE_DATE_2,
 timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time,
 'E' AS source_system_code
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service AS rs
   LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs.remittance_claim AS rc ON upper(rc.claim_guid) = upper(rs.claim_guid)
   WHERE DATE(rs.dw_last_update_date_time) =
       (SELECT max(DATE(remittance_service.dw_last_update_date_time))
        FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service) ) AS a 