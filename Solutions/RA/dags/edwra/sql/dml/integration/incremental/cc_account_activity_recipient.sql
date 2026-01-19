DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_account_activity_recipient.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/************************************************************************************************
     Developer: Holly Ray
          Date: 8/13/2011
          Name: CC_Account_Recipient_Build.sql
          Mod1: Replaces original CC_Account_Recipient_Build table due to revised Activity Data Model.
          Mod2: Another table revision from Architect Review. 09072011 HR
          Mod3: Changed path to DB Logon for DMExpress conversion on 2/12/2012 SW.
**************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA217;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_account_act_recipient AS x USING
  (SELECT a.patient_dw_id AS patient_dw_id,
          maar.mon_account_activity_id AS activity_id,
          maar.id AS activity_recipient_id,
          reforg.company_code AS company_cd,
          CASE
              WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
              ELSE og.client_id
          END AS coido,
          og.short_name AS unit_num,
          ma.account_no AS pat_acct_nbr,
          usr.login_id AS recipient_login_userid,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
          'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_act_recipient AS maar
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr ON maar.user_id_recipient = usr.user_id
   AND maar.schema_id = usr.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_activity AS maa ON maar.mon_account_activity_id = maa.id
   AND maar.schema_id = maa.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON maa.mon_account_id = ma.id
   AND maa.schema_id = ma.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
   AND ma.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(CASE
                                                                                                                             WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                             ELSE og.client_id
                                                                                                                         END))
   AND reforg.schema_id = maar.schema_id
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(CASE
                                                                                                                        WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                        ELSE og.client_id
                                                                                                                    END))
   AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
   AND a.pat_acct_num = ma.account_no) AS z ON x.patient_dw_id = z.patient_dw_id
AND x.activity_id = z.activity_id
AND x.activity_recipient_id = z.activity_recipient_id WHEN MATCHED THEN
UPDATE
SET company_code = z.company_cd,
    coid = substr(z.coido, 1, 5),
    unit_num = substr(z.unit_num, 1, 5),
    pat_acct_num = z.pat_acct_nbr,
    recipient_login_userid = substr(z.recipient_login_userid, 1, 20),
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        activity_id,
        activity_recipient_id,
        company_code,
        coid,
        unit_num,
        pat_acct_num,
        recipient_login_userid,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.patient_dw_id, z.activity_id, z.activity_recipient_id, z.company_cd, substr(z.coido, 1, 5), substr(z.unit_num, 1, 5), z.pat_acct_nbr, substr(z.recipient_login_userid, 1, 20), datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id,
             activity_id,
             activity_recipient_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_account_act_recipient
      GROUP BY patient_dw_id,
               activity_id,
               activity_recipient_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_account_act_recipient');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;