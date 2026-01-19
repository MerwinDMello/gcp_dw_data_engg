
delete from hca-hin-prod-cur-parallon.`edwra_staging.wq_org_account` a
where exists ( select 1 from hca-hin-qa-cur-parallon.`edwra_staging.wq_org_account_06232025` b
                where a.id =b.id and a.schema_id=b.schema_id and a.org_id=b.org_id and a.wq_profile_id=b.wq_profile_id
                    and a.wq_rule_id=b.wq_rule_id and a.mon_account_id=b.mon_account_id and a.mon_account_payer_id=b.mon_account_payer_id
                    and a.job_no=b.job_no and a.run_date=b.run_date and a.dw_last_update_date=b.dw_last_update_date);


INSERT INTO hca-hin-prod-cur-parallon.`edwra_staging.wq_org_account` 
(id,schema_id,org_id,wq_profile_id,wq_rule_id,mon_account_id, mon_account_payer_id,job_no,run_date,dw_last_update_date)
SELECT id,schema_id,org_id,wq_profile_id,wq_rule_id,mon_account_id, mon_account_payer_id,job_no,run_date,dw_last_update_date
FROM hca-hin-qa-cur-parallon.`edwra_staging.wq_org_account_06232025`;