delete from edwra_staging.wq_org_account  a
where exists ( select 1 from edwra_staging.wq_org_account b  
                where b.id =a.id 
                and  b.schema_id =a.schema_id  
                and b.mon_account_payer_id =a.mon_account_payer_id 
                and a.dw_last_update_date < b.dw_last_update_date);

delete from edwra_staging.RA_Claim_Supplement  a
where exists ( select 1 from edwra_staging.RA_Claim_Supplement b  
            where b.id =a.id  
            and  b.schema_id =a.schema_id   
            and a.dw_last_update_date < b.dw_last_update_date)
