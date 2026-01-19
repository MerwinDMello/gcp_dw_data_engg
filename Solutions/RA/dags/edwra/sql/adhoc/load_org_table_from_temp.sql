

INSERT INTO hca-hin-prod-cur-parallon.`edwra_staging.org` 
(org_id,schema_id,customer_org_id,name,acct_id,org_status,org_role,is_contractor,is_public,password_threshold_days,purchased_products,manual_entry_fl,client_id,comments,degree_id,org_type,is_practitioner,market,org_affiliation,pract_first_name,pract_last_name,pract_middle_name,pract_title,gender,website,short_name,is_reassignable,dw_last_update_date,group_stack)
SELECT org_id,schema_id,customer_org_id,name,acct_id,org_status,org_role,is_contractor,is_public,password_threshold_days,purchased_products,manual_entry_fl,client_id,comments,degree_id,org_type,is_practitioner,market,org_affiliation,pract_first_name,pract_last_name,pract_middle_name,pract_title,gender,website,short_name,is_reassignable,dw_last_update_date,group_stack
FROM hca-hin-prod-cur-parallon.`edwra_staging.org_temp` ;