delete
from edwra_staging.ra_835_category_aggregated o
where exists
      (select 1 from edwra_staging.ra_835_category_aggregated i
        where i.schema_id =  o.schema_id
        and i.ra_claim_payment_id =  o.ra_claim_payment_id
        and i.ra_category_id = o.ra_category_id
        and i.amount = o.amount 
        and i.id > o.id)