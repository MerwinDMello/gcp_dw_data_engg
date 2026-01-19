
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefAgingBucket ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefAgingBucket (AgeInDays, BucketDescription1, BucketDescription2, BucketDescription3, Percnt, PercentBucket_0_10_gt100, PercentBucket_0_10_gt100Sort, AgingBucket_0_30_NoMax, AgingBucket_0_30_NoMaxSort, AgingBucket_000_030_720plus, AgingBucket_000_030_720plusSort, AgingBucket_a000_a030_NoMax, AgingBucket_a000_a030_NoMaxSort, AgingBucket_0_6_gt365, AgingBucket_0_6_gt365Sort, AgingBucket_0_30_gt360, AgingBucket_0_30_gt360Sort)
SELECT source.AgeInDays, TRIM(source.BucketDescription1), TRIM(source.BucketDescription2), TRIM(source.BucketDescription3), source.Percnt, TRIM(source.PercentBucket_0_10_gt100), source.PercentBucket_0_10_gt100Sort, TRIM(source.AgingBucket_0_30_NoMax), TRIM(source.AgingBucket_0_30_NoMaxSort), TRIM(source.AgingBucket_000_030_720plus), source.AgingBucket_000_030_720plusSort, TRIM(source.AgingBucket_a000_a030_NoMax), TRIM(source.AgingBucket_a000_a030_NoMaxSort), TRIM(source.AgingBucket_0_6_gt365), source.AgingBucket_0_6_gt365Sort, TRIM(source.AgingBucket_0_30_gt360), source.AgingBucket_0_30_gt360Sort
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefAgingBucket as source;
