
SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT Nav_Patient_Lab_Result_SID ,
 Hashbite_SSK
 FROM edwcr_staging.CN_Patient_Lab_Result_stg
 WHERE Hashbite_SSK NOT IN
 (SELECT Hashbite_SSK
 FROM edwcr.CN_Patient_Lab_Result) ) A;