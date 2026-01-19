SELECT
CONTACTID AS patient_contact_id,
PATIENTID AS cr_patient_id,
RELATION AS contact_relation_id,
TYPE AS contact_type_id,
CONTACTCODE AS contact_num_code,
TRIM(CNTFIRSTNAME) AS contact_first_name,
TRIM(CNTLASTNAME) AS contact_last_name,
TRIM(CNTMIDDLENAME) AS contact_middle_name,
REMARKS AS preferred_contact_method_text,
'M' as source_system_code,
'v_currtimestamp' as dw_last_update_date_time 
FROM
(
SELECT 
  C.CONTACTID, 
  PATIENTID, 
  RELATION, 
  TYPE, 
  CONTACTCODE, 
  CNTFIRSTNAME, 
  CNTLASTNAME, 
  CNTMIDDLENAME, 
  REMARKS 
FROM 
  MREGISTRY.DBO.CONTACTS C 
  INNER JOIN (
    SELECT 
      CONTACTID, 
      COUNT(*) CNT 
    FROM 
      MREGISTRY.DBO.CONTACTS 
    GROUP BY 
      CONTACTID 
    HAVING 
      COUNT(*)= 1
  ) A ON C.CONTACTID = A.CONTACTID 
UNION ALL 
SELECT 
  CONTACTID, 
  PATIENTID, 
  RELATION, 
  TYPE, 
  CONTACTCODE, 
  CNTFIRSTNAME, 
  CNTLASTNAME, 
  CNTMIDDLENAME, 
  REMARKS 
FROM 
  (
    SELECT 
      *, 
      ROW_NUMBER() OVER(
        PARTITION BY CONTACTID 
        ORDER BY 
          CONTACTSHOSPID DESC
      ) RN 
    FROM 
      MREGISTRY.DBO.CONTACTS 
    WHERE 
      CONTACTID IN (
        SELECT 
          C.CONTACTID 
        FROM 
          MREGISTRY.DBO.CONTACTS C 
          INNER JOIN (
            SELECT 
              CONTACTID, 
              COUNT(*) CNT 
            FROM 
              MREGISTRY.DBO.CONTACTS 
            GROUP BY 
              CONTACTID 
            HAVING 
              COUNT(*)> 1
          ) A ON C.CONTACTID = A.CONTACTID 
          INNER JOIN MREGISTRY.DBO.CONTACTS C1 ON C.CONTACTID = C1.CONTACTID 
          AND C1.CONTACTSHOSPID IS NULL
      )
  ) B 
WHERE 
  B.RN = 1
) C