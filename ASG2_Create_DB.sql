USE ROLE TRAINING_ROLE;

-- 1) Compute
CREATE OR REPLACE WAREHOUSE ADO_GROUP3_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- 2) Storage/project space
-- CREATE OR REPLACE DATABASE ADO_GROUP3_DB;
CREATE DATABASE IF NOT EXISTS ADO_GROUP3_DB;

-- 3) Layering
-- CREATE OR REPLACE SCHEMA ADO_GROUP3_DB.RAW;
-- CREATE OR REPLACE SCHEMA ADO_GROUP3_DB.ANALYTICS;

CREATE SCHEMA IF NOT EXISTS ADO_GROUP3_DB.RAW;
CREATE SCHEMA IF NOT EXISTS ADO_GROUP3_DB.ANALYTICS;

-- 4) Set context
USE WAREHOUSE ADO_GROUP3_WH;
USE DATABASE ADO_GROUP3_DB;
USE SCHEMA ADO_GROUP3_DB.RAW;


USE DATABASE ADO_GROUP3_DB;
USE SCHEMA ADO_GROUP3_DB.RAW;

CREATE OR REPLACE TABLE SNOWFLAKE_ERROR_LOG (
    log_ts           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_type        STRING,         
    error_message     STRING,
    encounter_id      INT,
    patient_nbr       INT,
    source_table      STRING,
    details           STRING
);

CREATE OR REPLACE FILE FORMAT FF_CSV_STD
  TYPE = CSV
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  TRIM_SPACE = TRUE
  EMPTY_FIELD_AS_NULL = TRUE
  NULL_IF = ('', 'NULL', 'null');

-- master data
CREATE OR REPLACE TABLE DIABETES_RAW(
    encounter_id NUMBER,
    patient_nbr NUMBER,
    race VARCHAR,
    gender VARCHAR,
    age VARCHAR,
    weight VARCHAR,
    admission_type_id NUMBER,
    discharge_disposition_id NUMBER,
    admission_source_id NUMBER,
    time_in_hospital NUMBER,
    payer_code VARCHAR,
    medical_specialty VARCHAR,
    num_lab_procedures NUMBER,
    num_procedures NUMBER,
    num_medications NUMBER,
    number_outpatient NUMBER,
    number_emergency NUMBER,
    number_inpatient NUMBER,
    diag_1 VARCHAR,
    diag_2 VARCHAR,
    diag_3 VARCHAR,
    number_diagnoses NUMBER,
    max_glu_serum VARCHAR,
    A1Cresult VARCHAR,
    metformin VARCHAR,
    repaglinide VARCHAR,
    nateglinide VARCHAR,
    chlorpropamide VARCHAR,
    glimepiride VARCHAR,
    acetohexamide VARCHAR,
    glipizide VARCHAR,
    glyburide VARCHAR,
    tolbutamide VARCHAR,
    pioglitazone VARCHAR,
    rosiglitazone VARCHAR,
    acarbose VARCHAR,
    miglitol VARCHAR,
    troglitazone VARCHAR,
    tolazamide VARCHAR,
    examide VARCHAR,
    citoglipton VARCHAR,
    insulin VARCHAR,
    glyburide_metformin VARCHAR,
    glipizide_metformin VARCHAR,
    glimepiride_pioglitazone VARCHAR,
    metformin_rosiglitazone VARCHAR,
    metformin_pioglitazone VARCHAR,
    change VARCHAR,
    diabetesMed VARCHAR,
    readmitted VARCHAR
);

COPY INTO DIABETES_RAW
FROM @"ADO_GROUP3_DB"."RAW"."CSV_FILES"/diabetic_data.csv
FILE_FORMAT = (FORMAT_NAME = 'ADO_GROUP3_DB.RAW.FF_CSV_STD');


DESC TABLE RAW.DIABETES_RAW;

SELECT *
FROM DIABETES_RAW
LIMIT 10;

-- Cleaning 
CREATE OR REPLACE TABLE RAW.DIABETIC_DATA_CLEAN AS
SELECT
    ENCOUNTER_ID,
    PATIENT_NBR,
     /* race: treat '?' as NULL, then fill Unknown */
    COALESCE(NULLIF(NULLIF(TRIM(RACE), '?'), ''), 'Unknown') AS RACE,

    /* gender: standardise */
    CASE
        WHEN LOWER(TRIM(GENDER)) = 'male' THEN 'M'
        WHEN LOWER(TRIM(GENDER)) = 'female' THEN 'F'
        ELSE 'Unknown'
    END AS GENDER,

    TRIM(AGE) AS AGE,

    ADMISSION_TYPE_ID,
    DISCHARGE_DISPOSITION_ID,
    ADMISSION_SOURCE_ID,
    TIME_IN_HOSPITAL,
    NUM_LAB_PROCEDURES,
    NUM_PROCEDURES,
    NUM_MEDICATIONS,
    NUMBER_OUTPATIENT,
    NUMBER_EMERGENCY,
    NUMBER_INPATIENT,

    TRIM(DIAG_1) AS DIAG_1,
    TRIM(DIAG_2) AS DIAG_2,
    TRIM(DIAG_3) AS DIAG_3,

    NUMBER_DIAGNOSES,
    
    /* fill missing lab results */
    COALESCE(NULLIF(NULLIF(TRIM(MAX_GLU_SERUM), '?'), ''), 'None') AS MAX_GLU_SERUM,
    COALESCE(NULLIF(NULLIF(TRIM(A1CRESULT), '?'), ''), 'None') AS A1CRESULT,

    /* meds */
    TRIM(METFORMIN) AS METFORMIN,
    TRIM(REPAGLINIDE) AS REPAGLINIDE,
    TRIM(NATEGLINIDE) AS NATEGLINIDE,
    TRIM(CHLORPROPAMIDE) AS CHLORPROPAMIDE,
    TRIM(GLIMEPIRIDE) AS GLIMEPIRIDE,
    TRIM(ACETOHEXAMIDE) AS ACETOHEXAMIDE,
    TRIM(GLIPIZIDE) AS GLIPIZIDE,
    TRIM(GLYBURIDE) AS GLYBURIDE,
    TRIM(TOLBUTAMIDE) AS TOLBUTAMIDE,
    TRIM(PIOGLITAZONE) AS PIOGLITAZONE,
    TRIM(ROSIGLITAZONE) AS ROSIGLITAZONE,
    TRIM(ACARBOSE) AS ACARBOSE,
    TRIM(MIGLITOL) AS MIGLITOL,
    TRIM(TROGLITAZONE) AS TROGLITAZONE,
    TRIM(TOLAZAMIDE) AS TOLAZAMIDE,
    TRIM(EXAMIDE) AS EXAMIDE,
    TRIM(INSULIN) AS INSULIN,
    TRIM(GLYBURIDE_METFORMIN) AS GLYBURIDE_METFORMIN,
    TRIM(GLIPIZIDE_METFORMIN) AS GLIPIZIDE_METFORMIN,
    TRIM(GLIMEPIRIDE_PIOGLITAZONE) AS GLIMEPIRIDE_PIOGLITAZONE,
    TRIM(METFORMIN_ROSIGLITAZONE) AS METFORMIN_ROSIGLITAZONE,
    TRIM(METFORMIN_PIOGLITAZONE) AS METFORMIN_PIOGLITAZONE,

    TRIM(CHANGE) AS CHANGE,
    TRIM(DIABETESMED) AS DIABETESMED,
    TRIM(READMITTED) AS READMITTED,
    
    /* target flag */
    CASE WHEN TRIM(READMITTED) = '<30' THEN 1 ELSE 0 END AS READMIT_30D
    
FROM RAW.DIABETES_RAW
WHERE DIAG_1 IS NOT NULL
  AND DIAG_2 IS NOT NULL
  AND DIAG_3 IS NOT NULL;

SELECT *
FROM DIABETIC_DATA_CLEAN
LIMIT 10;


-- confirm no '?' in race anymore

SELECT RACE, COUNT(*)
FROM RAW.DIABETIC_DATA_CLEAN
GROUP BY RACE
ORDER BY COUNT(*) DESC;

-- confirm lab imputations
SELECT MAX_GLU_SERUM, COUNT(*) FROM RAW.DIABETIC_DATA_CLEAN GROUP BY MAX_GLU_SERUM ORDER BY COUNT(*) DESC;
SELECT A1CRESULT, COUNT(*) FROM RAW.DIABETIC_DATA_CLEAN GROUP BY A1CRESULT ORDER BY COUNT(*) DESC;

-- confirm gender standardisation
SELECT GENDER, COUNT(*) FROM RAW.DIABETIC_DATA_CLEAN GROUP BY GENDER;


CREATE OR REPLACE TABLE RAW.DIABETIC_DATA_OUTLIERS AS
WITH iqr AS (
  SELECT
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY TIME_IN_HOSPITAL)     AS q1_time,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY TIME_IN_HOSPITAL)     AS q3_time,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY NUM_LAB_PROCEDURES)   AS q1_lab,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY NUM_LAB_PROCEDURES)   AS q3_lab,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY NUM_PROCEDURES)       AS q1_proc,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY NUM_PROCEDURES)       AS q3_proc,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY NUM_MEDICATIONS)      AS q1_meds,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY NUM_MEDICATIONS)      AS q3_meds,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY NUMBER_OUTPATIENT)    AS q1_outp,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY NUMBER_OUTPATIENT)    AS q3_outp,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY NUMBER_EMERGENCY)     AS q1_emer,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY NUMBER_EMERGENCY)     AS q3_emer,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY NUMBER_INPATIENT)     AS q1_inp,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY NUMBER_INPATIENT)     AS q3_inp,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY NUMBER_DIAGNOSES)     AS q1_diag,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY NUMBER_DIAGNOSES)     AS q3_diag
  FROM RAW.DIABETIC_DATA_CLEAN
)
SELECT
  d.*,

  (d.TIME_IN_HOSPITAL < (q1_time - 1.5*(q3_time - q1_time)) OR d.TIME_IN_HOSPITAL > (q3_time + 1.5*(q3_time - q1_time))) AS outlier_time_in_hospital,
  (d.NUM_LAB_PROCEDURES < (q1_lab - 1.5*(q3_lab - q1_lab)) OR d.NUM_LAB_PROCEDURES > (q3_lab + 1.5*(q3_lab - q1_lab)))   AS outlier_num_lab_procedures,
  (d.NUM_PROCEDURES < (q1_proc - 1.5*(q3_proc - q1_proc)) OR d.NUM_PROCEDURES > (q3_proc + 1.5*(q3_proc - q1_proc)))     AS outlier_num_procedures,
  (d.NUM_MEDICATIONS < (q1_meds - 1.5*(q3_meds - q1_meds)) OR d.NUM_MEDICATIONS > (q3_meds + 1.5*(q3_meds - q1_meds)))    AS outlier_num_medications,
  (d.NUMBER_OUTPATIENT < (q1_outp - 1.5*(q3_outp - q1_outp)) OR d.NUMBER_OUTPATIENT > (q3_outp + 1.5*(q3_outp - q1_outp))) AS outlier_number_outpatient,
  (d.NUMBER_EMERGENCY < (q1_emer - 1.5*(q3_emer - q1_emer)) OR d.NUMBER_EMERGENCY > (q3_emer + 1.5*(q3_emer - q1_emer))) AS outlier_number_emergency,
  (d.NUMBER_INPATIENT < (q1_inp - 1.5*(q3_inp - q1_inp)) OR d.NUMBER_INPATIENT > (q3_inp + 1.5*(q3_inp - q1_inp)))       AS outlier_number_inpatient,
  (d.NUMBER_DIAGNOSES < (q1_diag - 1.5*(q3_diag - q1_diag)) OR d.NUMBER_DIAGNOSES > (q3_diag + 1.5*(q3_diag - q1_diag))) AS outlier_number_diagnoses,

  (
    (d.TIME_IN_HOSPITAL < (q1_time - 1.5*(q3_time - q1_time)) OR d.TIME_IN_HOSPITAL > (q3_time + 1.5*(q3_time - q1_time)))
    OR (d.NUM_LAB_PROCEDURES < (q1_lab - 1.5*(q3_lab - q1_lab)) OR d.NUM_LAB_PROCEDURES > (q3_lab + 1.5*(q3_lab - q1_lab)))
    OR (d.NUM_PROCEDURES < (q1_proc - 1.5*(q3_proc - q1_proc)) OR d.NUM_PROCEDURES > (q3_proc + 1.5*(q3_proc - q1_proc)))
    OR (d.NUM_MEDICATIONS < (q1_meds - 1.5*(q3_meds - q1_meds)) OR d.NUM_MEDICATIONS > (q3_meds + 1.5*(q3_meds - q1_meds)))
    OR (d.NUMBER_OUTPATIENT < (q1_outp - 1.5*(q3_outp - q1_outp)) OR d.NUMBER_OUTPATIENT > (q3_outp + 1.5*(q3_outp - q1_outp)))
    OR (d.NUMBER_EMERGENCY < (q1_emer - 1.5*(q3_emer - q1_emer)) OR d.NUMBER_EMERGENCY > (q3_emer + 1.5*(q3_emer - q1_emer)))
    OR (d.NUMBER_INPATIENT < (q1_inp - 1.5*(q3_inp - q1_inp)) OR d.NUMBER_INPATIENT > (q3_inp + 1.5*(q3_inp - q1_inp)))
    OR (d.NUMBER_DIAGNOSES < (q1_diag - 1.5*(q3_diag - q1_diag)) OR d.NUMBER_DIAGNOSES > (q3_diag + 1.5*(q3_diag - q1_diag)))
  ) AS flagged_outlier

FROM RAW.DIABETIC_DATA_CLEAN d
CROSS JOIN iqr;

SELECT
  SUM(IFF(flagged_outlier,1,0)) AS rows_flagged,
  COUNT(*) AS total_rows
FROM RAW.DIABETIC_DATA_OUTLIERS;

USE SCHEMA ANALYTICS;

CREATE OR REPLACE TABLE ANALYTICS.DIABETIC_DATA_FINAL AS
SELECT *
FROM RAW.DIABETIC_DATA_OUTLIERS
QUALIFY ROW_NUMBER() OVER (PARTITION BY ENCOUNTER_ID ORDER BY ENCOUNTER_ID) = 1;

-- IDS Mapping 
CREATE OR REPLACE TABLE ADMISSION_SOURCE AS
SELECT * FROM VALUES
(1,'Physician Referral'),
(2,'Clinic Referral'),
(3,'HMO Referral'),
(4,'Transfer from a hospital'),
(5,'Transfer from a Skilled Nursing Facility (SNF)'),
(6,'Transfer from another health care facility'),
(7,'Emergency Room'),
(8,'Court/Law Enforcement'),
(9,'Not Available'),
(10,'Transfer from critial access hospital'),
(11,'Normal Delivery'),
(12,'Premature Delivery'),
(13,'Sick Baby'),
(14,'Extramural Birth'),
(15,'Not Available'),
(17,NULL),
(18,'Transfer From Another Home Health Agency'),
(19,'Readmission to Same Home Health Agency'),
(20,'Not Mapped'),
(21,'Unknown/Invalid'),
(22,'Transfer from hospital inpt/same fac reslt in a sep claim'),
(23,'Born inside this hospital'),
(24,'Born outside this hospital'),
(25,'Transfer from Ambulatory Surgery Center'),
(26,'Transfer from Hospice'
) AS T(admission_source_id, description);


SELECT *
FROM ADMISSION_SOURCE
LIMIT 10;


CREATE OR REPLACE TABLE ADMISSION_TYPE AS
SELECT * FROM VALUES
(1, 'Emergency'),
(2, 'Urgent'),
(3, 'Elective'),
(4, 'Newborn'),
(5, 'Not Available'),
(6, 'NULL'),
(7, 'Trauma Center'),
(8, 'Not Mapped')
AS T(admission_type_id, description);

SELECT *
FROM ADMISSION_TYPE
LIMIT 10;


CREATE OR REPLACE TABLE DISCHARGE_DISPOSITION AS
SELECT * FROM VALUES
(1,'Discharged to home'),
(2,'Discharged/transferred to another short term hospital'),
(3,'Discharged/transferred to SNF'),
(4,'Discharged/transferred to ICF'),
(5,'Discharged/transferred to another type of inpatient care institution'),
(6,'Discharged/transferred to home with home health service'),
(7,'Left AMA'),
(8,'Discharged/transferred to home under care of Home IV provider'),
(9,'Admitted as an inpatient to this hospital'),
(10,'Neonate discharged to another hospital for neonatal aftercare'),
(11,'Expired'),
(12,'Still patient or expected to return for outpatient services'),
(13,'Hospice / home'),
(14,'Hospice / medical facility'),
(15,'Discharged/transferred within this institution to Medicare approved swing bed'),
(16,'Discharged/transferred/referred another institution for outpatient services'),
(17,'Discharged/transferred/referred to this institution for outpatient services'),
(18,NULL),
(19,'Expired at home. Medicaid only, hospice.'),
(20,'Expired in a medical facility. Medicaid only, hospice.'),
(21,'Expired, place unknown. Medicaid only, hospice.'),
(22,'Discharged/transferred to another rehab fac including rehab units of a hospital .'),
(23,'Discharged/transferred to a long term care hospital.'),
(24,'Discharged/transferred to a nursing facility certified under Medicaid but not certified under Medicare.'),
(25,'Not Mapped'),
(26,'Unknown/Invalid'),
(27,'Discharged/transferred to a federal health care facility.'),
(28,'Discharged/transferred/referred to a psychiatric hospital of psychiatric distinct part unit of a hospital'),
(29,'Discharged/transferred to a Critical Access Hospital (CAH).'),
(30,'Discharged/transferred to another Type of Health Care Institution not Defined Elsewhere'
) AS T(discharge_disposition_id, description);


SELECT *
FROM DISCHARGE_DISPOSITION
LIMIT 10;

CREATE OR REPLACE TABLE DENORMALISED_TABLE AS
SELECT
d.*,
a. description as admission_source_desc,
x.description as admission_type_desc,
y.description as discharge_disposition_desc

FROM DIABETES_RAW d
LEFT JOIN ADMISSION_SOURCE a
    ON d.admission_source_id = a.admission_source_id
LEFT JOIN ADMISSION_TYPE x
    ON d.admission_type_id = x.admission_type_id
LEFT JOIN DISCHARGE_DISPOSITION y
    ON d.discharge_disposition_id = y.discharge_disposition_id;

SELECT *
FROM DENORMALISED_TABLE
LIMIT 10;

-- ===========================
-- VALIDATION: Mandatory Fields
-- ===========================
INSERT INTO SNOWFLAKE_ERROR_LOG (
    error_type,
    error_message,
    encounter_id,
    patient_nbr,
    source_table,
    details
)
SELECT
    'VALIDATION_ERROR',
    'Missing mandatory field(s)',
    encounter_id,
    patient_nbr,
    'DIABETES_RAW',
    'patient_nbr or encounter_id is NULL'
FROM DIABETES_RAW
WHERE patient_nbr IS NULL
   OR encounter_id IS NULL;
-- ===========================
-- VALIDATION: Duplicate Records
-- ===========================
INSERT INTO SNOWFLAKE_ERROR_LOG (
    error_type,
    error_message,
    encounter_id,
    patient_nbr,
    source_table,
    details
)
SELECT
    'DUPLICATE',
    'Duplicate encounter detected',
    encounter_id,
    patient_nbr,
    'DIABETES_RAW',
    'Duplicate on (encounter_id, patient_nbr)'
FROM DIABETES_RAW
QUALIFY COUNT(*) OVER (
    PARTITION BY encounter_id, patient_nbr
) > 1;
-- ===========================
-- VALIDATION: Reference Table Mismatches
-- ===========================
-- Admission Source
INSERT INTO SNOWFLAKE_ERROR_LOG (
    error_type,
    error_message,
    encounter_id,
    patient_nbr,
    source_table,
    details
)
SELECT
    'RESULT_MISMATCH',
    'Invalid admission_source_id',
    d.encounter_id,
    d.patient_nbr,
    'DENORMALISED_TABLE',
    'No matching admission_source_id'
FROM DENORMALISED_TABLE d
WHERE admission_source_id IS NOT NULL
  AND admission_source_desc IS NULL;

-- Admission Type
INSERT INTO SNOWFLAKE_ERROR_LOG (
    error_type,
    error_message,
    encounter_id,
    patient_nbr,
    source_table,
    details
)
SELECT
    'RESULT_MISMATCH',
    'Invalid admission_type_id',
    d.encounter_id,
    d.patient_nbr,
    'DENORMALISED_TABLE',
    'No matching admission_type_id'
FROM DENORMALISED_TABLE d
WHERE admission_type_id IS NOT NULL
  AND admission_type_desc IS NULL;

-- Discharge Disposition
INSERT INTO SNOWFLAKE_ERROR_LOG (
    error_type,
    error_message,
    encounter_id,
    patient_nbr,
    source_table,
    details
)
SELECT
    'RESULT_MISMATCH',
    'Invalid discharge_disposition_id',
    d.encounter_id,
    d.patient_nbr,
    'DENORMALISED_TABLE',
    'No matching discharge_disposition_id'
FROM DENORMALISED_TABLE d
WHERE discharge_disposition_id IS NOT NULL
  AND discharge_disposition_desc IS NULL;

-- =========================================
-- Test Error
-- =========================================
  INSERT INTO SNOWFLAKE_ERROR_LOG (
    error_type,
    error_message,
    encounter_id,
    patient_nbr,
    source_table,
    details
)
VALUES (
    'TEST_ERROR',
    'This is a test error to confirm logging works',
    999999,
    888888,
    'DIABETES_RAW',
    'Simulated test entry'
);
-- ===========================
-- VIEW VALIDATION ERRORS
-- ===========================
SELECT *
FROM SNOWFLAKE_ERROR_LOG
ORDER BY log_ts DESC
LIMIT 20;

ADO_GROUP3_DBADO_GROUP3_DB-- Suspend 
ALTER WAREHOUSE ADO_GROUP3_WH SUSPEND;









