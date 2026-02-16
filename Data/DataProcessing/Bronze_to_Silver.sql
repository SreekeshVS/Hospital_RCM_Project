
-- CREATING SILVER TABLE for Full Load : Providers
CREATE TABLE IF NOT EXISTS `project-b2d1202e-c674-4674-a8a.silver_dataset.providers`
(
	NPI INTEGER,
	Specialization STRING,
	DeptID STRING,
	LastName STRING,
	FirstName STRING,
	src_ProviderID STRING,
	hospital_source STRING,
	ProviderID STRING,
	is_quarantined BOOLEAN
);

-- Truncate silver table before moving bronze data 
TRUNCATE TABLE `project-b2d1202e-c674-4674-a8a.silver_dataset.providers`;

-- Moving data from bronze table to silver table CDM
INSERT INTO `project-b2d1202e-c674-4674-a8a.silver_dataset.providers`
SELECT
	NPI,
	Specialization,
	DeptID,
	LastName,
	FirstName,
	srcProviderID,
	hospital_source,
	concat(srcProviderID, '-', hospital_source) as Provider_ID,
	case when srcProviderID is null or DeptID is null then TRUE
	else FALSE
	end as is_quarantined
FROM
	(SELECT 
		distinct
		NPI,
		Specialization,
		DeptID,
		LastName,
		FirstName,
		ProviderID as srcProviderID,
		'hos_a' as hospital_source,
	FROM
		`project-b2d1202e-c674-4674-a8a.bronze_dataset.providers_hb`
	UNION ALL
	SELECT 
		distinct
		NPI,
		Specialization,
		DeptID,
		LastName,
		FirstName,
		ProviderID as srcProviderID,
		'hos_b' as hospital_source
	FROM
		`project-b2d1202e-c674-4674-a8a.bronze_dataset.providers_hb`) AS temp;
		
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------	
------------------------------------------------------------------------------------------------------------------------			
		
-- CREATING FULL Load table for departments in silver dataset
CREATE TABLE IF NOT EXISTS `project-b2d1202e-c674-4674-a8a.silver_dataset.departments`
(
	srcDeptID STRING,
	Name STRING,
	hospital_source STRING,
	DeptID STRING,
	is_quarantined BOOLEAN
);

-- Truncating departments table before loading data from bronze
TRUNCATE TABLE `project-b2d1202e-c674-4674-a8a.silver_dataset.departments`;

-- Combining data from Bronze Hospital A and B into silver departments table

INSERT INTO `project-b2d1202e-c674-4674-a8a.silver_dataset.departments`
SELECT 
	srcDeptID,
	Name,
	hospital_source,
	concat(srcDeptID, '-', hospital_source) as DeptID,
	case when srcDeptID is null or Name is null then TRUE
	else FALSE
	end as is_quarantined
FROM
(
	SELECT
		DeptID as srcDeptID,
		Name,
		'hos_a' as hospital_source
	FROM project-b2d1202e-c674-4674-a8a.bronze_dataset.departments_ha
	UNION ALL
	SELECT
		DeptID as srcDeptID,
		Name,
		'hos_b' as hospital_source
	FROM project-b2d1202e-c674-4674-a8a.bronze_dataset.departments_hb
) AS temp;


------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------	
------------------------------------------------------------------------------------------------------------------------	


-- CREATING incremental Load table for encounters in silver dataset
CREATE TABLE IF NOT EXISTS `project-b2d1202e-c674-4674-a8a.silver_dataset.encounters`
(
	EncounterKey STRING,
	srcEncounterID STRING,
	srcModifiedDate INTEGER,
	InsertedDate INTEGER,
	ProcedureCode INTEGER,
	DepartmentID STRING,
	ProviderID STRING,
	EncounterDate INTEGER,
	PatientID STRING,
	EncounterType STRING,
	hospital_source STRING,
	is_quarantined BOOLEAN,
	inserted_date TIMESTAMP,
	modified_date TIMESTAMP,
	is_current BOOLEAN
);

-- Combining data from Bronze Hospital A and B into pre silver encounters table
CREATE OR REPLACE TABLE `project-b2d1202e-c674-4674-a8a.silver_dataset.pre_encounters`
AS
SELECT 
	DISTINCT
	srcEncounterID,
	ModifiedDate as srcModifiedDate,
	InsertedDate,
	ProcedureCode,
	DepartmentID,
	ProviderID,
	EncounterDate,
	PatientID,
	EncounterType,
	hospital_source,
	concat(srcEncounterID,'-',hospital_source) as EncounterKey,
	case when srcEncounterID is null or EncounterDate is null or EncounterType is null or PatientID is null then TRUE
	ELSE FALSE
	end as is_quarantined
FROM
(
	SELECT
		DISTINCT
		EncounterID as srcEncounterID,
		ModifiedDate,
		InsertedDate,
		ProcedureCode,
		DepartmentID,
		ProviderID,
		EncounterDate,
		PatientID,
		EncounterType,
		'hos_a' as hospital_source
	from
		project-b2d1202e-c674-4674-a8a.bronze_dataset.encounters_ha
	UNION ALL
	SELECT
		DISTINCT
		EncounterID as srcEncounterID,
		ModifiedDate,
		InsertedDate,
		ProcedureCode,
		DepartmentID,
		ProviderID,
		EncounterDate,
		PatientID,
		EncounterType,
		'hos_b' as hospital_source
	from
		project-b2d1202e-c674-4674-a8a.bronze_dataset.encounters_hb
) AS temp;


-- Running MERGE to the silver dataset table encounters

MERGE INTO `project-b2d1202e-c674-4674-a8a.silver_dataset.encounters` as target
USING `project-b2d1202e-c674-4674-a8a.silver_dataset.pre_encounters` as source
ON target.EncounterKey = source.EncounterKey
AND target.is_current = TRUE

WHEN MATCHED AND(
	target.srcModifiedDate <> source.srcModifiedDate OR
	target.InsertedDate <> source.InsertedDate OR 
	target.ProcedureCode <> source.ProcedureCode OR 
	target.DepartmentID <> source.DepartmentID OR 
	target.ProviderID <> source.ProviderID OR 
	target.EncounterDate <> source.EncounterDate OR 
	target.EncounterType <> source.EncounterType OR 
	target.hospital_source <> source.hospital_source OR 
	target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET
	target.is_current = FALSE,
	target.modified_date = CURRENT_TIMESTAMP()
  
WHEN NOT MATCHED then
INSERT(
	EncounterKey,
	srcEncounterID,
	srcModifiedDate,
	InsertedDate,
	ProcedureCode,
	DepartmentID,
	ProviderID,
	EncounterDate,
	PatientID,
	EncounterType,
	hospital_source,
	is_quarantined,
	inserted_date,
	modified_date,
	is_current
)
VALUES(
	source.EncounterKey,
	source.srcEncounterID,
	source.srcModifiedDate,
	source.InsertedDate,
	source.ProcedureCode,
	source.DepartmentID,
	source.ProviderID,
	source.EncounterDate,
	source.PatientID,
	source.EncounterType,
	source.hospital_source,
	source.is_quarantined,
	CURRENT_TIMESTAMP(),
	NULL,
	TRUE
);


-- DROP pre table after merging the data to silver table
DROP TABLE `project-b2d1202e-c674-4674-a8a.silver_dataset.pre_encounters`;



------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------	
------------------------------------------------------------------------------------------------------------------------	


-- CREATING incremental Load table for patients in silver dataset
CREATE TABLE IF NOT EXISTS `project-b2d1202e-c674-4674-a8a.silver_dataset.patients`
(
	PatientKey STRING,
	srcPatientID STRING,
	FirstName STRING,
	MiddleName STRING,
	LastName STRING,
	DOB INTEGER,
	Address STRING,
	Gender STRING,
	PhoneNumber STRING,
	srcModifiedDate INTEGER,
	SSN STRING,
	hospital_source STRING,
	is_quarantined BOOLEAN,
	inserted_date TIMESTAMP,
	modified_date TIMESTAMP,
	is_current BOOLEAN
);


-- Combining data from Bronze Hospital A and B into pre silver patients table
CREATE OR REPLACE TABLE `project-b2d1202e-c674-4674-a8a.silver_dataset.pre_patients`
AS
SELECT 
	DISTINCT
	srcPatientID,
	FirstName,
	MiddleName,
	LastName,
	DOB,
	Address,
	Gender,
	PhoneNumber,
	srcModifiedDate,
	SSN,
	hospital_source,
	concat(srcPatientID,'-',hospital_source) as PatientKey,
	case when srcPatientID is null or FirstName is null or LastName is null or SSN is null or DOB is null then TRUE
	ELSE FALSE
	end as is_quarantined
FROM
(
	SELECT
		DISTINCT
		PatientID as srcPatientID,
		FirstName,
		MiddleName,
		LastName,
		DOB,
		Address,
		Gender,
		PhoneNumber,
		ModifiedDate as srcModifiedDate,
		SSN,
		'hos_a' as hospital_source
	from
		project-b2d1202e-c674-4674-a8a.bronze_dataset.patients_ha
	UNION ALL
	SELECT
		DISTINCT
		ID as srcPatientID,
		F_Name as FirstName,
		M_Name as MiddleName,
		L_Name as LastName,
		DOB,
		Address,
		Gender,
		PhoneNumber,
		ModifiedDate as srcModifiedDate,
		SSN,
		'hos_b' as hospital_source
	from
		project-b2d1202e-c674-4674-a8a.bronze_dataset.patients_hb
) AS temp;


-- Running MERGE to the silver dataset table patients

MERGE INTO `project-b2d1202e-c674-4674-a8a.silver_dataset.patients` as target
USING `project-b2d1202e-c674-4674-a8a.silver_dataset.pre_patients` as source
ON target.PatientKey = source.PatientKey
AND target.is_current = TRUE

WHEN MATCHED AND(
	target.Address <> source.Address OR
	target.PhoneNumber <> source.PhoneNumber OR 
	target.srcModifiedDate <> source.srcModifiedDate OR
	target.hospital_source <> source.hospital_source OR 
	target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET
	target.is_current = FALSE,
	target.modified_date = CURRENT_TIMESTAMP()
	
WHEN NOT MATCHED then
INSERT(
	PatientKey,
	srcPatientID,
	FirstName,
	MiddleName,
	LastName,
	DOB,
	Address,
	Gender,
	PhoneNumber,
	srcModifiedDate,
	SSN,
	hospital_source,
	is_quarantined,
	inserted_date,
	modified_date,
	is_current
)
VALUES(
	source.PatientKey,
	source.srcPatientID,
	source.FirstName,
	source.MiddleName,
	source.LastName,
	source.DOB,
	source.Address,
	source.Gender,
	source.PhoneNumber,
	source.srcModifiedDate,
	source.SSN,
	source.hospital_source,
	source.is_quarantined,
	CURRENT_TIMESTAMP(),
	NULL,
	TRUE
);


-- DROP pre table after merging the data to silver table
DROP TABLE `project-b2d1202e-c674-4674-a8a.silver_dataset.pre_patients`;



------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------	
------------------------------------------------------------------------------------------------------------------------	

-- CREATING incremental Load table for transactions in silver dataset
CREATE TABLE IF NOT EXISTS `project-b2d1202e-c674-4674-a8a.silver_dataset.transactions`
(
	TransactionKey STRING,
	srcTransactionID STRING,
	DeptID STRING,
	EncounterID STRING,
	PatientID STRING,
	MedicaidID STRING,
	ProviderID STRING,
	MedicareID STRING,
	ClaimID STRING,
	PayorID STRING,
	srcModifiedDate INTEGER,
	srcInsertDate INTEGER,
	LineOfBusiness STRING,
	ICDCode STRING,
	AmountType STRING,
	Amount FLOAT64,
	VisitType STRING,
	ProcedureCode INTEGER,
	PaidDate INTEGER,
	ServiceDate INTEGER,
	VisitDate INTEGER,
	PaidAmount FLOAT64,
	hospital_source STRING,
	is_quarantined BOOLEAN,
	inserted_date TIMESTAMP,
	modified_date TIMESTAMP,
	is_current BOOLEAN
);


-- Combining data from Bronze Hospital A and B into pre silver transactions table
CREATE OR REPLACE TABLE `project-b2d1202e-c674-4674-a8a.silver_dataset.pre_transactions`
AS
SELECT 
	DISTINCT
	srcTransactionID,
	DeptID,
	EncounterID,
	PatientID,
	MedicaidID,
	ProviderID,
	MedicareID,
	ClaimID,
	PayorID,
	srcModifiedDate,
	srcInsertDate,
	LineOfBusiness,
	ICDCode,
	AmountType,
	Amount,
	VisitType,
	ProcedureCode,
	PaidDate,
	ServiceDate,
	VisitDate,
	PaidAmount,
	hospital_source,
	concat(srcTransactionID,'-',hospital_source) as TransactionKey,
	case when srcTransactionID is null or DeptID is null or EncounterID is null or PatientID is null or ProviderID is null or PayorID is null or AmountType is null or Amount is null or ProcedureCode is null or PaidAmount is null then TRUE
	ELSE FALSE
	end as is_quarantined
FROM
(
	SELECT
		DISTINCT
		TransactionID as srcTransactionID,
		DeptID,
		EncounterID,
		PatientID,
		MedicaidID,
		ProviderID,
		MedicareID,
		ClaimID,
		PayorID,
		ModifiedDate as srcModifiedDate,
		InsertDate as srcInsertDate,
		LineOfBusiness,
		ICDCode,
		AmountType,
		Amount,
		VisitType,
		ProcedureCode,
		PaidDate,
		ServiceDate,
		VisitDate,
		PaidAmount,
		'hos_a' as hospital_source
	from
		project-b2d1202e-c674-4674-a8a.bronze_dataset.transactions_ha
	UNION ALL
	SELECT
		DISTINCT
		TransactionID as srcTransactionID,
		DeptID,
		EncounterID,
		PatientID,
		MedicaidID,
		ProviderID,
		MedicareID,
		ClaimID,
		PayorID,
		ModifiedDate as srcModifiedDate,
		InsertDate as srcInsertDate,
		LineOfBusiness,
		ICDCode,
		AmountType,
		Amount,
		VisitType,
		ProcedureCode,
		PaidDate,
		ServiceDate,
		VisitDate,
		PaidAmount,
		'hos_b' as hospital_source
	from
		project-b2d1202e-c674-4674-a8a.bronze_dataset.transactions_hb
) AS temp;


-- Running MERGE to the silver dataset table transactions

MERGE INTO `project-b2d1202e-c674-4674-a8a.silver_dataset.transactions` as target
USING `project-b2d1202e-c674-4674-a8a.silver_dataset.pre_transactions` as source
ON target.TransactionKey = source.TransactionKey
AND target.is_current = TRUE

WHEN MATCHED AND(
	target.srcTransactionID <> source.srcTransactionID OR
	target.DeptID <> source.DeptID OR
	target.EncounterID <> source.EncounterID OR
	target.PatientID <> source.PatientID OR
	target.MedicaidID <> source.MedicaidID OR
	target.ProviderID <> source.ProviderID OR
	target.MedicareID <> source.MedicareID OR
	target.ClaimID <> source.ClaimID OR
	target.PayorID <> source.PayorID OR
	target.srcModifiedDate <> source.srcModifiedDate OR
	target.srcInsertDate <> source.srcInsertDate OR
	target.LineOfBusiness <> source.LineOfBusiness OR
	target.ICDCode <> source.ICDCode OR
	target.AmountType <> source.AmountType OR
	target.Amount <> source.Amount OR
	target.VisitType <> source.VisitType OR
	target.ProcedureCode <> source.ProcedureCode OR
	target.PaidDate <> source.PaidDate OR
	target.ServiceDate <> source.ServiceDate OR
	target.VisitDate <> source.VisitDate OR
	target.PaidAmount <> source.PaidAmount
)
THEN UPDATE SET
	target.is_current = FALSE,
	target.modified_date = CURRENT_TIMESTAMP()
	
WHEN NOT MATCHED then
INSERT(
	TransactionKey,
	srcTransactionID,
	DeptID,
	EncounterID,
	PatientID,
	MedicaidID,
	ProviderID,
	MedicareID,
	ClaimID,
	PayorID,
	srcModifiedDate,
	srcInsertDate,
	LineOfBusiness,
	ICDCode,
	AmountType,
	Amount,
	VisitType,
	ProcedureCode,
	PaidDate,
	ServiceDate,
	VisitDate,
	PaidAmount,
	hospital_source,
	is_quarantined,
	inserted_date,
	modified_date,
	is_current
)
VALUES(
	source.TransactionKey,
	source.srcTransactionID,
	source.DeptID,
	source.EncounterID,
	source.PatientID,
	source.MedicaidID,
	source.ProviderID,
	source.MedicareID,
	source.ClaimID,
	source.PayorID,
	source.srcModifiedDate,
	source.srcInsertDate,
	source.LineOfBusiness,
	source.ICDCode,
	source.AmountType,
	source.Amount,
	source.VisitType,
	source.ProcedureCode,
	source.PaidDate,
	source.ServiceDate,
	source.VisitDate,
	source.PaidAmount,
	source.hospital_source,
	source.is_quarantined,
	CURRENT_TIMESTAMP(),
	NULL,
	TRUE
);


-- DROP pre table after merging the data to silver table
DROP TABLE `project-b2d1202e-c674-4674-a8a.silver_dataset.pre_transactions`;



------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------	
------------------------------------------------------------------------------------------------------------------------	

-- CREATE silver dataset Claims TABLE

CREATE TABLE IF NOT EXISTS project-b2d1202e-c674-4674-a8a.silver_dataset.claims(
	ClaimKey STRING,
	srcClaimID STRING,
	TransactionID STRING,
	PatientID STRING, 
	EncounterID STRING,
	ProviderID STRING,
	DeptID STRING,
	ServiceDate STRING,
	ClaimDate STRING,
	PayorID STRING,
	ClaimAmount STRING,
	PaidAmount STRING, 
	ClaimStatus STRING,
	PayorType STRING,
	Deductible STRING,
	Coinsurance STRING,
	Copay STRING,
	srcInsertDate STRING,
	srcModifiedDate STRING,
	hospital_source STRING,
	is_quarantined BOOLEAN,
	inserted_date TIMESTAMP,
	modified_date TIMESTAMP,
	is_current BOOLEAN
);


-- Moving the data from bronze claims to pre-claims silver table before merging to claims table
CREATE OR REPLACE TABLE project-b2d1202e-c674-4674-a8a.silver_dataset.pre_claims AS
SELECT
	DISTINCT
	srcClaimID,
	TransactionID,
	PatientID,
	EncounterID,
	ProviderID,
	DeptID,
	ServiceDate,
	ClaimDate,
	PayorID,
	ClaimAmount,
	PaidAmount,
	ClaimStatus,
	PayorType,
	Deductible,
	Coinsurance,
	Copay,
	srcInsertDate,
	srcModifiedDate,
	hospital_source,
	concat(srcClaimID,'-',hospital_source) as ClaimKey,
	case when srcClaimID is null or TransactionID is null or PatientID is null or EncounterID is null or ProviderID is null or DeptID is null or ClaimDate is null or PayorID is null or ClaimAmount is null or ClaimAmount is null then TRUE
	ELSE FALSE
	end as is_quarantined
FROM
(
	SELECT
		DISTINCT
		ClaimID as srcClaimID,
		TransactionID,
		PatientID,
		EncounterID,
		ProviderID,
		DeptID,
		ServiceDate,
		ClaimDate,
		PayorID,
		ClaimAmount,
		PaidAmount,
		ClaimStatus,
		PayorType,
		Deductible,
		Coinsurance,
		Copay,
		InsertDate as srcInsertDate,
		ModifiedDate as srcModifiedDate,
		hospital_source
	FROM
		project-b2d1202e-c674-4674-a8a.bronze_dataset.claims_data
) AS temp;


-- Merging the data to silver claims table
MERGE INTO project-b2d1202e-c674-4674-a8a.silver_dataset.claims as target
USING project-b2d1202e-c674-4674-a8a.silver_dataset.pre_claims as source
ON target.ClaimKey = source.ClaimKey
AND target.is_current = TRUE

WHEN MATCHED AND(
	target.srcClaimID <> source.srcClaimID OR
	target.TransactionID <> source.TransactionID OR
	target.PatientID <> source.PatientID OR
	target.EncounterID <> source.EncounterID OR
	target.ProviderID <> source.ProviderID OR
	target.DeptID <> source.DeptID OR
	target.ServiceDate <> source.ServiceDate OR
	target.ClaimDate <> source.ClaimDate OR
	target.PayorID <> source.PayorID OR
	target.ClaimAmount <> source.ClaimAmount OR
	target.PaidAmount <> source.PaidAmount OR
	target.ClaimStatus <> source.ClaimStatus OR
	target.PayorType <> source.PayorType OR
	target.Deductible <> source.Deductible OR
	target.Coinsurance <> source.Coinsurance OR
	target.Copay <> source.Copay OR
	target.hospital_source <> source.hospital_source OR
	target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
	target.modified_date = CURRENT_TIMESTAMP(),
	target.is_current = FALSE

WHEN NOT MATCHED
THEN
INSERT (
	ClaimKey,
	srcClaimID,
	TransactionID,
	PatientID, 
	EncounterID,
	ProviderID,
	DeptID,
	ServiceDate,
	ClaimDate,
	PayorID,
	ClaimAmount,
	PaidAmount, 
	ClaimStatus,
	PayorType,
	Deductible,
	Coinsurance,
	Copay,
	srcInsertDate,
	srcModifiedDate,
	hospital_source,
	is_quarantined,
	inserted_date,
	modified_date,
	is_current
)
VALUES(
	source.ClaimKey,
	source.srcClaimID,
	source.TransactionID,
	source.PatientID, 
	source.EncounterID,
	source.ProviderID,
	source.DeptID,
	source.ServiceDate,
	source.ClaimDate,
	source.PayorID,
	source.ClaimAmount,
	source.PaidAmount, 
	source.ClaimStatus,
	source.PayorType,
	source.Deductible,
	source.Coinsurance,
	source.Copay,
	source.srcInsertDate,
	source.srcModifiedDate,
	source.hospital_source,
	source.is_quarantined,
	CURRENT_TIMESTAMP(),
	NULL,
	TRUE
);
	
	
-- DROP Temporary pre-claims table after the MERGE

DROP TABLE project-b2d1202e-c674-4674-a8a.silver_dataset.pre_claims;



------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------	
------------------------------------------------------------------------------------------------------------------------	


-- CREATE silver table for cpt_codes

CREATE TABLE IF NOT EXISTS project-b2d1202e-c674-4674-a8a.silver_dataset.cptcodes(
	Procedure_Code_Category STRING,
	CPT_Codes STRING,
	Procedure_Code_Descriptions STRING,
	Code_Status STRING,
	is_quarantined BOOLEAN,
	inserted_date TIMESTAMP,
	modified_date TIMESTAMP,
	is_current BOOLEAN
);

-- Moving the data from bronze cpt_codes to silver pre-cpt_codes table

CREATE OR REPLACE TABLE project-b2d1202e-c674-4674-a8a.silver_dataset.pre_cpt_codes AS
SELECT
	Procedure_Code_Category,
	CPT_Codes,
	Procedure_Code_Descriptions,
	Code_Status,
	is_quarantined
FROM
(
	SELECT 
		*,
		CASE WHEN CPT_Codes is null or Code_Status is null then TRUE
		ELSE FALSE
		END AS is_quarantined
	FROM
		project-b2d1202e-c674-4674-a8a.bronze_dataset.cpt_codes
) AS temp;

-- Merging the pre-cpt_codes codes table to silver cpt_codes using SCD-2
MERGE INTO project-b2d1202e-c674-4674-a8a.silver_dataset.cptcodes AS target
USING project-b2d1202e-c674-4674-a8a.silver_dataset.pre_cpt_codes AS source
ON target.CPT_Codes = source.CPT_Codes
AND target.is_current = TRUE

WHEN MATCHED AND(
	target.Procedure_Code_Category <> source.Procedure_Code_Category OR
	target.Procedure_Code_Descriptions <> source.Procedure_Code_Descriptions OR 
	target.Code_Status <> source.Code_Status OR 
	target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
	target.modified_date = CURRENT_TIMESTAMP(),
	target.is_current = FALSE
	
WHEN NOT MATCHED THEN
INSERT (
	Procedure_Code_Category,
	CPT_Codes,
	Procedure_Code_Descriptions,
	Code_Status,
	is_quarantined,
	inserted_date,
	modified_date,
	is_current
)VALUES(
	source.Procedure_Code_Category,
	source.CPT_Codes,
	source.Procedure_Code_Descriptions,
	source.Code_Status,
	source.is_quarantined,
	CURRENT_TIMESTAMP(),
	NULL,
	TRUE
);

-- DROP temporary pre-cpt_codes table
DROP TABLE project-b2d1202e-c674-4674-a8a.silver_dataset.pre_cpt_codes;



