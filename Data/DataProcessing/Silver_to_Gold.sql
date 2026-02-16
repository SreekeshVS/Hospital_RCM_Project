--1. Total Charge Amount per provider by department

CREATE TABLE IF NOT EXISTS project-b2d1202e-c674-4674-a8a.gold_dataset.provider_charge_summary(
	wProviderID STRING,
	providerFullname STRING,
	departmentName STRING,
	Total_amount_charged FLOAT64
);


TRUNCATE TABLE project-b2d1202e-c674-4674-a8a.gold_dataset.provider_charge_summary;




INSERT INTO project-b2d1202e-c674-4674-a8a.gold_dataset.provider_charge_summary
WITH Provider_details AS (
	SELECT
		DISTINCT
		split(p.src_ProviderID, '-')[1] as wProviderID ,
		concat(p.FirstName, ', ', p.LastName) as providerFullname,
		d.Name as departmentName
	FROM	
		project-b2d1202e-c674-4674-a8a.silver_dataset.providers p
	LEFT JOIN
		project-b2d1202e-c674-4674-a8a.silver_dataset.departments d ON p.DeptID = d.srcDeptID
	WHERE 
		p.is_quarantined = FALSE
	AND
		d.Name is not NULL
)
SELECT
	p.wProviderID,
	p.providerFullname,
	p.departmentName,
	round(sum(t.Amount),2) as Total_amount_charged
FROM
	Provider_details p
LEFT JOIN 
	project-b2d1202e-c674-4674-a8a.silver_dataset.transactions t 
ON 
	p.wProviderID = t.ProviderID
WHERE 
	t.is_quarantined = FALSE
group by
	wProviderID, providerFullname, departmentName;


---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------


--2. Patient History (Gold) : This table provides a complete history of a patient’s visits, diagnoses, and financial interactions.

CREATE TABLE IF NOT EXISTS project-b2d1202e-c674-4674-a8a.gold_dataset.patient_history (
	PatientKey STRING,
	Patient_Fullname STRING,
	DOB DATE,
	Gender STRING,
	Address STRING,
	srcEncounterID STRING,
	InsertedDate DATE,
	EncounterDate DATE,
	EncounterType STRING,
	ProviderID STRING,
	srcTransactionID STRING,
	AmountBilled FLOAT64,
	PaidDate DATE,
	PaidAmount FLOAT64,
	srcClaimID STRING,
	ClaimDate DATE,
	ClaimStatus STRING,
	ClaimAmount FLOAT64,
	ClaimPaidAmount FLOAT64
);

TRUNCATE TABLE project-b2d1202e-c674-4674-a8a.gold_dataset.patient_history;


INSERT INTO project-b2d1202e-c674-4674-a8a.gold_dataset.patient_history
SELECT
	p.PatientKey,
	concat(p.FirstName, ', ', p.LastName) as Patient_Fullname,
	date(timestamp_millis(p.DOB)) as DOB,
	p.Gender,
	p.Address,
	e.srcEncounterID,
	date(timestamp_millis(e.InsertedDate)) as InsertedDate,
	date(timestamp_millis(e.EncounterDate)) as EncounterDate,
	e.EncounterType,
	e.ProviderID,
	t.srcTransactionID,
	t.Amount as AmountBilled,
	date(timestamp_millis(t.PaidDate)) as PaidDate,
	t.PaidAmount,
	c.srcClaimID,
	date(c.ClaimDate) as ClaimDate,
	c.ClaimStatus,
	cast(c.ClaimAmount as FLOAT64) as ClaimAmount,
	cast(c.PaidAmount as FLOAT64) as ClaimPaidAmount
FROM
	project-b2d1202e-c674-4674-a8a.silver_dataset.patients p 
LEFT JOIN
	project-b2d1202e-c674-4674-a8a.silver_dataset.encounters e ON p.srcPatientID = e.PatientID
LEFT JOIN
	project-b2d1202e-c674-4674-a8a.silver_dataset.transactions t ON p.srcPatientID = t.PatientID
LEFT JOIN
	project-b2d1202e-c674-4674-a8a.silver_dataset.claims c ON p.srcPatientID = c.PatientID



---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
-- 3. Provider Performance Summary (Gold) : This table summarizes provider activity, including the number of encounters, total billed amount, and claim success rate.

CREATE TABLE IF NOT EXISTS project-b2d1202e-c674-4674-a8a.gold_dataset.provider_performance_summary(
	wProviderID STRING,
	ProviderFullName STRING,
	DeptID STRING,
	Specialization STRING,
	num_encounters INTEGER,
	TotalBilledAmount FLOAT64,
	TotalPaidAmount FLOAT64,
	totalClaims INTEGER,
	ApprovedClaims INTEGER,
	Claims_Success_Rate FLOAT64
);

TRUNCATE TABLE project-b2d1202e-c674-4674-a8a.gold_dataset.provider_performance_summary;

INSERT INTO project-b2d1202e-c674-4674-a8a.gold_dataset.provider_performance_summary
WITH provider_details AS (
SELECT
	DISTINCT
	split(src_ProviderID, '-')[1] as wProviderID,
	concat(FirstName, ', ', LastName) as ProviderFullName,
	DeptID,
	Specialization
FROM
	project-b2d1202e-c674-4674-a8a.silver_dataset.providers p
), Claims_details AS (
	SELECT 
		p.wProviderID,
		p.ProviderFullName,
		p.DeptID,
		p.Specialization,
		count(distinct e.srcEncounterID) as num_encounters,
		round(SUM(T.Amount),2) as TotalBilledAmount,
		round(SUM(T.PaidAmount),2) as TotalPaidAmount,
		count(DISTINCT c.srcClaimID) as totalClaims,
		count(distinct case when lower(c.ClaimStatus) = 'approved' then c.srcClaimID else null end) as ApprovedClaims
	FROM	
		provider_details p
	LEFT JOIN
		project-b2d1202e-c674-4674-a8a.silver_dataset.encounters e ON p.wProviderID = e.ProviderID
	LEFT JOIN
		project-b2d1202e-c674-4674-a8a.silver_dataset.transactions t ON p.wProviderID = t.ProviderID
	LEFT JOIN
		project-b2d1202e-c674-4674-a8a.silver_dataset.claims c ON p.wProviderID = c.ProviderID
	GROUP BY
		p.wProviderID,
		p.ProviderFullName,
		p.DeptID,
		p.Specialization
)
SELECT 
	*,
	round((ApprovedClaims / totalClaims * 100), 2) as Claims_Success_Rate
FROM
	Claims_details
	
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------

-- 4. Department Performance Analytics (Gold): Provides insights into department-level efficiency, revenue, and patient volume.

CREATE TABLE IF NOT EXISTS project-b2d1202e-c674-4674-a8a.gold_dataset.department_performance_summary(
	srcDeptID STRING,
	Name STRING,
	num_encounters INTEGER,
	TotalBilledAmount FLOAT64,
	RevenueGenerated FLOAT64,
	AverageRevenuePerTransaction FLOAT64,
	PatientVolume INTEGER
);


TRUNCATE TABLE project-b2d1202e-c674-4674-a8a.gold_dataset.department_performance_summary;

INSERT INTO project-b2d1202e-c674-4674-a8a.gold_dataset.department_performance_summary
WITH Dept_details AS (
SELECT
	d.srcDeptID,
	d.Name,
  count(DISTINCT e.srcEncounterID) as num_encounters,
FROM 
	project-b2d1202e-c674-4674-a8a.silver_dataset.departments d
LEFT JOIN
  project-b2d1202e-c674-4674-a8a.silver_dataset.encounters e ON d.srcDeptID = e.DepartmentID
GROUP BY 
  d.srcDeptID,
  d.Name
)
SELECT 
  d.srcDeptID,
  d.Name,
  d.num_encounters,
  round(sum(t.Amount),2) as TotalBilledAmount,
  round(sum(t.PaidAmount),2) as RevenueGenerated,
  round(avg(t.PaidAmount),2) as AverageRevenuePerTransaction,
  count(distinct t.PatientID) as PatientVolume
FROM 
  Dept_details d
LEFT JOIN
  project-b2d1202e-c674-4674-a8a.silver_dataset.transactions t ON d.srcDeptID = t.DeptID
GROUP BY
  d.srcDeptID,
  d.Name,
  d.num_encounters;




---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------

-- 5. Financial Metrics (Gold) : Aggregates financial KPIs, such as total revenue, claim success rate, and outstanding balances.

CREATE TABLE IF NOT EXISTS project-b2d1202e-c674-4674-a8a.gold_dataset.Financial_Metrics(
	TotalTransactions INTEGER,
	TotalAmountBilled FLOAT64,
	TotalAmountReceived FLOAT64,
	TotalClaims INTEGER,
	Approved_Claims INTEGER,
	Outstanding_Balance FLOAT64,
	Claims_Ratio FLOAT64
);



TRUNCATE TABLE project-b2d1202e-c674-4674-a8a.gold_dataset.Financial_Metrics;


INSERT INTO project-b2d1202e-c674-4674-a8a.gold_dataset.Financial_Metrics
WITH Revenue_details AS(
SELECT
  count(distinct srcTransactionID) as TotalTransactions,
	round(SUM(t.Amount),2) as TotalAmountBilled,
	round(SUM(t.PaidAmount),2) as TotalAmountReceived,
	count(distinct c.srcClaimID) as TotalClaims,
	count(distinct case when c.ClaimStatus = 'Approved' then c.srcClaimID else null end) as Approved_Claims,
FROM
	project-b2d1202e-c674-4674-a8a.silver_dataset.transactions t 
LEFT JOIN
	project-b2d1202e-c674-4674-a8a.silver_dataset.claims c ON t.srcTransactionID = c.TransactionID
)
select 
  *,
  round(TotalAmountBilled - TotalAmountReceived,2) as Outstanding_Balance,
  round(Approved_Claims / TotalClaims * 100, 2) as Claims_Ratio
FROM
	Revenue_details
	


---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------


--5. Payor Performance & Claims Summary (Gold): This table tracks the performance of insurance payors, focusing on claim approval rates, payout amounts, and processing efficiency.

CREATE TABLE IF NOT EXISTS project-b2d1202e-c674-4674-a8a.gold_dataset.Payor_Claims_Summary(
	PayorID STRING,
	TotalClaims INTEGER,
	ApprovedClaims INTEGER,
	DeniedClaims INTEGER,
	PendingClaims INTEGER,
	TotalPayout FLOAT64,
	Claim_Ratio FLOAT64
);

TRUNCATE TABLE project-b2d1202e-c674-4674-a8a.gold_dataset.Payor_Claims_Summary;
 
 
INSERT INTO project-b2d1202e-c674-4674-a8a.gold_dataset.Payor_Claims_Summary
WITH Claims_details AS(
	SELECT
		PayorID,
		count(distinct srcClaimID) as TotalClaims,
		count(distinct case when ClaimStatus = 'Approved' then srcClaimID else null end) as ApprovedClaims,
		count(distinct case when ClaimStatus = 'Denied' then srcClaimID else null end) as DeniedClaims,
		count(distinct case when ClaimStatus = 'Pending' then srcClaimID else null end) as PendingClaims,
		round(sum(cast(PaidAmount as FLOAT64)), 2) as TotalPayout
	FROM
		project-b2d1202e-c674-4674-a8a.silver_dataset.claims
  GROUP BY
    PayorID
)
SELECT
	*,
	round(ApprovedClaims / TotalClaims * 100, 2) as Claim_Ratio
FROM 
	Claims_details;
	
	
	


---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------

