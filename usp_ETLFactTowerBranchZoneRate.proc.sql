

/* 
USE EDW
GO 
===================================================================================================
Author		: Rob Durkee
Create date	: 08/29/2011
Description	: Populate the EDW FactTowerBranchZoneRate table from CDC	
===================================================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number	
10/24/2011	Juan X. Jacome		-- Added code to drop temp Tables (DBA Code Review)
12/1/2011   Preetham Duvva      Removed the columns TowMileageBand and TowRadius Miles as per the 
                                Story 4429
12/15/2011 Juan X. Jacome		-- Added code nolock(DBA Code Review)
4/12/2012  Juan X. Jacome		-- DBA Code Review removed dup code to insert [Usp_etlloadaudit]

===================================================================================================
Usage Example:
Exec usp_ETLFactTowerBranchZoneRate
===================================================================================================
*/

CREATE PROCEDURE [dbo].[usp_ETLFactTowerBranchZoneRate]
AS     
BEGIN


DECLARE @ETLLoadID INT,
		@ExtractRowCount_sp INT,
		@InsertRowCount INT,
		@UpdateRowCount INT,
		@DeleteRowCount INT,
		@StartLSN BINARY(10),
		@EndLSN BINARY(10)
		
SET NOCOUNT ON;
BEGIN TRY


--Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLFactTowerBranchZoneRate'

--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WHERE ETLJobName = 'DailyETLJob'

/*********************************************************************************************
Create the temp table TowerZoneRate
*********************************************************************************************/
CREATE TABLE #TMP_TowerZoneRate
(	[Tower_IAA_Branch_Zone_Rate_ID] [int] NOT NULL,
	[Branch_Number] [int] NULL,
	[Buyer_ID] [int] NULL,
	[FinancialTransactionTypeID] [int] NULL,
	[Salvage_Provider_ID] [int] NULL,
	[Tower_ID] [int] NULL,
	[Tow_Zone_Number] [smallint] NULL,
	[Additional_Fee_Per_Mile] [money] NULL,
	[Buyer_Override_IND] [tinyint] NOT NULL,
	[Contract_Effective_Date_ID] [int] NULL,
	[Contract_Effective_Date] [datetime] NULL,
	[Contract_Type_Code] [nchar](3) NULL,
	[Dry_Run_Amt] [float] NULL,
	[Dry_Run_Calculation_Method] [nchar](3) NULL,
	[Fuel_Surcharge_Amt] [float] NULL,
	[Fuel_Surcharge_Calculation_Method] [nchar](3) NULL,
	[Pickup_Time_Limit_Hours] [smallint] NULL,
	[Rate_Amount] [money] NULL,
	[Salvage_Duty_Type_Code] [nchar](3) NULL,
	[Stock_Type] [nchar](5) NULL,
	[Tow_Customer_Type_Code] [nchar](2) NULL,
	[Tower_Override_IND] [tinyint] NOT NULL,
	[Zip_Code_Override_IND] [tinyint] NOT NULL,

)
/*********************************************************************************************
Create a temp table to store record count.
*********************************************************************************************/

CREATE TABLE #TMP_TowerZoneRateRecordCount(ChangeType VARCHAR(30)) 

/*********************************************************************************************
Selects data that has changed since the last ETL run using CDC, joins to tables in ASAP and EDW
and inserts into temp table
*********************************************************************************************/

INSERT INTO #TMP_TowerZoneRate
    (Tower_IAA_Branch_Zone_Rate_ID,
	Branch_Number,
	Buyer_ID,
	FinancialTransactionTypeID,
	Salvage_Provider_ID,
	Tower_ID,
	Tow_Zone_Number,
	Additional_Fee_Per_Mile,
	Buyer_Override_Ind,
	Contract_Effective_Date_ID,
	Contract_Effective_Date,
	Contract_Type_Code,
	Dry_Run_Amt,
	Dry_Run_Calculation_Method,
	Fuel_Surcharge_Amt,
	Fuel_Surcharge_Calculation_Method,
	Pickup_Time_Limit_Hours,
	Rate_Amount,
	Salvage_Duty_Type_Code,
	Stock_Type,
	Tow_Customer_Type_Code,
	Tower_Override_Ind,
	Zip_Code_Override_IND
) 	
SELECT	tzr.Tower_IAA_Branch_Zone_Rate_ID,
	ISNULL(tzr.Branch_Number,0),
	ISNULL(tzr.Buyer_ID,0),
	ISNULL(st.FinancialTransactionTypeID,0),	
	ISNULL(tzr.Salvage_Provider_ID,0),	
	ISNULL(tzr.Tower_ID,0),
	ISNULL(tzr.Tow_Zone_Number,0),
	tzr.Additional_Fee_Per_Mile,
	CASE WHEN tzr.Buyer_ID IS NOT NULL THEN 1 ELSE 0 END,
	ISNULL(CAST(CONVERT(VARCHAR(8),tzr.Contract_Effective_Date,112) as int),29991231),
	ISNULL(tzr.Contract_Effective_Date,'12/31/2999'),
	ISNULL(tzr.Contract_Type_Code,''),		
	drfs.Dry_Run,
	ISNULL(drfs.Dry_Run_Calculation_Method,''),
	drfs.Fuel_Surcharge,
	ISNULL(drfs.Fuel_Surcharge_Calculation_Method,''),	
	tzr.Pickup_Time_Limit_Hours,
	tzr.Rate_Amount,
	ISNULL(tzr.Salvage_Duty_Type_Code,''),
	ISNULL(tzr.Salvage_Type,''),
	ISNULL(tzr.Tow_Customer_Type_Code,''),
	CASE WHEN tzr.Tower_ID IS NOT NULL THEN 1 ELSE 0 END as Tower_Override_Ind,
	CASE WHEN TZOR.Tow_Zone_ID IS NULL THEN 0 ELSE 1 END AS Zip_Code_Override_IND 
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Tower_IAA_Branch_Zone_Rate] (
     @StartLSN,@EndLSN,'all with merge') tzr
left outer join dbo.DimFinancialTransactionType st WITH (NOLOCK)
on FinancialTransactionTypeID=Charge_Type_ID
LEFT OUTER JOIN
	(SELECT	Tower_IAA_Branch_Zone_Rate_ID, 
		MAX(CASE WHEN Charge_Type_ID = 37 THEN Charge_Amount ELSE 0 END) AS Dry_Run,
		MAX(CASE WHEN Charge_Type_ID = 37 THEN Charge_Method_Code ELSE '' END) AS Dry_Run_Calculation_Method,
		MAX(CASE WHEN Charge_Type_ID = 460 THEN Charge_Amount ELSE 0 END) AS Fuel_Surcharge,
		MAX(CASE WHEN Charge_Type_ID = 460 THEN Charge_Method_Code ELSE '' END) AS Fuel_Surcharge_Calculation_Method
	FROM [BI_ASAP_Rep].dbo.Tower_IAA_Branch_Zone_By_ChargeType bzct WITH (NOLOCK)
	WHERE Charge_Type_ID IN (37,460) and Charge_Amount <> 0
	GROUP BY Tower_IAA_Branch_Zone_Rate_ID) drfs
ON tzr.Tower_IAA_Branch_Zone_Rate_ID = drfs.Tower_IAA_Branch_Zone_Rate_ID
LEFT OUTER JOIN [BI_ASAP_Rep].dbo.Tow_Zone tz WITH (NOLOCK)
ON tz.Branch_Number = tzr.Branch_Number
AND tz.Tow_Zone_Number = tzr.Tow_Zone_Number
AND tz.Tow_Customer_Type_Code = tzr.Tow_Customer_Type_Code
LEFT OUTER JOIN (SELECT DISTINCT Tow_Zone_ID FROM [BI_ASAP_Rep].dbo.Tow_Zone_Override WITH (NOLOCK)) TZOR 
ON tz.Tow_Zone_ID = TZOR.Tow_Zone_ID
WHERE [__$operation] = 5

SELECT @ExtractRowCount_sp = COUNT(*) FROM #TMP_TowerZoneRate



/*********************************************************************************************
MERGE the data from the temp table into the EDW DimTowType table
This does Update and Insert only.  No Deletes
*********************************************************************************************/

BEGIN TRANSACTION 
  
    MERGE [dbo].[FactTowerBranchZoneRate] AS TZ
    USING #TMP_TowerZoneRate AS T
          ON T.Tower_IAA_Branch_Zone_Rate_ID = TZ.TowerBranchZoneRateID
    WHEN MATCHED THEN 
    UPDATE SET 
	tz.BranchNumber = t.Branch_Number,
	tz.BuyerID = t.Buyer_ID,
	tz.FinancialTransactionTypeID = t.FinancialTransactionTypeID,
	tz.ProviderID = t.Salvage_Provider_ID,
	tz.TowerID = t.Tower_ID,
	tz.TowZoneNumber = t.Tow_Zone_Number,
	tz.AdditionalFeePerMile = t.Additional_Fee_Per_Mile,
	tz.IsBuyerOverride = Buyer_Override_Ind,
	tz.ContractEffectiveDayID = t.Contract_Effective_Date_ID,
	tz.ContractEffectiveDateTime = t.Contract_Effective_Date,
	tz.ContractTypeCode = t.Contract_Type_Code,
	tz.DryRunAmt = t.Dry_Run_Amt,
	tz.DryRunCalculationMethod = t.Dry_Run_Calculation_Method,
	tz.FuelSurchargeAmt = t.Fuel_Surcharge_Amt,
	tz.FuelSurchargeCalculationMethod = t.Fuel_Surcharge_Calculation_Method,
	tz.PickupTimeLimitHours = t.Pickup_Time_Limit_Hours,
	tz.RateAmount = t.Rate_Amount,
	tz.SalvageDutyTypeCode = t.Salvage_Duty_Type_Code,
	tz.StockType = t.Stock_Type,
	tz.TowCustomerTypeCode = t.Tow_Customer_Type_Code,
	tz.IsTowerOverride = t.Tower_Override_Ind,
	tz.IsZipCodeOverride = t.Zip_Code_Override_IND,
	tz.ETLLoadUpdateID = @ETLLoadID	
   WHEN NOT MATCHED THEN
   INSERT( TowerBranchZoneRateID,
	BranchNumber,
	BuyerID,
	FinancialTransactionTypeID,
	ProviderID,
	TowerID,
	TowZoneNumber,
	AdditionalFeePerMile,
	IsBuyerOverride,
	ContractEffectiveDayID,
	ContractEffectiveDateTime,
	ContractTypeCode,
	DryRunAmt,
	DryRunCalculationMethod,
	FuelSurchargeAmt,
	FuelSurchargeCalculationMethod,
	PickupTimeLimitHours,
	RateAmount,
	SalvageDutyTypeCode,
	StockType,
	TowCustomerTypeCode,
	IsTowerOverride,
	IsZipCodeOverride,
	ETLLoadID,
	ETLLoadUpdateID		
		)	 
	VALUES (Tower_IAA_Branch_Zone_Rate_ID,
	Branch_Number,
	Buyer_ID,
	FinancialTransactionTypeID,
	Salvage_Provider_ID,
	Tower_ID,
	Tow_Zone_Number,
	Additional_Fee_Per_Mile,
	Buyer_Override_Ind,
	Contract_Effective_Date_ID,
	Contract_Effective_Date,
	Contract_Type_Code,
	Dry_Run_Amt,
	Dry_Run_Calculation_Method,
	Fuel_Surcharge_Amt,
	Fuel_Surcharge_Calculation_Method,
	Pickup_Time_Limit_Hours,
	Rate_Amount,
	Salvage_Duty_Type_Code,
	Stock_Type,
	Tow_Customer_Type_Code,
	Tower_Override_Ind,
	Zip_Code_Override_IND,
	@ETLLoadID,
	@ETLLoadID)
	     
	OUTPUT $action INTO #TMP_TowerZoneRateRecordCount;
	  


  /*********************************************************************************************
Get the counts from the MERGE operation
*********************************************************************************************/
SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #TMP_TowerZoneRateRecordCount

/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		COMMIT TRAN
		
		IF OBJECT_ID('tempdb..#TMP_TowerZoneRate') IS NOT NULL DROP TABLE #TMP_TowerZoneRate
		IF OBJECT_ID('tempdb..#TMP_TowerZoneRateRecordCount') IS NOT NULL DROP TABLE #TMP_TowerZoneRateRecordCount
	
		RETURN 0
	END TRY     

	BEGIN CATCH
		SET NOCOUNT OFF
		IF @@TRANCOUNT > 0 
			ROLLBACK TRAN

		EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ErrorCode = @@ERROR, @UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

		IF OBJECT_ID('tempdb..#TMP_TowerZoneRate') IS NOT NULL DROP TABLE #TMP_TowerZoneRate
		IF OBJECT_ID('tempdb..#TMP_TowerZoneRateRecordCount') IS NOT NULL DROP TABLE #TMP_TowerZoneRateRecordCount

		RETURN -1

	END CATCH

	
	END


