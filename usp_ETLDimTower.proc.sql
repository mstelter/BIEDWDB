/* 
USE EDW
GO 
==============================================================================
Author		: Juan X. Jacome
Create date	: 08/19/2011
Description	: Populate the EDW DimTower table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number
10/03/2011  svelupal			updated the proc as per TO BE Document	
==============================================================================
Usage Example:
Exec usp_ETLDimTower
==============================================================================
*/

CREATE PROCEDURE [dbo].[usp_ETLDimTower]
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

----Run the Audit begin
----Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'uspETLDimTower'

--Get the start and end LSNs

SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WHERE ETLJobName = 'DailyETLJob'

/*********************************************************************************************
Create the temp table Tower
*********************************************************************************************/


IF OBJECT_ID('tempdb..#TMP_Tower') IS NOT NULL DROP TABLE #TMP_Tower
CREATE TABLE #TMP_Tower
(	[Tower_ID] [int]NOT NULL,
	[Tower_Name] [varchar](60) NULL,
	[Tower_Type_Code] [char](3) NULL,
	[Address_Line_1] [varchar](40) NULL,
	[Address_Line_2] [varchar](40) NULL,
	[City_Name] [varchar](40) NULL,
	[State_Abbreviation] [char](3) NULL,
	[Zip_Code] [varchar](9) NULL,
	[Branch] [varchar](100) NULL,
	[Check_Limit_Amount] [decimal](11, 2) NULL,
	[Driver_Handles_Cash_Ind] [bit] NOT NULL,
	[Heavy_Duty_Towing_Ind] [bit] NOT NULL,	
	[Internal_Branch_Number] [int] NULL,
	[Phone_Extension_1] [nvarchar](8) NULL,
	[Phone_Extension_2] [nvarchar](8) NULL,
	[Phone_Number_1] [nvarchar](25) NULL,
	[Phone_Number_2] [nvarchar](25) NULL,
	[Fax_Number] [nvarchar](25) NULL,
	[Status_Code] [char](3) NULL,	
	[Tow_Contact_Method_Code] [char](2) NULL,
	[Email_Address] [nvarchar](60) NULL,
	[Federal_Tax_Id] [int] NULL,
	[Insurance_Policy_Number] [nvarchar](60) NULL,
	[Insurance_Carrier_Name] [nvarchar](60) NULL,
	[Insurance_Liability_Amount] [money] NULL,
	[Insurance_policy_Expiration_Date] [Datetime] NULL
)

IF OBJECT_ID('tempdb..#temp_TowerRecordCount') IS NOT NULL DROP TABLE #temp_TowerRecordCount
CREATE TABLE #temp_TowerRecordCount(ChangeType VARCHAR(30)) 

/*********************************************************************************************
Create a temp table and Insert the values from the CDC Current Tow_Type table.
*********************************************************************************************/



INSERT INTO #TMP_Tower
       ([Tower_ID],
		[Tower_Name],
		[Tower_Type_Code],
		[Address_Line_1],
		[Address_Line_2],
		[City_Name],
		[State_Abbreviation],
		[Zip_Code],
		[Branch],
		[Check_Limit_Amount],
		[Driver_Handles_Cash_Ind],
		[Heavy_Duty_Towing_Ind],	
		[Internal_Branch_Number],
		[Phone_Number_1],
		[Phone_Extension_1],
		[Fax_Number],
		[Status_Code],
		[Tow_Contact_Method_Code],
		[Email_Address],
	    [Federal_Tax_Id],
	    [Insurance_Policy_Number],
	    [Insurance_Carrier_Name],
	    [Insurance_Liability_Amount],
	    [Insurance_policy_Expiration_Date]		
		) 	
SELECT	[Tower_ID],
		[Tower_Name],
		[Tower_Type_Code],
		[Address_Line_1],
		[Address_Line_2],
		[City_Name],
		[State_Abbreviation],
		[Zip_Code],
		dbo.fn_Tower_Branches(Tower_ID),
		[Check_Limit_Amount],
		[Driver_Handles_Cash_Ind],
		[Heavy_Duty_Towing_Ind],	
		[Internal_Branch_Number],
		[Phone_Number_1],
		[Phone_Extension_1],
		[Fax_Number],
		[Status_Code],
		[Tow_Contact_Method_Code],
		[EMail_Address],
		[Federal_Tax_ID],
		[Insurance_Policy_Number],
		[Insurance_Carrier_Name],
		[Insurance_Liability_Amount],
		[Insurance_Policy_Expiration_Date]
						
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Tower](
     @StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5

--SELECT @ExtractRowCount_sp = COUNT(*) FROM #TMP_Tower

--select * from #TMP_Tower


/*********************************************************************************************
MERGE the data from the temp table into the EDW DimTowType table
This does Update and Insert only.  No Deletes
*********************************************************************************************/


BEGIN TRANSACTION 
  
    MERGE [dbo].[DimTower] AS DT
    USING #TMP_Tower AS T
          ON T.Tower_ID = DT.TowerID
    
    WHEN MATCHED THEN 
    UPDATE SET 
        TowerName = [Tower_Name],
		TowerTypeCode = [Tower_Type_Code],
		AddressLine1 = [Address_Line_1],
		AddressLine2 = [Address_Line_2],
		CityName = [City_Name],
		StateAbbreviation = [State_Abbreviation],
		ZipCode = [Zip_Code],
		Branches = [Branch],
		CheckLimitAmount = [Check_Limit_Amount],
		IsDriverHandlesCash = [Driver_Handles_Cash_Ind],
		IsHeavyDutyTowing = [Heavy_Duty_Towing_Ind],
		InternalBranchNumber = [Internal_Branch_Number],
		PhoneNumber1 = [Phone_Number_1],
		PhoneExtension1 = [Phone_Extension_1],
		FaxNumber = [Fax_Number],
		StatusCode = [Status_Code], 
		TowContactMethod = [Tow_Contact_Method_Code],
		EmailAddress = [EMail_Address],
		FederalTaxID = [Federal_Tax_ID],
		InsurancePolicyNumber = [Insurance_Policy_Number],
		InsuranceCarrierName = [Insurance_Carrier_Name],
		InsuranceLiabilityAmount = [Insurance_Liability_Amount],
		InsurancePolicyExpirationDate = [Insurance_Policy_Expiration_Date],
		ETLLoadUpdateID = @ETLLoadID	
   WHEN NOT MATCHED THEN
   INSERT(  TowerID,
			TowerName,
			TowerTypeCode,
			AddressLine1,
			AddressLine2,
			CityName,
			StateAbbreviation,
			ZipCode,
			Branches,
			CheckLimitAmount,
			IsDriverHandlesCash,
			IsHeavyDutyTowing,
			InternalBranchNumber,
			PhoneNumber1,
			PhoneExtension1,
			FaxNumber,
			StatusCode,
			TowContactMethod,
			EmailAddress,
			FederalTaxID,
			InsurancePolicyNumber,
			InsuranceCarrierName,
			InsuranceLiabilityAmount,
			InsurancePolicyExpirationDate,
			ETLLoadID,
			ETLLoadUpdateID
		)	 
	VALUES ([Tower_ID],
			[Tower_Name],
			[Tower_Type_Code],
			IsNULL([Address_Line_1],''),
			IsNULL([Address_Line_2],''),
			IsNUll([City_Name],''),
			[State_Abbreviation],
			[Zip_Code],
			[Branch],
			IsNULL([Check_Limit_Amount],0),
			IsNULL([Driver_Handles_Cash_Ind],0),
			IsNULL([Heavy_Duty_Towing_Ind],0),	
			[Internal_Branch_Number],
			[Phone_Number_1],
			[Phone_Extension_1],
			[Fax_Number],
			[Status_Code],
			[Tow_Contact_Method_Code],
			IsNull([EMail_Address],''),
		    IsNull([Federal_Tax_ID],''),
		    IsNULL([Insurance_Policy_Number],''),
		    IsNULL([Insurance_Carrier_Name],''),
		    IsNULL([Insurance_Liability_Amount],0),
		    IsNULL([Insurance_Policy_Expiration_Date],''),	
			@ETLLoadID,
			@ETLLoadID
			)
	     
	OUTPUT $action INTO #temp_TowerRecordCount;
	  
  /*********************************************************************************************
Get the counts from the MERGE operation
*********************************************************************************************/
SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #temp_TowerRecordCount

/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		COMMIT TRAN
		IF OBJECT_ID('tempdb..#TMP_Tower') IS NOT NULL DROP TABLE #TMP_Tower
		
		RETURN 0
	END TRY     

	BEGIN CATCH
		SET NOCOUNT OFF
		IF @@TRANCOUNT > 0 
			ROLLBACK TRAN
		
		IF OBJECT_ID('tempdb..#TMP_Tower') IS NOT NULL DROP TABLE #TMP_Tower	

		EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ErrorCode = @@ERROR, @UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

		RETURN -1
	END CATCH
	END
