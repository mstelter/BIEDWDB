

/* 
USE EDW
GO 
======================================================================================
Author		: Nandini Manay
Create date	: 08/11/2011
Description	: Populate the EDW DimTowZone table from CDC	
======================================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number
08/19/2011  Nandini Manay		Added logic to get Country and MailCountryName 
09/29/2011  Preetham Duvva      Updated the script for nulls.
11/30/2011  Preetham Duvva      Added the column TowMileageBand as per the Story 4429
4/4/2013	Matt Stelter		Added Tow Zone Update at end
======================================================================================
Usage Example:
Exec usp_ETLDimTowZone
======================================================================================
*/

CREATE PROCEDURE [dbo].[usp_ETLDimTowZone]
AS
BEGIN

DECLARE @ETLLoadID        INT,
		@ExtractRowCount  INT,
		@InsertRowCount INT,
		@UpdateRowCount INT,
		@StartLSN BINARY(10),
		@EndLSN BINARY(10)

SET NOCOUNT ON;
BEGIN TRY

--Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'uspETLDimTowZone'

		
--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WHERE ETLJobName = 'DailyETLJob'

/*********************************************************************************************
Create the temp table and insert data from the CDC table into the temp table
*********************************************************************************************/


IF OBJECT_ID('tempdb..#TMP_Tow_Zone') IS NOT NULL DROP TABLE #TMP_Tow_Zone
CREATE TABLE #TMP_Tow_Zone
(
      [Tow_Zone_ID] [int] NULL,
      [Branch_Number] [int] NULL,
      [Tow_Zone_Number] [smallint] NULL,
      [Radius_Miles] [decimal](4,1) NULL,
      [Travel_Time_Minutes] [smallint] NULL,
      [Tow_Customer_Type_Code] [char](2) NULL,
      [Tow_Mileage_Band] [nvarchar](30) NULL,
)


IF OBJECT_ID('tempdb..#temp_TowZoneRecordCount') IS NOT NULL DROP TABLE #temp_TowZoneRecordCount
CREATE TABLE #temp_TowZoneRecordCount (ChangeType VARCHAR(10)) 

INSERT INTO #TMP_Tow_Zone
SELECT  [Tow_Zone_ID]
	   ,[Branch_Number]
       ,[Tow_Zone_Number]
       ,[Radius_Miles]
       ,[Travel_Time_Minutes]
       ,[Tow_Customer_Type_Code]
       ,dbo.fn_TowZoneMileageBand(Tow_Zone_ID)
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Tow_Zone] (
   @StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5	 


SELECT @ExtractRowCount = COUNT(*) FROM #TMP_Tow_Zone



/*********************************************************************************************
MERGE the data from the temp table into the EDW DimTowZone table
This does Update and Insert only.  No Deletes
*********************************************************************************************/


BEGIN TRANSACTION 

--Synchronize source data with target
	MERGE dbo.DimTowZone AS t
	USING #TMP_Tow_Zone AS s    
		ON t.TowZoneID = s.Tow_Zone_ID
  
	WHEN NOT MATCHED THEN 
	--Row exists in source but not in target
	INSERT (  TowZoneID
			 ,BranchNumber
			 ,RadiusMiles
			 ,TowCustomerTypeCode
			 ,TowZoneNumber
			 ,TravelTimeMinutes
			 ,TowMileageBand
			 ,ETLLoadID
			 ,ETLLoadUpdateID)
	VALUES ( s.[Tow_Zone_ID]
			,ISNULL(s.[Branch_Number],0)
			,ISNULL(s.[Radius_Miles],0)
			,ISNULL(s.[Tow_Customer_Type_Code],'')
			,ISNULL(s.[Tow_Zone_Number],0)
			,ISNULL(s.[Travel_Time_Minutes],0)
			,ISNULL(s.[Tow_Mileage_Band],'')
			,@ETLLoadID
			,@ETLLoadID)

	WHEN MATCHED THEN
      --Row exists and data is different
	UPDATE SET 
			 t.BranchNumber=ISNULL(s.[Branch_Number],0)
			,t.RadiusMiles = ISNULL(s.[Radius_Miles],0)
			,t.TowCustomerTypeCode=ISNULL(s.[Tow_Customer_Type_Code],'')
			,t.TowZoneNumber=ISNULL(s.[Tow_Zone_Number],0)
			,t.TravelTimeMinutes=ISNULL(s.[Travel_Time_Minutes],0)	
			,t.TowMileageBand=ISNULL(s.[Tow_Mileage_Band],'')
			,[ETLLoadUpdateID] = @ETLLoadID
	
 
OUTPUT $action INTO #temp_TowZoneRecordCount;

UPDATE dbo.DimTowZone SET 
			TowMileageBand=ISNULL(dbo.fn_TowZoneMileageBand(TowZoneID),'')
	WHERE [ETLLoadUpdateID] = @ETLLoadID

COMMIT TRAN

SELECT @InsertRowCount = COUNT(*)  FROM #temp_TowZoneRecordCount WHERE ChangeType = 'INSERT'
SELECT @UpdateRowCount = COUNT(*)  FROM #temp_TowZoneRecordCount WHERE ChangeType = 'UPDATE'

EXEC [dbo].[usp_ETLLoadAudit]
	@StartEnd = 'E',
	@ETLLoadID = @ETLLoadID,
	@ExtractRowCount = @ExtractRowCount,
	@InsertRowCount = @InsertRowCount,
    @UpdateRowCount = @UpdateRowCount

	RETURN 0
END TRY

BEGIN CATCH

	SET NOCOUNT OFF
	IF @@TRANCOUNT > 0 
		ROLLBACK TRAN
	
	EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID,
		@ExtractRowCount = @ExtractRowCount,
		@InsertRowCount = @InsertRowCount, 
		@UpdateRowCount = @UpdateRowCount,
		@ErrorCode = @@ERROR
		
		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

		RETURN -1
	

END CATCH

END


