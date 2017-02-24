/* 
USE EDW
GO 
==============================================================================
Author		: Matt Stelter
Create date	: 04/17/2015
Description	: Populate the EDW Salvage_Enhancement_Level table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number

==============================================================================
Usage Example:
Exec usp_ETLSalvageEnhancementLevelHistory
	SELECT * FROM EDW.dbo.SalvageEnhancementLevelHistory
==============================================================================
*/
CREATE PROCEDURE [dbo].usp_ETLSalvageEnhancementLevelHistory
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

EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLSalvageEnhancementLevelHistory'

--Get the start and end LSNs

SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WHERE ETLJobName = 'DailyETLJob'

BEGIN TRANSACTION 
  
INSERT INTO [dbo].[SalvageEnhancementLevelHistory]
           ([__$operation]
           ,[Salvage_Enhancement_Level_ID]
           ,[Salvage_ID]
           ,[Enhancement_Level_ID]
           ,[Update_DateTime]
           ,[Update_User_ID]) 	
SELECT	[__$operation]
		,[Salvage_Enhancement_Level_ID]
		,[Salvage_ID]
		,[Enhancement_Level_ID]
		,Update_DateTime
		,Update_User_ID
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Enhancement_Level](
     @StartLSN,@EndLSN,'all with merge') --WHERE [__$operation] = 5


--select * from [SalvageEnhancementLevelHistory]

/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		COMMIT TRAN
		
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

		RETURN -1
	END CATCH
	END