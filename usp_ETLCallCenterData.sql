/* 
USE EDW
GO 
==============================================================================
Author		: Matt Stelter
Create date	: 03/19/2014
Description	: Populate the EDW CallCenterData table from Staging	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number	
9/16/2014	MSTELTER			Update date in staging	
==============================================================================
Usage Example:
Exec usp_ETLCallCenterData
==============================================================================
*/

CREATE PROCEDURE [dbo].[usp_ETLCallCenterData]
AS     
BEGIN


DECLARE @ExtractRowCount_sp int
		,@ETLLoadID_sp int
		,@InsertRowCount_sp int
		,@UpdateRowCount_sp int
		
SET NOCOUNT ON;
BEGIN TRY

--Run the Audit begin
EXEC @ETLLoadID_sp = [dbo].[usp_ETLLoadAudit] 
		@StartEnd = 'S',
		@PackageName = 'usp_ETLCallCenterData'

IF OBJECT_ID('tempdb..#temp_RecordCount') IS NOT NULL DROP TABLE #temp_RecordCount
CREATE TABLE #temp_RecordCount(ChangeType VARCHAR(30)) 
		
/*********************************************************************************************
MERGE the data from the temp table into the EDW DimTowType table
This does Update and Insert only.  No Deletes
*********************************************************************************************/
UPDATE [Staging].[dbo].[CallCenterData] SET [Weekof] = (SELECT CONVERT(DATE,(SELECT MAX(DayDate) AS DayDate from EDW.dbo.DimDay WHERE DayDate < GETDATE()-8 and DayOfWeek = 1)))

BEGIN TRANSACTION 
MERGE dbo.CallCenterData  AS CCD
USING [Staging].[dbo].[CallCenterData] as SRC
on CCD.[Weekof] = SRC.[Weekof] AND CCD.[Queue] = SRC.[Queue]
    WHEN MATCHED THEN 
UPDATE SET
CCD.[Queue] = SRC.[Queue]
      ,CCD.[CallsHandledLTSLA]=SRC.[CallsHandledLTSLA]
      ,CCD.[CallsAbandonedLTSLA]=SRC.[CallsAbandonedLTSLA]
      ,CCD.[CallsPresented]=SRC.[CallsPresented]
      ,CCD.[CallsHandled]=SRC.[CallsHandled]
      ,CCD.[CallsAbandoned]=SRC.[CallsAbandoned]
      ,CCD.[AvgSpdtoAns]=SRC.[AvgSpdtoAns]
      ,CCD.[AvgTalkTime]=SRC.[AvgTalkTime]
	  ,CCD.[SLAPcnt]=SRC.[SLAPcnt]
      ,CCD.[Weekof]=SRC.[Weekof]
   WHEN NOT MATCHED THEN
INSERT ([Queue]
      ,[CallsHandledLTSLA]
      ,[CallsAbandonedLTSLA]
      ,[CallsPresented]
      ,[CallsHandled]
      ,[CallsAbandoned]
      ,[AvgSpdtoAns]
      ,[AvgTalkTime]
	  ,[SLAPcnt]
      ,[Weekof])
VALUES ([Queue]
      ,[CallsHandledLTSLA]
      ,[CallsAbandonedLTSLA]
      ,[CallsPresented]
      ,[CallsHandled]
      ,[CallsAbandoned]
      ,[AvgSpdtoAns]
      ,[AvgTalkTime]
	  ,[SLAPcnt]
      ,[Weekof])

	  OUTPUT $action INTO #temp_RecordCount;
	  
  
  
  SELECT @InsertRowCount_sp = COUNT(*)  FROM #temp_RecordCount WHERE ChangeType = 'INSERT'
  SELECT @UpdateRowCount_sp = COUNT(*)  FROM #temp_RecordCount WHERE ChangeType = 'UPDATE'


  EXEC [dbo].[usp_ETLLoadAudit]
	   @StartEnd = 'E',
	   @ETLLoadID = @ETLLoadID_sp,
	   @ExtractRowCount = @ExtractRowCount_sp,
	   @InsertRowCount = @InsertRowCount_sp,
	   @UpdateRowCount = @UpdateRowCount_sp
	   
	   
COMMIT TRAN

    RETURN 0
END TRY

BEGIN CATCH

	SET NOCOUNT OFF
	IF @@TRANCOUNT > 0 
		ROLLBACK TRAN
	
	EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID_sp,
		@ExtractRowCount = @ExtractRowCount_sp,
		@InsertRowCount = @InsertRowCount_sp, 
		@UpdateRowCount = @UpdateRowCount_sp,
		@ErrorCode = @@ERROR
		
		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

		RETURN -1
	 
 END CATCH

END