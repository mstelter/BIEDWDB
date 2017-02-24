
/* 
USE EDW
GO 
==============================================================================
Author		: Venkata Vempali
Create date	: 08/12/2011
Description	: Populate the EDW DimStockEventType table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number	
8/17/2011	Venkata Vempali	    Changed to use CDC Net Change Function 
10/28/2011	Juan X. Jacome		Added code to drop temp Tables (DBA Code Review)

==============================================================================
Usage Example:
Exec usp_ETLDimStockEventType
==============================================================================
*/

CREATE PROCEDURE [dbo].[usp_ETLDimStockEventType]
AS     
BEGIN


DECLARE @ExtractRowCount_sp int
		,@ETLLoadID_sp int
		,@InsertRowCount_sp int
		,@UpdateRowCount_sp int
		,@MinLSN Binary(10)
		,@MaxLSN Binary (10)
		
SET NOCOUNT ON;
BEGIN TRY

--Run the Audit begin
EXEC @ETLLoadID_sp = [dbo].[usp_ETLLoadAudit] 
		@StartEnd = 'S',
		@PackageName = 'usp_ETLDimStockEventType'
		
--Get the Min and Max LSN's
SELECT @MinLSN = MinLSN, @MaxLSN = MaxLSN FROM dbo.ETLRun WHERE ETLJobName = 'DailyETLJob'


IF OBJECT_ID('tempdb..#TMP_Salvage_Event_Code') IS NOT NULL DROP TABLE #TMP_Salvage_Event_Code
CREATE TABLE #TMP_Salvage_Event_Code(Salvage_Event_Code CHAR(4),Salvage_Event_Description VARCHAR(60),Salvage_Event_Category_Code CHAR(3))

IF OBJECT_ID('tempdb..#TMP_Salvage_Event_Category') IS NOT NULL DROP TABLE #TMP_Salvage_Event_Category
CREATE TABLE #TMP_Salvage_Event_Category(Salvage_Event_Category_Code CHAR(3),Salvage_Event_Category_Description VARCHAR(40))

IF OBJECT_ID('tempdb..#temp_EventTypeRecordCount') IS NOT NULL DROP TABLE #temp_EventTypeRecordCount
CREATE TABLE #temp_EventTypeRecordCount (ChangeType VARCHAR(10)) 


/*********************************************************************************************
Create a temp table and Insert the values from the CDC Current Salvage Event Code table.
*********************************************************************************************/

INSERT INTO #TMP_Salvage_Event_Code(Salvage_Event_Code,Salvage_Event_Description,Salvage_Event_Category_Code) 	
SELECT Salvage_Event_Code,Salvage_Event_Description,Salvage_Event_Category_Code
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_Code] (
   @MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5	 

SELECT @ExtractRowCount_sp = COUNT(*) FROM #TMP_Salvage_Event_Code

/*********************************************************************************************
MERGE the data from the temp table into the EDW DimStockEventType table
This does Update and Insert only.  No Deletes
*********************************************************************************************/

BEGIN TRANSACTION  

	MERGE [dbo].[DimStockEventType] AS DST
	USING #TMP_Salvage_Event_Code AS SRC
		   ON SRC.Salvage_Event_Code = DST.EventCode
		 
	WHEN MATCHED THEN
	UPDATE SET 
		   DST.[EventDescription] = SRC.[Salvage_Event_Description]
		  ,DST.[EventCategoryCode] = ISNULL(SRC.[Salvage_Event_Category_Code],'')
		  ,DST.[EventCategory] = ''
		  ,ETLLoadUpdateID = @ETLLoadID_sp 
	     
	WHEN NOT MATCHED  THEN
	INSERT ([EventCode]
	      ,[EventCategory]
	      ,[EventDescription]
		  ,[EventCategoryCode]
		  ,[ETLLoadID],[ETLLoadUpdateID])
		  VALUES (ISNULL(Salvage_Event_Code,''),'',Salvage_Event_Description,Salvage_Event_Category_Code,@ETLLoadID_sp,@ETLLoadID_sp)

	OUTPUT $action INTO #temp_EventTypeRecordCount;
 

COMMIT TRAN

SELECT @InsertRowCount_sp = COUNT(*)  FROM #temp_EventTypeRecordCount WHERE ChangeType = 'INSERT'
SELECT @UpdateRowCount_sp = COUNT(*)  FROM #temp_EventTypeRecordCount WHERE ChangeType = 'UPDATE'


EXEC [dbo].[usp_ETLLoadAudit]
	@StartEnd = 'E',
	@ETLLoadID = @ETLLoadID_sp,
	@ExtractRowCount = @ExtractRowCount_sp,
	@InsertRowCount = @InsertRowCount_sp,
	@UpdateRowCount = @UpdateRowCount_sp
	



/*********************************************************************************************
To update DimStockEventType based on the Salvage Event Category
*********************************************************************************************/

EXEC @ETLLoadID_sp = [dbo].[usp_ETLLoadAudit] 
		@StartEnd = 'S',
		@PackageName = 'usp_ETLDimStockEventType'




/*********************************************************************************************
Create a temp table and Insert the values from the CDC Current Salvage Event Code table.
*********************************************************************************************/

INSERT INTO #TMP_Salvage_Event_Category(Salvage_Event_Category_Code,Salvage_Event_Category_Description) 	
SELECT Salvage_Event_Category_Code,Salvage_Event_Category_Description
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_Category] (
   @MinLSN,@MaxLSN,'all with merge') WHERE [__$operation] = 5	



SELECT @ExtractRowCount_sp = COUNT(*) FROM #TMP_Salvage_Event_Category	

BEGIN TRANSACTION  

	UPDATE DSET 
	SET 
	--SELECT 
		EventCategory = T.Salvage_Event_Category_Description 
		,ETLLoadUpdateID = @ETLLoadID_sp 
	FROM [dbo].[DimStockEventType] AS DSET
		INNER JOIN #TMP_Salvage_Event_Category AS T
			ON DSET.EventCategoryCode = T.Salvage_Event_Category_Code

COMMIT TRAN

SET  @UpdateRowCount_sp = @@ROWCOUNT 	
SET @InsertRowCount_sp = NULL

EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID_sp,
		@ExtractRowCount = @ExtractRowCount_sp,
		@UpdateRowCount = @UpdateRowCount_sp
		
		IF OBJECT_ID('tempdb..#TMP_Salvage_Event_Code') IS NOT NULL DROP TABLE #TMP_Salvage_Event_Code
		IF OBJECT_ID('tempdb..#TMP_Salvage_Event_Category') IS NOT NULL DROP TABLE #TMP_Salvage_Event_Category
		IF OBJECT_ID('tempdb..#temp_EventTypeRecordCount') IS NOT NULL DROP TABLE #temp_EventTypeRecordCount
	
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
		
		IF OBJECT_ID('tempdb..#TMP_Salvage_Event_Code') IS NOT NULL DROP TABLE #TMP_Salvage_Event_Code
		IF OBJECT_ID('tempdb..#TMP_Salvage_Event_Category') IS NOT NULL DROP TABLE #TMP_Salvage_Event_Category
		IF OBJECT_ID('tempdb..#temp_EventTypeRecordCount') IS NOT NULL DROP TABLE #temp_EventTypeRecordCount

		RETURN -1
	

END CATCH

END

