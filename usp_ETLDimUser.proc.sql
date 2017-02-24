/* 
USE EDW
GO 
==============================================================================
Author		: Nandini Manay
Create date	: 08/11/2011
Description	: Populate the EDW DimUser table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number	
8/18/2011	Nandini Manay	Changed to use CDC Net Change Function 
10/03/2111  Preetham Duvva  Updated the script for nulls
==============================================================================
Usage Example:
Exec usp_ETLDimUser
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLDimUser]

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
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimUser'


--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WHERE ETLJobName = 'DailyETLJob'

/*********************************************************************************************
Create the temp table and insert data from the CDC table into the temp table
*********************************************************************************************/

IF OBJECT_ID('tempdb..#TMP_User') IS NOT NULL DROP TABLE #TMP_User
CREATE TABLE #TMP_User(
       [User_ID] [INT] NOT NULL
      ,[User_Name] [varchar] (40) NULL
	  ,[EffectiveDayID] [int] NULL
      ,[Effective_Date] [datetime] NULL
      ,[ExpirationDayID] [int] NULL
      ,[Expiration_Date] [datetime] NULL
      ,[First_Name] [varchar] (30) NULL
      ,[Middle_Initial] [char] (1) NULL
      ,[Last_Name] [varchar] (30) NULL)
            

IF OBJECT_ID('tempdb..#temp_UserRecordCount') IS NOT NULL DROP TABLE #temp_UserRecordCount
CREATE TABLE #temp_UserRecordCount (ChangeType VARCHAR(10)) 

INSERT INTO #TMP_User
SELECT 
	 [User_ID]
	,[User_Name]
	,CONVERT(INT, CONVERT(CHAR(8), [Effective_Date], 112)) AS [EffectiveDayID]
	,[Effective_Date]
	,CONVERT(INT, CONVERT(CHAR(8), [Expiration_Date], 112)) AS [ExpirationDayID]
	,[Expiration_Date]
	,[First_Name]
	,[Middle_Initial]
	,[Last_Name]
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_User_Info] (
	@StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5

SELECT @ExtractRowCount = COUNT(*) FROM #TMP_User

/*********************************************************************************************
MERGE the data from the temp table into the EDW DimUser table
This does Update and Insert only.  No Deletes
*********************************************************************************************/


BEGIN TRANSACTION  

--Synchronize source data with target
	MERGE dbo.DimUser AS t
	USING #TMP_User AS s    
			ON t.[UserID] = s.[User_ID]
  

	WHEN MATCHED THEN
    --Row exists and data is different
	UPDATE SET 
	 t.UserName = ISNULL(s.[User_Name],'')
	,t.EffectiveDayID = ISNULL(CONVERT(INT, CONVERT(CHAR(8),s.[Effective_Date], 112)),29991231) 
	,t.EffectiveDateTime= ISNULL(s.[Effective_Date],'2999-12-31 00:00:00.000')
	,t.ExpirationDayID = ISNULL(CONVERT(INT, CONVERT(CHAR(8), [Expiration_Date], 112)),29991231)
	,t.ExpirationDateTime = ISNULL(s.[Expiration_Date],'2999-12-31 00:00:00.000')
	,t.FirstName = ISNULL(s.[First_Name],'')
	,t.MiddleInitial = ISNULL(s.[Middle_Initial],'')
	,t.LastName = ISNULL(s.[Last_Name],'')
	,t.[ETLLoadUpdateID] = @ETLLoadID

	WHEN NOT MATCHED THEN
	--Row exists in source but not in target
	INSERT(  [UserID]
		    ,[UserName]
		    ,[EffectiveDayID]
		    ,[EffectiveDateTime]
		    ,[ExpirationDayID]
		    ,[ExpirationDateTime]
		    ,[FirstName]
		    ,[MiddleInitial]
		    ,[LastName]
			,[ETLLoadID]
,[ETLLoadUpdateID])
	VALUES(  s.[User_ID]
			,ISNULL(s.[User_Name],'')
			,ISNULL(CONVERT(INT, CONVERT(CHAR(8),s.[Effective_Date], 112)),29991231)
			,ISNULL(s.[Effective_Date],'2999-12-31 00:00:00.000')
			,ISNULL(CONVERT(INT, CONVERT(CHAR(8), [Expiration_Date], 112)),29991231)
			,ISNULL(s.[Expiration_Date],'2999-12-31 00:00:00.000')
			,ISNULL(s.[First_Name],'')
			,ISNULL(s.[Middle_Initial],'')
			,ISNULL(s.[Last_Name],'')
			,@ETLLoadID
			,@ETLLoadID) 
    
OUTPUT $action INTO #temp_UserRecordCount;

COMMIT TRAN


SELECT @InsertRowCount = COUNT(*)  FROM #temp_UserRecordCount WHERE ChangeType = 'INSERT'
SELECT @UpdateRowCount = COUNT(*)  FROM #temp_UserRecordCount WHERE ChangeType = 'UPDATE'


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
