/* 
USE EDW
GO 
================================================================================================
Author		: Preetham Duvva
Create date	: 5/15/2012
Description	: Populate the EDW FactReleaseProblem table from CDC	
================================================================================================
REVISION History
ChangeDate	   Developer	
05/21/2012   Preetham Duvva  Added the logic for LastWorkedDate and LastWorkedDayID
05/23/2012   Preetham Duvva  Added the logic to update the ReleaseProblemCount in DimStock
05/25/2012   Preetham Duvva  Added the logic to update IsReleaseProblem and 
							 IsAllReleaseProblemsResolved indicators
05/29/2012   Preetham Duvva  Removed the logic for ReleaseProblemCount, IsReleaseProblem,
							 IsAllReleaseProblemsResolved and added them to usp_ETLDimStock_Update
================================================================================================
Usage Example:
Exec usp_ETLFactReleaseProblem
================================================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLFactReleaseProblem]
AS     
BEGIN

DECLARE @ExtractRowCount int
		,@ETLLoadID int
		,@InsertRowCount int
		,@UpdateRowCount int
		,@MinLSN Binary(10)
		,@MaxLSN Binary (10)
		,@MinAllowableDate	Datetime
		,@MaxAllowableDate	Datetime
		,@MinAllowableDayId	INT
		,@MaxAllowableDayID	INT  

		
				
SET NOCOUNT ON;

BEGIN TRY
         
--Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLFactReleaseProblem'

--Get the start and end LSNs
SELECT   @MinLSN = MinLSN, @MaxLSN = MaxLSN FROM dbo.ETLRun WITH (NOLOCK) WHERE ETLJobName = 'DailyETLJob'

--Validate the DateTime values
SELECT          @MinAllowableDate = MIN(DayDate)  
			   ,@MinAllowableDayId = (CONVERT(INT, CONVERT(CHAR(8), MIN(DayDate), 112)))
			   ,@MaxAllowableDate = MAX(DayDate)
			   ,@MaxAllowableDayID =  (CONVERT(INT, CONVERT(CHAR(8), MAX(DayDate), 112)))
FROM DimDay WITH (NOLOCK) WHERE DayID > 19000101 and DayID < 29991231

---Create #ReleaseProblem

IF OBJECT_ID('tempdb..#TMP_ReleaseProblem') IS NOT NULL DROP TABLE #TMP_ReleaseProblem
CREATE TABLE #TMP_ReleaseProblem
			( 
			  ReleaseProblemID INT NOT NULL,
			  StockID INT NULL,
			  ReleaseProblemStartDate Datetime NULL,
			  ReleaseProblemStartDayID INT NULL,
			  ReleaseProblemCode nchar(4) NULL,
			  ReleaseProblemDescription NVARCHAR(40) NULL,
			  ReleaseProblemLastWorkedDate Datetime NULL,
			  ReleaseProblemLastWorkedDayID INT NULL,
			  ReleaseProblemFollowUpDate Datetime NULL,
			  ReleaseProblemFollowUpDayID INT NULL,
			  ReleaseProblemEndDate Datetime NULL,
			  ReleaseProblemEndDayID INT NULL,
			  IsProblemResolved TINYINT NULL,
			)


IF OBJECT_ID('tempdb..#TMP_FactReleaseProblemRecordCount') IS NOT NULL DROP TABLE #TMP_FactReleaseProblemRecordCount
CREATE TABLE #TMP_FactReleaseProblemRecordCount(ChangeType VARCHAR(10)) 

INSERT INTO #TMP_ReleaseProblem
(
			   ReleaseProblemID
			   ,StockID
			   ,ReleaseProblemStartDate
			   ,ReleaseProblemStartDayID
			   ,ReleaseProblemCode
			   ,ReleaseProblemDescription
			   ,ReleaseProblemLastWorkedDate
			   ,ReleaseProblemLastWorkedDayID
			   ,ReleaseProblemFollowUpDate
			   ,ReleaseProblemFollowUpDayID
			   ,ReleaseProblemEndDate
			   ,ReleaseProblemEndDayID
			   ,IsProblemResolved
			  
   )
SELECT        0
              ,DS.StockID As StockID
              ,CASE WHEN Isnull(SRP.Release_Problem_DateTime, '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(SRP.Release_Problem_DateTime, '2999-12-31') ELSE '2999-12-31' END AS ReleaseProblemStartDate
              ,CASE WHEN Isnull(SRP.Release_Problem_DateTime, '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), SRP.Release_Problem_DateTime, 112)), 29991231) ELSE 29991231 END AS ReleaseProblemStartDayID
			  ,ISNULL(SRP.Release_Problem_Code,'') As ReleaseProblemCode
			  ,ISNULL(RP.Release_Problem_Description,'') As ReleaseProblemDescription
			  ,CASE WHEN Isnull(SRP.Last_Worked_DateTime, '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(SRP.Last_Worked_DateTime, '2999-12-31') ELSE '2999-12-31' END AS ReleaseProblemLastWorkedDate
              ,CASE WHEN Isnull(SRP.Last_Worked_DateTime, '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), SRP.Last_Worked_DateTime, 112)), 29991231) ELSE 29991231 END AS ReleaseProblemLastWorkedDayID
			  ,CASE WHEN Isnull(SRP.Follow_Up_DateTime, '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(SRP.Follow_Up_DateTime, '2999-12-31') ELSE '2999-12-31' END AS ReleaseProblemFollowUpDate
              ,CASE WHEN Isnull(SRP.Follow_Up_DateTime, '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), SRP.Follow_Up_DateTime, 112)), 29991231) ELSE 29991231 END AS ReleaseProblemFollowUpDayID
			  ,CASE WHEN Isnull(SRP.Problem_Resolved_DateTime, '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(SRP.Problem_Resolved_DateTime, '2999-12-31') ELSE '2999-12-31' END AS ReleaseProblemEndDate
              ,CASE WHEN Isnull(SRP.Problem_Resolved_DateTime, '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), SRP.Problem_Resolved_DateTime, 112)), 29991231) ELSE 29991231 END AS ReleaseProblemEndDayID
			  ,Problem_Resolved_Ind AS IsReleaseProblemResolved

FROM   [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Release_Problem](@MinLSN, @MaxLSN, 'all with merge') SRP
INNER JOIN BI_ASAP_Rep.dbo.Release_Problem RP On SRP.Release_Problem_Code = RP.Release_Problem_Code
INNER JOIN DimStock DS ON SRP.Salvage_ID = DS.ASAPSalvageID
where [__$operation] = 5

---Update the ReleaseProblemID
		
UPDATE TRP
SET ReleaseProblemID = FRP.ReleaseProblemID
FROM #TMP_ReleaseProblem TRP WITH (NOLOCK)
INNER JOIN dbo.FactReleaseProblem FRP WITH (NOLOCK)
	ON TRP.StockID = FRP.StockID
	AND TRP.ReleaseProblemStartDate = FRP.ReleaseProblemStartDate
	

---Update the LastWorkedDate and LastWorkedDayID
UPDATE TRP
SET
ReleaseProblemLastWorkedDate = CASE WHEN SRP.Problem_Resolved_Ind = 0 THEN ISNULL(SRP.Update_DateTime,'2999-12-31') ELSE '2999-12-31' END,
ReleaseProblemLastWorkedDayID = CASE WHEN SRP.Problem_Resolved_Ind = 0 THEN ISNULL(CONVERT(INT, CONVERT(CHAR(8), SRP.Update_DateTime, 112)), 29991231) ELSE 29991231 END
FROM #TMP_ReleaseProblem TRP WITH (NOLOCK)	
  Inner Join DimStock DS WITH (NOLOCK) On TRP.StockID = DS.StockID 
  Inner join [BI_ASAP_Rep].dbo.Salvage_Release_Problem SRP WITH (NOLOCK) On DS.ASAPSalvageID = SRP.Salvage_ID
and TRP.ReleaseProblemStartDate = SRP.Release_Problem_DateTime



SELECT @ExtractRowCount=COUNT(*) FROM #TMP_ReleaseProblem


/*********************************************************************************************
MERGE the data from the temp table into the EDW FactReleaseProblem table
This does Update and Insert only.  No Deletes
*********************************************************************************************/


BEGIN TRANSACTION 
--Synchronize source data with target
	MERGE dbo.FactReleaseProblem AS FRP
	USING #TMP_ReleaseProblem AS TRP    
	ON  FRP.ReleaseProblemID = TRP.ReleaseProblemID 
		
		
    	
	WHEN MATCHED THEN
	UPDATE SET
     ReleaseProblemLastWorkedDate = TRP.ReleaseProblemLastWorkedDate
    ,ReleaseProblemLastWorkedDayID = TRP.ReleaseProblemLastWorkedDayID
    ,ReleaseProblemFollowUpDate= TRP.ReleaseProblemFollowUpDate
    ,ReleaseProblemFollowUpDayID = TRP.ReleaseProblemFollowUpDayID
    ,ReleaseProblemEndDate = TRP.ReleaseProblemEndDate
    ,ReleaseProblemEndDayID = TRP.ReleaseProblemEndDayID
    ,IsProblemResolved = TRP.IsProblemResolved
    ,ETLLoadUpdateID = @ETLLoadID
    
    
	WHEN NOT MATCHED  THEN 
	INSERT (
	StockID
    ,ReleaseProblemStartDate
    ,ReleaseProblemStartDayID
    ,ReleaseProblemCode
    ,ReleaseProblemDescription
    ,ReleaseProblemLastWorkedDate
    ,ReleaseProblemLastWorkedDayID
    ,ReleaseProblemFollowUpDate
    ,ReleaseProblemFollowUpDayID
    ,ReleaseProblemEndDate
    ,ReleaseProblemEndDayID
    ,IsProblemResolved
    ,ETLLoadID
    ,ETLLoadUpdateID)
	
	Values(   
    StockID
    ,TRP.ReleaseProblemStartDate
    ,TRP.ReleaseProblemStartDayID
    ,TRP.ReleaseProblemCode
    ,TRP.ReleaseProblemDescription
    ,TRP.ReleaseProblemLastWorkedDate
    ,TRP.ReleaseProblemLastWorkedDayID
    ,TRP.ReleaseProblemFollowUpDate
    ,TRP.ReleaseProblemFollowUpDayID
    ,TRP.ReleaseProblemEndDate
    ,TRP.ReleaseProblemEndDayID
    ,IsProblemResolved
    ,@ETLLoadID
    ,@ETLLoadID
	)
  

OUTPUT $action INTO #TMP_FactReleaseProblemRecordCount;


SELECT @InsertRowCount = COUNT(*)  FROM #TMP_FactReleaseProblemRecordCount WHERE ChangeType = 'INSERT'
SELECT @UpdateRowCount = COUNT(*)  FROM #TMP_FactReleaseProblemRecordCount WHERE ChangeType = 'UPDATE'


	EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID,
		@ExtractRowCount = @ExtractRowCount,
		@InsertRowCount = @InsertRowCount,
		@UpdateRowCount = @UpdateRowCount
COMMIT TRAN
	
	IF OBJECT_ID('tempdb..#TMP_ReleaseProblem') IS NOT NULL DROP TABLE #TMP_ReleaseProblem
	IF OBJECT_ID('tempdb..#TMP_FactReleaseProblemRecordCount') IS NOT NULL DROP TABLE #TMP_FactReleaseProblemRecordCount


		
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
		
		IF OBJECT_ID('tempdb..#TMP_ReleaseProblem') IS NOT NULL DROP TABLE #TMP_ReleaseProblem
		IF OBJECT_ID('tempdb..#TMP_FactReleaseProblemRecordCount') IS NOT NULL DROP TABLE #TMP_FactReleaseProblemRecordCount
	


		RETURN -1
	

END CATCH

END







GO



