/* 
USE EDW
GO 
================================================================================================
Author		: Matt Stelter
Create date	: 7/14/2014
Description	: Populate the EDW FactTitleProblem table from CDC	
================================================================================================
REVISION History
ChangeDate	   Developer	
2/12/2016		Matt Stelter	Add [Note_Text]
11/13/2016      Pratyusha Koduru  Added logic to remove all title problems when title received date is removed
================================================================================================
Usage Example:
Exec usp_ETLFactTitleProblem
================================================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLFactTitleProblem]
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
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLFactTitleProblem'

--Get the start and end LSNs
SELECT   @MinLSN = MinLSN, @MaxLSN = MaxLSN FROM dbo.ETLRun WITH (NOLOCK) WHERE ETLJobName = 'DailyETLJob'

--Validate the DateTime values
SELECT          @MinAllowableDate = MIN(DayDate)  
			   ,@MinAllowableDayId = (CONVERT(INT, CONVERT(CHAR(8), MIN(DayDate), 112)))
			   ,@MaxAllowableDate = MAX(DayDate)
			   ,@MaxAllowableDayID =  (CONVERT(INT, CONVERT(CHAR(8), MAX(DayDate), 112)))
FROM DimDay WITH (NOLOCK) WHERE DayID > 19000101 and DayID < 29991231

---Create #TitleProblem

IF OBJECT_ID('tempdb..#TMP_TitleProblem') IS NOT NULL DROP TABLE #TMP_TitleProblem
CREATE TABLE #TMP_TitleProblem
			(Salvage_Title_Problem_ID INT NULL,
			[StockID] [int] NOT NULL,
	[TitleProblemStartDate] [datetime] NULL,
	[TitleProblemStartDayID] [int] NULL,
	[TitleProblemDescription] [varchar](100) NULL,
	[TitleProblemLastWorkedDate] [datetime] NULL,
	[TitleProblemLastWorkedDayID] [int] NULL,
	[TitleProblemFollowUpDate] [datetime] NULL,
	[TitleProblemFollowUpDayID] [int] NULL,
	[TitleProblemEndDate] [datetime] NULL,
	[TitleProblemEndDayID] [int] NULL,
	[IsTitleProblemResolved] [tinyint] NOT NULL,
	[ETLLoadID] [int] NOT NULL,
	[ETLLoadUpdateID] [int] NOT NULL,
	[TitleProblemSolution] [varchar](100) NULL,
	[ASAPTitleProblemID] [int] NULL,
	[Note_Text] VARCHAR (1000) )


IF OBJECT_ID('tempdb..#TMP_FactTitleProblemRecordCount') IS NOT NULL DROP TABLE #TMP_FactTitleProblemRecordCount
CREATE TABLE #TMP_FactTitleProblemRecordCount(ChangeType VARCHAR(10)) 

INSERT INTO #TMP_TitleProblem (Salvage_Title_Problem_ID
			,[StockID]
           ,[TitleProblemStartDate]
           ,[TitleProblemStartDayID]
           ,[TitleProblemDescription]
           ,[TitleProblemLastWorkedDate]
           ,[TitleProblemLastWorkedDayID]
           ,[TitleProblemFollowUpDate]
           ,[TitleProblemFollowUpDayID]
           ,[TitleProblemEndDate]
           ,[TitleProblemEndDayID]
           ,[IsTitleProblemResolved]
           ,[ETLLoadID]
           ,[ETLLoadUpdateID]
           ,[TitleProblemSolution]
           ,[ASAPTitleProblemID]
		   ,[Note_Text])
SELECT
  STP.Salvage_Title_Problem_ID,
	DS.StockID,
	STP.Title_Problem_Date,
	CAST(CONVERT(CHAR(8),STP.Title_Problem_Date,112) AS INT),
	TP.[Title_Problem_Description],
	ISNULL(STP.Title_Problem_Date,'12/31/2999') AS TitleProblemLastWorkedDate,-- STP.Update_DateTime,
	CAST(CONVERT(CHAR(8),ISNULL(STP.Title_Problem_Date,'12/31/2999'),112) AS INT) AS TitleProblemLastWorkedDayID, --CAST(CONVERT(CHAR(8),STP.Update_DateTime,112) AS INT),
	STP.Follow_Up_Date,
	CAST(CONVERT(CHAR(8),STP.Follow_Up_Date,112) AS INT),
	STP.Resolved_DateTime,
	CAST(CONVERT(CHAR(8),STP.Resolved_DateTime,112) AS INT),
	Case WHEN STP.Resolved_DateTime IS NULL THEN 0 ELSE 1 END AS IsTitleProblemResolved,
	1,
	1,
	TS.[Title_Solution_Description], 
	TP.Title_Problem_ID,
	'' AS Note_Text
	--, * 
 --FROM BI_ASAP_REP.[dbo].[Salvage_Title_Problem] STP WITH (NOLOCK)
 FROM   [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Title_Problem](@MinLSN, @MaxLSN, 'all with merge') STP
INNER JOIN EDW.dbo.DimStock DS WITH (NOLOCK)
	ON STP.Salvage_ID = DS.ASAPSalvageID
INNER JOIN  BI_ASAP_REP.[dbo].[Title_Problem] TP  WITH (NOLOCK)
	ON STP.[Title_Problem_ID] = TP.[Title_Problem_ID]
LEFT OUTER JOIN [BI_ASAP_Rep].[dbo].[Title_Solution] TS  WITH (NOLOCK)
	ON TP.Title_Solution_ID = TS.Title_Solution_ID
where [__$operation] = 5

---Update the TitleProblemQuickNote
		
UPDATE FTP SET Note_Text = Note
--SELECT * 
FROM #TMP_TitleProblem FTP
inner join (SELECT Salvage_Title_Problem_ID,
					   LEFT(MAX( CASE RowNo WHEN 1 THEN Note_Text ELSE '' END ) +
					   MAX( CASE RowNo WHEN 2 THEN '; ' + Note_Text ELSE '' END ) +
					   MAX( CASE RowNo WHEN 3 THEN '; ' + Note_Text ELSE '' END ) +
					   MAX( CASE RowNo WHEN 4 THEN '; ' + Note_Text ELSE '' END ),1000) AS Note
			FROM (SELECT [Salvage_Title_Problem_ID]
							,Note_Text
							,ROW_NUMBER() OVER ( PARTITION BY [Salvage_Title_Problem_ID] ORDER BY SN.Update_DateTime DESC ) AS RowNo
							,SN.Update_DateTime
					  FROM BI_ASAP_REP.[dbo].[Salvage_Title_Problem_History] STPH WITH (NOLOCK)
					  INNER JOIN BI_ASAP_REP.[dbo].[Salvage_Note] SN WITH (NOLOCK)
						ON STPH.Salvage_Note_ID = SN.Salvage_Note_ID
					  --WHERE STPH.Salvage_ID = 16393296
					  ) SN
			GROUP BY Salvage_Title_Problem_ID)  TP
ON FTP.Salvage_Title_Problem_ID = TP.Salvage_Title_Problem_ID

---Update the LastWorkedDate and LastWorkedDayID
UPDATE TRP
SET
TitleProblemLastWorkedDate = CASE WHEN IsTitleProblemResolved = 0 THEN ISNULL(SRP.Update_DateTime,'2999-12-31') ELSE TitleProblemLastWorkedDate END,
TitleProblemLastWorkedDayID = CASE WHEN IsTitleProblemResolved = 0 THEN ISNULL(CONVERT(INT, CONVERT(CHAR(8), SRP.Update_DateTime, 112)), 29991231) ELSE TitleProblemLastWorkedDayID END
FROM #TMP_TitleProblem TRP WITH (NOLOCK)	
  Inner join [BI_ASAP_Rep].dbo.Salvage_Title_Problem SRP WITH (NOLOCK) 
  On TRP.Salvage_Title_Problem_ID = SRP.Salvage_Title_Problem_ID

 SELECT @ExtractRowCount=COUNT(*) FROM #TMP_TitleProblem


/*********************************************************************************************
MERGE the data from the temp table into the EDW FactTitleProblem table
This does Update and Insert only.  No Deletes
*********************************************************************************************/


BEGIN TRANSACTION 
--Synchronize source data with target
	MERGE dbo.FactTitleProblem AS FRP
	USING #TMP_TitleProblem AS TRP    
	ON  FRP.ASAPSalvageTitleProblemID = TRP.Salvage_Title_Problem_ID 
		
		
    	
	WHEN MATCHED THEN
	UPDATE SET
     TitleProblemLastWorkedDate = TRP.TitleProblemLastWorkedDate
    ,TitleProblemLastWorkedDayID = TRP.TitleProblemLastWorkedDayID
    ,TitleProblemFollowUpDate= TRP.TitleProblemFollowUpDate
    ,TitleProblemFollowUpDayID = TRP.TitleProblemFollowUpDayID
    ,TitleProblemEndDate = TRP.TitleProblemEndDate
    ,TitleProblemEndDayID = TRP.TitleProblemEndDayID
    ,IsTitleProblemResolved = TRP.IsTitleProblemResolved
    ,ETLLoadUpdateID = @ETLLoadID
	,QuickNoteText = TRP.[Note_Text]
    
    
	WHEN NOT MATCHED  THEN 
	INSERT (
	StockID
    ,TitleProblemStartDate
    ,TitleProblemStartDayID
    ,TitleProblemDescription
    ,TitleProblemLastWorkedDate
    ,TitleProblemLastWorkedDayID
    ,TitleProblemFollowUpDate
    ,TitleProblemFollowUpDayID
    ,TitleProblemEndDate
    ,TitleProblemEndDayID
    ,IsTitleProblemResolved
    ,ETLLoadID
    ,ETLLoadUpdateID
	,[TitleProblemSolution]
	,ASAPTitleProblemID
	,ASAPSalvageTitleProblemID
	,QuickNoteText)
	
	Values(   
    StockID
    ,TRP.TitleProblemStartDate
    ,TRP.TitleProblemStartDayID
    ,TRP.TitleProblemDescription
    ,TRP.TitleProblemLastWorkedDate
    ,TRP.TitleProblemLastWorkedDayID
    ,TRP.TitleProblemFollowUpDate
    ,TRP.TitleProblemFollowUpDayID
    ,TRP.TitleProblemEndDate
    ,TRP.TitleProblemEndDayID
    ,IsTitleProblemResolved
    ,@ETLLoadID
    ,@ETLLoadID
	,[TitleProblemSolution]
	,ASAPTitleProblemID	
	,Salvage_Title_Problem_ID
	,TRP.[Note_Text])

OUTPUT $action INTO #TMP_FactTitleProblemRecordCount;


SELECT @InsertRowCount = COUNT(*)  FROM #TMP_FactTitleProblemRecordCount WHERE ChangeType = 'INSERT'
SELECT @UpdateRowCount = COUNT(*)  FROM #TMP_FactTitleProblemRecordCount WHERE ChangeType = 'UPDATE'

--================================================================================
--========================== Update TitleProblems based on TitleReceivedRemoved===
--================================================================================

  SELECT TP.StockID,Max(SN.Update_DateTime) AS TitleReceivedRemovedDateTime
  INTO #TMP_TitleRcvdRmvd
  FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Note](@MinLSN, @MaxLSN, 'all with merge') SN
  INNER JOIN EDW.dbo.DimStock AS DS WITH (NOLOCK)
	ON DS.ASAPSalvageID = SN.Salvage_ID
  INNER JOIN EDW.dbo.FactTitleProblem AS TP
    ON TP.StockID = DS.StockID
  WHERE [__$operation] = 5
  AND SN.Note_Text like '%All title problems are removed because the Title Received Date is removed%'
  Group BY TP.StockId

   SELECT 
        CASE WHEN TitleReceivedRemovedDateTime  > TitleProblemStartDate THEN 1 ELSE 0 END AS TPDeletedFlag,
        TitleReceivedRemovedDateTime,tp.*
   INTO #TMP_DeleteRecord
   FROM EDW.dbo.FactTitleProblem AS TP WITH (NOLOCK)
   LEFT OUTER JOIN #TMP_TitleRcvdRmvd AS TR WITH (NOLOCK)
   ON TP.StockID = TR.StockID
   ORDER BY TitleProblemStartDate

  DELETE FROM EDW.dbo.FactTitleProblem 
  WHERE TitleProblemId in (SELECT TitleProblemId FROM #TMP_DeleteRecord WHERE TPDeletedFlag = 1)
  --================================================================================
  --================================================================================


	EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID,
		@ExtractRowCount = @ExtractRowCount,
		@InsertRowCount = @InsertRowCount,
		@UpdateRowCount = @UpdateRowCount
COMMIT TRAN
	
	IF OBJECT_ID('tempdb..#TMP_TitleProblem') IS NOT NULL DROP TABLE #TMP_TitleProblem
	IF OBJECT_ID('tempdb..#TMP_FactTitleProblemRecordCount') IS NOT NULL DROP TABLE #TMP_FactTitleProblemRecordCount
	IF OBJECT_ID('tempdb..#TMP_TitleRcvdRmvd') IS NOT NULL DROP TABLE #TMP_TitleRcvdRmvd
	IF OBJECT_ID('tempdb..#TMP_DeleteRecord') IS NOT NULL DROP TABLE #TMP_DeleteRecord


		
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
		
		IF OBJECT_ID('tempdb..#TMP_TitleProblem') IS NOT NULL DROP TABLE #TMP_TitleProblem
		IF OBJECT_ID('tempdb..#TMP_FactTitleProblemRecordCount') IS NOT NULL DROP TABLE #TMP_FactTitleProblemRecordCount
		IF OBJECT_ID('tempdb..#TMP_TitleRcvdRmvd') IS NOT NULL DROP TABLE #TMP_TitleRcvdRmvd
	    IF OBJECT_ID('tempdb..#TMP_DeleteRecord') IS NOT NULL DROP TABLE #TMP_DeleteRecord
	


		RETURN -1
	

END CATCH

END

GO