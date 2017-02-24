

/* 
USE EDW
GO 
==============================================================================
Author		: Rob Durkee
Create date	: 09/06/2011
Description	: Populate the EDW FactStockEvent table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number	

10/24/2011	Juan X. Jacome		-- Added code to drop temp Tables (DBA Code Review)
12/1/2011   Matt S              -- Updated the logic for IsFirstEvent & IsMostRecent 
								   to use ASAPUpdateDateTime instead of EventDateTime.
04/16/2012  Venkata Vempali     --Updated the logic for IsProviderReported	--R5.3 S13
04/18/2012  Venkata Vempali		--Updated the logic for IsProviderReported	--R5.3 S13
8/8/2012   Matt S              -- Updated the logic for IsFirstEvent & IsMostRecent to use temp table		
9/20/2012  Sidd Murao           -- Added Update User ID			   
==============================================================================
Usage Example:
Exec usp_ETLFactStockEvent
==============================================================================
*/

CREATE PROCEDURE [dbo].[usp_ETLFactStockEvent] 
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
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLFactStockEvent'

--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WHERE ETLJobName = 'DailyETLJob'

/*********************************************************************************************
Create the temp table StockEvent
*********************************************************************************************/

CREATE TABLE #TMP_StockEvent
(	[SalvageID] [int] NULL,
	[EventTypeID] [int] NOT NULL,
	[EventDayID] [int] NULL,
	[EventDateTime] [datetime] NULL,
	[StorageLocationID] [int] NULL,
	[TowBillNumber] [int] NULL,
	[EventCount] [tinyint] NULL,
	[IsFirstEvent] [tinyint] NULL,
	[IsMostRecentEvent] [tinyint] NULL,
	[IsProviderReported] [tinyint] NULL,
	[ASAPUpdateDateTime] [datetime] NULL,
	[UpdateUserID] [int] NULL)

	
/*********************************************************************************************
Create a temp table to store record count.
*********************************************************************************************/

CREATE TABLE #TMP_StockEventRecordCount(ChangeType VARCHAR(30)) 

/*********************************************************************************************
temp tables used for populating IsProviderReported
*********************************************************************************************/

CREATE TABLE #TMP_StockEvent_SID(
	Salvage_ID [int] NOT NULL)

CREATE TABLE #TMP_FSEN_StockEvent(
	Salvage_ID [int] NOT NULL,
    Settlement_DateTime [datetime] NULL)

CREATE TABLE #TMP_Void_DateTime(Salvage_ID [int] NOT NULL,Void_DateTime [datetime] NULL)
CREATE TABLE #EU(ASAPSalvageID INT, EventTypeID int, MinEventDateTime datetime, MaxEventDateTime datetime, CountEventID int)
/*********************************************************************************************
Selects data that has changed since the last ETL run using CDC, joins to tables in ASAP and EDW
and inserts into temp table
*********************************************************************************************/

INSERT INTO #TMP_StockEvent
(	   [SalvageID]
      ,[EventTypeID]
      ,[EventDayID]
      ,[EventDateTime]
      ,[StorageLocationID]
      ,[TowBillNumber]
      ,[EventCount]
      ,[IsFirstEvent]
      ,[IsMostRecentEvent]
	  ,[IsProviderReported]
	  ,[ASAPUpdateDateTime]
	  ,[UpdateUserID])
SELECT
	seh.Salvage_ID
	, dset.EventTypeID
	, ISNULL(CAST(CONVERT(VARCHAR(8),seh.Event_DateTime,112) as int),29991231)
	, ISNULL(seh.Event_DateTime,'2999-12-31')
	, ISNULL(seh.Storage_Location_ID,0)
	, ISNULL(seh.Towbill_Number,0)
	, 1
	, 1
	, 1
	, 1
	, Update_DateTime
	, Update_User_ID
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_History] (
     @StartLSN,@EndLSN,'all with merge') seh
INNER JOIN DimStockEventType dset with (NOLOCK)
ON seh.Salvage_Event_Code = dset.EventCode
INNER JOIN dbo.DimDay DD WITH (NOLOCK)
ON ISNULL(CAST(CONVERT(VARCHAR(8),seh.Event_DateTime,112) as int),29991231) = DD.DayID
WHERE [__$operation] = 5

SELECT @ExtractRowCount_sp = COUNT(*) FROM #TMP_StockEvent




INSERT #TMP_StockEvent_SID(Salvage_ID)
SELECT DISTINCT SalvageID FROM #TMP_StockEvent 

CREATE UNIQUE CLUSTERED INDEX IX_TMP_StockEvent_SID_Salvage_ID  ON #TMP_StockEvent_SID (Salvage_ID) 
	with fillfactor = 100; 	

-- Get the Max Void Datetime
INSERT INTO #TMP_Void_DateTime(Salvage_ID ,Void_DateTime)
SELECT fse.ASAPSalvageID, MAX(fse.EventDateTime) Void_DateTime
FROM [dbo].[FactStockEvent] fse with (NOLOCK)
	INNER JOIN #TMP_StockEvent_SID EU with (NOLOCK)
		ON fse.ASAPSalvageID = EU.Salvage_ID
WHERE fse.EventTypeID = (Select dset.EventTypeID FROM EDW.dbo.DimStockEventType dset WHERE dset.EventCode = 'STLV')
GROUP BY fse.ASAPSalvageID
--Insert default date when there is no Void.
INSERT INTO #TMP_Void_DateTime(Salvage_ID ,Void_DateTime)
SELECT Salvage_ID , '1900-01-01' AS Void_DateTime
FROM #TMP_StockEvent_SID
WHERE Salvage_ID NOT IN (SELECT Salvage_ID FROM #TMP_Void_DateTime)

CREATE UNIQUE CLUSTERED INDEX IX_TMP_Void_DateTime_Salvage_ID  ON #TMP_Void_DateTime (Salvage_ID) 
	with fillfactor = 100; 


--Insert the Max Settlement DateTime
INSERT #TMP_FSEN_StockEvent(Salvage_ID ,Settlement_DateTime)
SELECT fse.ASAPSalvageID, MAX(fse.EventDateTime) EventDateTime 
FROM [dbo].[FactStockEvent] fse with (NOLOCK)
	INNER JOIN #TMP_Void_DateTime AS VOD 
		ON fse.ASAPSalvageID = VOD.Salvage_ID
WHERE fse.EventTypeID = (Select dset.EventTypeID FROM EDW.dbo.DimStockEventType dset WHERE dset.EventCode = 'FSEN')
	AND  fse.EventDateTime > VOD.Void_DateTime
GROUP BY fse.ASAPSalvageID

CREATE UNIQUE CLUSTERED INDEX IX_TMP_FSEN_StockEventSalvage_ID  ON #TMP_FSEN_StockEvent (Salvage_ID) 
	with fillfactor = 100; 



/*********************************************************************************************
MERGE the data from the temp table into the EDW FactStockEvent table
This does Update and Insert only.  No Deletes
*********************************************************************************************/

BEGIN TRANSACTION 

	MERGE [dbo].[FactStockEvent] AS FSE
	USING #TMP_StockEvent AS TSE
		ON TSE.SalvageID = FSE.ASAPSalvageID
		and TSE.EventDateTime = FSE.EventDateTime
		and TSE.EventTypeID = FSE.EventTypeID
	WHEN MATCHED THEN 
	UPDATE SET 
	FSE.TowBillNumber = TSE.TowBillNumber
	, FSE.StorageLocationID = TSE.StorageLocationID
	, FSE.ETLLoadUpdateID = @ETLLoadID
	, FSE.ASAPUpdateDateTime = TSE.ASAPUpdateDateTime
	,FSE.UpdateUserID = TSE.UpdateUserID
	WHEN NOT MATCHED THEN
	INSERT (
	ASAPSalvageID
	,EventTypeID
	,EventDayID
	,EventDateTime
	,StorageLocationID
	,TowBillNumber
	,EventCount
	,IsFirstEvent
	,IsMostRecentEvent
	,IsProviderReported
	,ETLLoadID
	,ETLLoadUpdateID
	,ASAPUpdateDateTime
	,UpdateUserID)
	VALUES (
	tse.SalvageID
	,tse.EventTypeID
	,tse.EventDayID
	,tse.EventDateTime
	,tse.StorageLocationID
	,tse.TowBillNumber
	,tse.EventCount
	,tse.IsFirstEvent
	,tse.IsMostRecentEvent
	,tse.IsProviderReported
	,@ETLLoadID
	,@ETLLoadID
	,ASAPUpdateDateTime
	,UpdateUserID)

	OUTPUT $action INTO #TMP_StockEventRecordCount;
	  
/*********************************************************************************************
Get the counts from the MERGE operation
*********************************************************************************************/
SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #TMP_StockEventRecordCount

/*********************************************************************************************
Update IsMostRecentEvent indicator to 0 for stocks that have an earlier record with the 
same Event Type ID and the IsFirstEvent indicator to 0 for stocks that have a later record
with the same Event Type ID.
*********************************************************************************************/

INSERT INTO #EU(ASAPSalvageID, EventTypeID, MinEventDateTime, MaxEventDateTime, CountEventID)
SELECT fse.ASAPSalvageID, fse.EventTypeID, MIN(fse.ASAPUpdateDateTime) MinEventDateTime, MAX(fse.ASAPUpdateDateTime) MaxEventDateTime, COUNT(fse.EventID) CountEventID
FROM dbo.FactStockEvent fse with (NOLOCK)  
INNER JOIN (SELECT DISTINCT SalvageID,EventTypeID FROM #TMP_StockEvent WITH (NOLOCK)) tse
	ON tse.SalvageID = fse.ASAPSalvageID
		AND fse.EventTypeID = tse.EventTypeID
GROUP BY fse.ASAPSalvageID, fse.EventTypeID
HAVING COUNT(fse.EventID) > 1

Update [dbo].[FactStockEvent] 
SET IsFirstEvent = CASE WHEN fse.ASAPUpdateDateTime > eu.MinEventDateTime THEN 0 ELSE 1 END
	,IsMostRecentEvent = CASE WHEN fse.ASAPUpdateDateTime < eu.MaxEventDateTime THEN 0 ELSE 1 END
	,ETLLoadUpdateID = @ETLLoadID
FROM [dbo].[FactStockEvent] fse with (NOLOCK)
INNER JOIN #EU eu WITH (NOLOCK) 
ON fse.ASAPSalvageID = eu.ASAPSalvageID
AND fse.EventTypeID = eu.EventTypeID

/*********************************************************************************************
Update IsProviderReported indicator to 0 for all events for a stock with a Event DateTime
after the Final Settlement Ended - 'FSEN' - event
*********************************************************************************************/
UPDATE [dbo].[FactStockEvent] SET 
--SELECT 
	IsProviderReported = CASE WHEN FSE.EventDateTime > T.Settlement_DateTime THEN 0 ELSE 1 END
	,ETLLoadUpdateID = @ETLLoadID
FROM [dbo].[FactStockEvent] fse with (NOLOCK)
INNER JOIN (SELECT ID.Salvage_ID ,ISNULL(MAX(Settlement_DateTime),'2999-12-31') AS Settlement_DateTime
			FROM #TMP_StockEvent_SID ID
					LEFT OUTER JOIN #TMP_FSEN_StockEvent FSEN
						ON ID.Salvage_ID = FSEN.Salvage_ID
			Group By ID.Salvage_ID) T
ON FSE.ASAPSalvageID = T.Salvage_ID


/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		COMMIT TRAN
		
		IF OBJECT_ID('tempdb..#TMP_StockEvent') IS NOT NULL DROP TABLE #TMP_StockEvent
		IF OBJECT_ID('tempdb..#TMP_StockEventRecordCount') IS NOT NULL DROP TABLE #TMP_StockEventRecordCount
		IF OBJECT_ID('tempdb..#TMP_StockEvent_SID') IS NOT NULL DROP TABLE #TMP_StockEvent_SID
		IF OBJECT_ID('tempdb..#TMP_FSEN_StockEvent') IS NOT NULL DROP TABLE #TMP_FSEN_StockEvent
		IF OBJECT_ID('tempdb..#TMP_Void_DateTime') IS NOT NULL DROP TABLE #TMP_Void_DateTime
		
		
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
		
		IF OBJECT_ID('tempdb..#TMP_StockEvent') IS NOT NULL DROP TABLE #TMP_StockEvent
		IF OBJECT_ID('tempdb..#TMP_StockEventRecordCount') IS NOT NULL DROP TABLE #TMP_StockEventRecordCount
		IF OBJECT_ID('tempdb..#TMP_StockEvent_SID') IS NOT NULL DROP TABLE #TMP_StockEvent_SID
		IF OBJECT_ID('tempdb..#TMP_FSEN_StockEvent') IS NOT NULL DROP TABLE #TMP_FSEN_StockEvent
		IF OBJECT_ID('tempdb..#TMP_Void_DateTime') IS NOT NULL DROP TABLE #TMP_Void_DateTime
	
		RETURN -1

	END CATCH


	END

GO


