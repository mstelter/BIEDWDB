/* 
USE EDW
GO 
==============================================================================
Author		: Matt Stelter
Create date	: 5/21/2014
Description	: Populate the EDW DimLiveRepDetail table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer		Release/Problem Number	
5/27/2014	Matt Stelter	Fixed On the Block
8/25/2015	Juan X Jacome	Update Join to use BuyerID for Insert to dbo.DimLiveRepDetail
==============================================================================
Usage Example:
Exec usp_ETLDimLiveRepDetail
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLDimLiveRepDetail]
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
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimLiveRepDetail'

--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WITH (NOLOCK) WHERE ETLJobName = 'DailyETLJob'

/*********************************************************************************************
Create the temp table to consolidate the Current and Static Storage Locations
*********************************************************************************************/

IF OBJECT_ID('tempdb..#TMPDimLiveRepDetail') IS NOT NULL DROP TABLE #TMPDimLiveRepDetail
CREATE TABLE #TMPDimLiveRepDetail(
	[AuctionItemID] [int] NULL,
	[BuyerId] [int] NULL,
	[IsOnTheBlock] [tinyint] NULL,
	[SalvageEventCode] [char](4) NOT NULL,
	[MinBidAmount] [money] NULL,
	[BidAmount] [money] NULL,
	[CounterBidAmount] [money] NULL,
	[ActivityDescription] [varchar](200) NULL,
	[BidStatusCode] [char](3) NULL,
	[ASAPUpdateDateTime] [datetime] NULL,
	[ASAPUpdateUserId] [int] NULL,
	[ASAPUpdateUserName] [varchar](40) NULL)

INSERT INTO #TMPDimLiveRepDetail
           ([AuctionItemID]
           ,[BuyerId]
           ,[IsOnTheBlock]
           ,[SalvageEventCode]
           ,[MinBidAmount]
           ,[BidAmount]
           ,[CounterBidAmount]
           ,[ActivityDescription]
           ,[BidStatusCode]
           ,[ASAPUpdateDateTime]
           ,[ASAPUpdateUserId]
           ,[ASAPUpdateUserName])
  SELECT SLR.[AuctionItemId]
      ,SLR.[BuyerId]
	  ,0 AS IsOnTheBlock
      ,SLR.[SalvageEventCode]
      ,SLR.[MinBidAmount]
      ,SLR.[BidAmount]
      ,SLR.[CounterBidAmount]
      ,SLR.[ActivityDescription]
      ,SLR.[BidStatusCode]
      ,SLR.[UpdateDateTime]
      ,SLR.[UpdateUserId]
	  ,DU.UserName
  FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_SalvageLiveRepQualifiedHistory] (
   @StartLSN,@EndLSN,'all with merge') SLR 
  INNER JOIN dbo.DimUser DU WITH (NOLOCK)
  ON SLR.[UpdateUserId] = DU.UserID
 WHERE [__$operation] = 5
 
SELECT @ExtractRowCount = COUNT(*) FROM #TMPDimLiveRepDetail

/*********************************************************************************************
MERGE the data from the temp table into the EDW DimUser table
This does Update and Insert only.  No Deletes
*********************************************************************************************/

IF OBJECT_ID('tempdb..#TMPDimLiveRepDetailCount') IS NOT NULL DROP TABLE #TMPDimLiveRepDetailCount
CREATE TABLE #TMPDimLiveRepDetailCount(ChangeType VARCHAR(10)) 

BEGIN TRANSACTION 
--Synchronize source data with target
	MERGE dbo.DimLiveRepDetail AS t
	USING #TMPDimLiveRepDetail AS s    
		ON t.[AuctionItemID] = s.[AuctionItemID] AND t.[ASAPUpdateDateTime] = s.[ASAPUpdateDateTime] 

WHEN MATCHED  THEN
      --Row exists and data is different
	UPDATE SET 
t.[AuctionItemID]=ISNULL(s.[AuctionItemID],0)
,t.[BuyerId]=ISNULL(s.[BuyerId],0)
,t.[IsOnTheBlock]=ISNULL(s.[IsOnTheBlock],0)
,t.[SalvageEventCode]=ISNULL(s.[SalvageEventCode],'')
,t.[MinBidAmount]=ISNULL(s.[MinBidAmount],0)
,t.[BidAmount]=ISNULL(s.[BidAmount],0)
,t.[CounterBidAmount]=ISNULL(s.[CounterBidAmount],0)
,t.[ActivityDescription]=ISNULL(s.[ActivityDescription],'')
,t.[BidStatusCode]=ISNULL(s.[BidStatusCode],'')
,t.[ASAPUpdateDateTime]=ISNULL(s.[ASAPUpdateDateTime],'12/31/2999')
,t.[ASAPUpdateUserId]=ISNULL(s.[ASAPUpdateUserId],0)
,t.[ASAPUpdateUserName]=ISNULL(s.[ASAPUpdateUserName],'')
	WHEN NOT MATCHED  THEN 
	--Row exists in source but not in target
	INSERT ([AuctionItemID]
           ,[BuyerId]
           ,[IsOnTheBlock]
           ,[SalvageEventCode]
           ,[MinBidAmount]
           ,[BidAmount]
           ,[CounterBidAmount]
           ,[ActivityDescription]
           ,[BidStatusCode]
           ,[ASAPUpdateDateTime]
           ,[ASAPUpdateUserId]
           ,[ASAPUpdateUserName])
	VALUES (ISNULL(s.[AuctionItemID],0)
			,ISNULL(s.[BuyerId],0)
			,ISNULL(s.[IsOnTheBlock],0)
			,ISNULL(s.[SalvageEventCode],'')
			,ISNULL(s.[MinBidAmount],0)
			,ISNULL(s.[BidAmount],0)
			,ISNULL(s.[CounterBidAmount],0)
			,ISNULL(s.[ActivityDescription],'')
			,ISNULL(s.[BidStatusCode],'')
			,ISNULL(s.[ASAPUpdateDateTime],'12/31/2999')
			,ISNULL(s.[ASAPUpdateUserId],0)
			,ISNULL(s.[ASAPUpdateUserName],''))
  
OUTPUT $action INTO #TMPDimLiveRepDetailCount;

INSERT INTO dbo.DimLiveRepDetail([AuctionItemID]
           ,[BuyerId]
           ,[IsOnTheBlock]
           ,[SalvageEventCode]
           ,[MinBidAmount]
           ,[BidAmount]
           ,[CounterBidAmount]
           ,[ActivityDescription]
           ,[BidStatusCode]
           ,[ASAPUpdateDateTime]
           ,[ASAPUpdateUserId]
           ,[ASAPUpdateUserName])
SELECT
DBD.[AuctionItemID]
,COALESCE(PROP.BuyerID,DAI.BuyerID) AS BuyerID  --We will have to get the last BuyerID prior with accepted bid
,1 as IsOnTheBlock
,DBD.[BidStatusCode] AS SalvageEventCode
,DS.MinimumBidAmount AS MinBidAmount
,PROP.[BidAmount]
,CASE WHEN BidStatusDescription = 'Accept offer' THEN 0 ELSE DBD.[BidAmount] END AS CounterBidAmount --We will have to get the last accepted bid prior to counter bid or the counter bid prior to buyer counter bid
,DBD.BidStatusDescription as ActivityDescription
,DBD.[BidStatusCode]
,DBD.BidEventDateTime as ASAPUpdateDatetime
,0 AS ASAPUpdateUserID
,DisplayName as ASAPUpdateUserNAme
  FROM [EDW].[dbo].[DimBidDetail] DBD WITH (NOLOCK) 
  INNER JOIN dbo.DimAuctionITem DAI WITH (NOLOCK)
	ON DBD.[AuctionItemID] = DAI.[AuctionItemID]
  INNER JOIN dbo.DimStock DS WITH (NOLOCK)
	ON DS.StockID = DAI.StockID
LEFT OUTER JOIN (SELECT DISTINCT DBD.AuctionItemID,DBD.BidAmount,DBD.BuyerID, COBidEventDateTime,MAXBidEventDateTime
				 FROM [EDW].[dbo].[DimBidDetail] DBD WITH (NOLOCK) 
				 INNER JOIN (SELECT DBD.AuctionItemID, DBD.BuyerID, MAX(DBD.BidEventDateTime) AS  MAXBidEventDateTime, MAX(CO.BidEventDateTime) AS COBidEventDateTime
							 FROM [EDW].[dbo].[DimBidDetail] DBD WITH (NOLOCK) 
							 INNER JOIN (SELECT AuctionItemID,BidEventDateTime, BuyerID  
										 FROM [EDW].[dbo].[DimBidDetail] DBD WITH (NOLOCK) 
										 where BidStatusCode = 'E' and GroupNumber = 1) CO
								ON DBD.AuctionItemID = CO.AuctionItemID 
								AND DBD.BidEventDateTime < CO.BidEventDatetime 
								AND CONVERT(DATE,DBD.BidEventDateTime) = CONVERT(DATE,CO.BidEventDatetime)
								AND DBD.BuyerID = CO.BuyerID
							WHERE DBD.BidStatusCode = 'K'
							GROUP BY DBD.AuctionItemID, DBD.BuyerID) MaxK
					ON DBD.AuctionItemID = MaxK.AuctionItemID 
					AND DBD.BidEventDatetime = MaxK.MAXBidEventDateTime
					AND DBD.BuyerID = MaxK.BuyerID) PROP
	ON DBD.AuctionItemID = PROP.AuctionItemID AND DBD.BidEventDatetime = PROP.COBidEventDateTime
LEFT OUTER JOIN dbo.DimLiveRepDetail DLRD WITH (NOLOCK)
	ON  DLRD.[AuctionItemID] = DAI.[AuctionItemID] 
	AND DBD.BidEventDateTime = ASAPUpdateDatetime
	AND DBD.BuyerID = DLRD.BuyerId 
WHERE DBD.BidStatusCode = 'E' and GroupNumber = 1 AND DBD.BidEventDatetime > GETDATE() - 3 AND DLRD.[DimLiveRepDetailID] IS NULL

COMMIT TRAN

SELECT @InsertRowCount = COUNT(*)  FROM #TMPDimLiveRepDetailCount WHERE ChangeType = 'INSERT'
SELECT @UpdateRowCount = COUNT(*)  FROM #TMPDimLiveRepDetailCount WHERE ChangeType = 'UPDATE'

SET  @UpdateRowCount = @@ROWCOUNT 	
SET @InsertRowCount = NULL

EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID,
		@ExtractRowCount = @ExtractRowCount,
		@UpdateRowCount = @UpdateRowCount
		
		IF OBJECT_ID('tempdb..#TMPDimLiveRepDetailCount') IS NOT NULL DROP TABLE #TMPDimLiveRepDetailCount
		IF OBJECT_ID('tempdb..#TMPDimLiveRepDetail') IS NOT NULL DROP TABLE #TMPDimLiveRepDetail

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
		
		IF OBJECT_ID('tempdb..#TMPDimLiveRepDetailCount') IS NOT NULL DROP TABLE #TMPDimLiveRepDetailCount
		IF OBJECT_ID('tempdb..#TMPDimLiveRepDetail') IS NOT NULL DROP TABLE #TMPDimLiveRepDetail

		RETURN -1
	

END CATCH

END