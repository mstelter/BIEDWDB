
/* 
USE EDW
GO 
==============================================================================
Author		: Venkata Vempali
Create date	: 08/11/2011
Description	: Populate the EDW dbo.FactPreBidAuctionItem table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer		 Release/Problem Number	
1/8/2013	MStelter		Changed CDC for Auction
1/14/2013   Venkata Vempali  Added condition to aviod Userid with 0 or NULL.
1/14/2012   Venkata Vempali  Added function fn_TimeZoneConvert to convert the timezone for BuyerPreBidAuctionDateTime
10/27/2016  MAtt Stelter	 Changed to use entire Auction_Schedule table instead of CDC to prevent errors

==============================================================================
Usage Example:
Exec usp_ETLFactPreBidAuctionItem
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLFactPreBidAuctionItem]
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
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLFactPreBidAuctionItem'

--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WITH (NOLOCK) WHERE ETLJobName = 'DailyETLJob_BI_Auction_Rep'

/*********************************************************************************************
Create the temp table to consolidate the Current and Static Storage Locations
*********************************************************************************************/

IF OBJECT_ID('tempdb..#TMP_PreBidAuctions') IS NOT NULL DROP TABLE #TMP_PreBidAuctions
CREATE TABLE #TMP_PreBidAuctions(
	AuctionId   INT,
	ItemId		INT,
	Closedate	DATETIME,
	CloseDate_DayId	INT)
INSERT INTO #TMP_PreBidAuctions(AuctionId ,ItemId ,Closedate ,CloseDate_DayId)
SELECT	AuctionId,
		ItemId,
		Closedate,
		CAST(CONVERT(CHAR(8),Closedate,112) AS INT) AS CloseDate_DayId
FROM 
[BI_Auction_Rep].dbo.Auctions
--[BI_Auction_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Auctions] (
--	@StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5	 



IF OBJECT_ID('tempdb..#TMP_PreBidAuctionsItems') IS NOT NULL DROP TABLE #TMP_PreBidAuctionsItems
CREATE TABLE #TMP_PreBidAuctionsItems(
			AuctionId INT,
			ItemId INT,
			Salvage_ID INT,
			UserID INT,	
			OpenDate DATETIME,
			OpenDayId INT,
			CloseDate DATETIME,
			CloseDayId INT,
			MinAmount MONEY,	
			MaxAmountPlaced MONEY,	
			PreBidPlacedDate DATETIME,
			PreBidPlacedDayId INT,
			WinningPreBidAmount MONEY,
			LiveDate DATETIME,
			AuctionLiveDate_DayID INT,
			MaxAmount MONEY)
INSERT INTO #TMP_PreBidAuctionsItems(
			Auctions.AuctionId,
			ItemId,
			Salvage_ID,
			UserID,	
			OpenDate,
			OpenDayId,
			CloseDate,
			CloseDayId,
			MinAmount,	
			MaxAmountPlaced,	
			PreBidPlacedDate,
			PreBidPlacedDayId,
			WinningPreBidAmount,
			LiveDate,
			AuctionLiveDate_DayID,
			MaxAmount)
SELECT	Auctions.AuctionId,
		Items.ItemId,
		Items.Salvage_ID,
		Bids.UserID,	
		Auctions.OpenDate,
		CAST(CONVERT(CHAR(8),Auctions.OpenDate,112) AS INT) AS OpenDayId,
		Auctions.CloseDate,
		CAST(CONVERT(CHAR(8),Auctions.CloseDate,112) AS INT) AS CloseDayId,
		ISNULL(min(Bids.Amount), 0) AS MinAmount,	
		ISNULL(max(Bids.Amount), 0) AS MaxAmountPlaced,	
		ISNULL(max(Bids.Date), NULL)    AS PreBidPlacedDate,
		CAST(CONVERT(CHAR(8),max(Bids.Date),112) AS INT) AS PreBidPlacedDayId,
		Auctions.LastBidAmount AS WinningPreBidAmount,
		Items.LiveDate,
		CAST(CONVERT(CHAR(8),Items.LiveDate,112) AS INT) AS AuctionLiveDate_DayID,
		ISNULL(max(Proxies.MaxAmount),0) AS MaxAmount
FROM #TMP_PreBidAuctions TPBA
INNER JOIN BI_Auction_Rep.dbo.Auctions AS Auctions
	ON  TPBA.AuctionId = Auctions.AuctionId AND TPBA.ItemId = Auctions.ItemID
INNER JOIN BI_Auction_Rep.dbo.Items AS Items
	ON Auctions.ItemId = Items.ItemID 
LEFT OUTER JOIN BI_Auction_Rep.dbo.bids AS Bids
	ON Auctions.AuctionId = Bids.AuctionId
LEFT OUTER JOIN BI_Auction_Rep.dbo.proxies Proxies
	ON bids.AuctionId = Proxies.AuctionId
AND bids.UserId = Proxies.UserId
GROUP BY Auctions.AuctionId,
		Items.ItemId,
		Items.Salvage_ID,
		Bids.UserID,	
		Auctions.OpenDate,
		CAST(CONVERT(CHAR(8),Auctions.OpenDate,112) AS INT),
		Auctions.CloseDate,
		Auctions.LastBidAmount,
		Items.LiveDate,
		CAST(CONVERT(CHAR(8),Items.LiveDate,112) AS INT)
Order by AuctionId, ItemId


IF OBJECT_ID('tempdb..#TMP_PreBidAuctionsItems_Stage') IS NOT NULL DROP TABLE #TMP_PreBidAuctionsItems_Stage
CREATE TABLE #TMP_PreBidAuctionsItems_Stage(
	[AuctionItemID] [int] NOT NULL,
	[StockID] [int] NOT NULL,
	[UserID] [int] NOT NULL,
	[BranchNumber] [int] NOT NULL,
	[ProviderID] [int] NOT NULL,
	[BuyerID] [int] NOT NULL,
	[BuyerPreBidAuctionDateTime] [datetime] NULL,
	[BuyerPreBidAuctionDayID] [int] NULL,
	[PreBidAuctionOpenDateTime] [datetime] NOT NULL,
	[PreBidAuctionOpenDayID] [int] NOT NULL,
	[PreBidAuctionCloseDateTime] [datetime] NOT NULL,
	[PreBidAuctionCloseDayID] [int] NOT NULL,
	[PreBidAuctionMinAmount] [money] NOT NULL,
	[PreBidAuctionMaxAmount] [money] NOT NULL,
	[PreBidAuctionMaxAmountReached] [money] NOT NULL,
	[PreBidAuctionWinningBidAmount] [money] NOT NULL,
	[BIUpdateDateTime] [datetime] NOT NULL,
	[BuyerEmployeeID] [int] NULL)
INSERT INTO #TMP_PreBidAuctionsItems_Stage(
	   AuctionItemID
      ,StockID
      ,UserID
      ,BranchNumber
      ,ProviderID
      ,BuyerID
      ,BuyerPreBidAuctionDateTime
      ,BuyerPreBidAuctionDayID
      ,PreBidAuctionOpenDateTime
      ,PreBidAuctionOpenDayID
      ,PreBidAuctionCloseDateTime
      ,PreBidAuctionCloseDayID
      ,PreBidAuctionMinAmount
      ,PreBidAuctionMaxAmount
      ,PreBidAuctionMaxAmountReached
      ,PreBidAuctionWinningBidAmount
      ,BIUpdateDateTime
      ,BuyerEmployeeID)
SELECT DAI.AuctionItemID 
	  ,DS.StockID
	  ,ISNULL(PBAI.UserID,0) AS PreBidAuction_Buyer_ID 
      ,DS.AdministrativeBranchNumber AS BranchNumber
      ,DS.CurrentProviderID
      ,ISNULL(ASAPBuyerID,0) AS Buyer_ID
	  ,dbo.fn_TimeZoneConvert(PBAI.PreBidPlacedDate ,DS.AdministrativeBranchNumber) AS Buyer_PreBidAuction_DateTime
      ,CAST(CONVERT(CHAR(8),edw.dbo.fn_TimeZoneConvert(PBAI.PreBidPlacedDate ,DS.AdministrativeBranchNumber),112) AS INT) AS Buyer_PreBidAuction_Day_ID
      --,PBAI.PreBidPlacedDate AS Buyer_PreBidAuction_DateTime
      --,PBAI.PreBidPlacedDayId AS Buyer_PreBidAuction_Day_ID
      ,PBAI.OpenDate AS PreBidAuction_Open_DateTime
      ,PBAI.OpenDayId AS PreBidAuction_Open_Day_ID
      ,PBAI.CloseDate AS PreBidAuction_Close_DateTime
      ,PBAI.CloseDayId AS PreBidAuction_Close_Day_ID
      ,PBAI.MinAmount AS PreBidAuction_Min_Amount
      ,PBAI.MaxAmount AS PreBidAuction_Max_Amount
      ,PBAI.MaxAmountPlaced AS PreBidAuction_Max_Amount_Reached
      ,PBAI.WinningPreBidAmount AS PreBidAuction_WinningBid_Amount
      ,getDate()  AS BI_Update_DateTime
      ,ISNULL(ASAPEmployeeID,0) AS Buyer_Employee_ID
FROM #TMP_PreBidAuctionsItems PBAI
INNER JOIN dbo.DimStock DS WITH(NOLOCK)
	ON PBAI.Salvage_ID = DS.ASAPSalvageID
INNER JOIN dbo.DimAuctionItem DAI WITH(NOLOCK)
	ON PBAI.AuctionLiveDate_DayID = CAST(CONVERT(CHAR(8),AuctionDateTime,112) AS INT)
		AND DS.StockID = DAI.StockID
INNER JOIN [BI_Directory_Rep].[dbo].[users] U WITH(NOLOCK)
	ON PBAI.UserID = U.UserID AND ISNULL(U.UserID,0) <> 0



SELECT @ExtractRowCount = COUNT(*) FROM #TMP_PreBidAuctionsItems_Stage

/*********************************************************************************************
MERGE the data from the temp table into the EDW FactPreBidAuctionItem table
This does Update and Insert only.  No Deletes
*********************************************************************************************/

IF OBJECT_ID('tempdb..#temp_PreBidAuctionItemRecordCount') IS NOT NULL DROP TABLE #temp_PreBidAuctionItemRecordCount
CREATE TABLE #temp_PreBidAuctionItemRecordCount(ChangeType VARCHAR(10)) 


BEGIN TRANSACTION 


	MERGE dbo.FactPreBidAuctionItem AS T
	USING #TMP_PreBidAuctionsItems_Stage AS S
			ON  T.AuctionItemID = S.AuctionItemID AND T.StockID = S.StockID AND T.UserID = S.UserID
			
	
	WHEN MATCHED THEN
		UPDATE SET
			   T.AuctionItemID				= S.AuctionItemID
			  ,T.StockID					= S.StockID
			  ,T.UserID						= S.UserID	
			  ,T.BranchNumber				= S.BranchNumber
			  ,T.ProviderID					= S.ProviderID
			  ,T.BuyerID					= S.BuyerID
			  ,T.BuyerPreBidAuctionDateTime	= S.BuyerPreBidAuctionDateTime
			  ,T.BuyerPreBidAuctionDayID	= S.BuyerPreBidAuctionDayID
			  ,T.PreBidAuctionOpenDateTime	= S.PreBidAuctionOpenDateTime
			  ,T.PreBidAuctionOpenDayID     = S.PreBidAuctionOpenDayID
			  ,T.PreBidAuctionCloseDateTime = S.PreBidAuctionCloseDateTime
			  ,T.PreBidAuctionCloseDayID	= S.PreBidAuctionCloseDayID
			  ,T.PreBidAuctionMinAmount		= S.PreBidAuctionMinAmount
			  ,T.PreBidAuctionMaxAmount		= S.PreBidAuctionMaxAmount
			  ,T.PreBidAuctionMaxAmountReached = S.PreBidAuctionMaxAmountReached
			  ,T.PreBidAuctionWinningBidAmount = S.PreBidAuctionWinningBidAmount
			  ,T.BIUpdateDateTime			= S.BIUpdateDateTime
			  ,T.BuyerEmployeeID			= S.BuyerEmployeeID
			  ,T.ETLLoadUpdateID = @ETLLoadID
	WHEN NOT MATCHED THEN
	INSERT (AuctionItemID
		  ,StockID
		  ,UserID
		  ,BranchNumber
		  ,ProviderID
		  ,BuyerID
		  ,BuyerPreBidAuctionDateTime
		  ,BuyerPreBidAuctionDayID
		  ,PreBidAuctionOpenDateTime
		  ,PreBidAuctionOpenDayID
		  ,PreBidAuctionCloseDateTime
		  ,PreBidAuctionCloseDayID
		  ,PreBidAuctionMinAmount
		  ,PreBidAuctionMaxAmount
		  ,PreBidAuctionMaxAmountReached
		  ,PreBidAuctionWinningBidAmount
		  ,BIUpdateDateTime
		  ,ETLLoadID
		  ,ETLLoadUpdateID
		  ,BuyerEmployeeID)
	 VALUES (AuctionItemID
		  ,StockID
		  ,UserID
		  ,BranchNumber
		  ,ProviderID
		  ,BuyerID
		  ,BuyerPreBidAuctionDateTime
		  ,BuyerPreBidAuctionDayID
		  ,PreBidAuctionOpenDateTime
		  ,PreBidAuctionOpenDayID
		  ,PreBidAuctionCloseDateTime
		  ,PreBidAuctionCloseDayID
		  ,PreBidAuctionMinAmount
		  ,PreBidAuctionMaxAmount
		  ,PreBidAuctionMaxAmountReached
		  ,PreBidAuctionWinningBidAmount
		  ,BIUpdateDateTime
		  ,@ETLLoadID
		  ,@ETLLoadID
		  ,BuyerEmployeeID)


OUTPUT $action INTO #temp_PreBidAuctionItemRecordCount;
COMMIT TRAN

SELECT @InsertRowCount = COUNT(*)  FROM #temp_PreBidAuctionItemRecordCount WHERE ChangeType = 'INSERT'
SELECT @UpdateRowCount = COUNT(*)  FROM #temp_PreBidAuctionItemRecordCount WHERE ChangeType = 'UPDATE'


SET  @UpdateRowCount = @@ROWCOUNT 	
SET @InsertRowCount = @InsertRowCount

EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID,
		@ExtractRowCount = @ExtractRowCount,
		@InsertRowCount = @InsertRowCount,
		@UpdateRowCount = @UpdateRowCount
		
		IF OBJECT_ID('tempdb..#TMP_PreBidAuctions') IS NOT NULL DROP TABLE #TMP_PreBidAuctions
		IF OBJECT_ID('tempdb..#TMP_PreBidAuctionsItems') IS NOT NULL DROP TABLE #TMP_PreBidAuctionsItems
		IF OBJECT_ID('tempdb..#TMP_PreBidAuctionsItems_Stage') IS NOT NULL DROP TABLE #TMP_PreBidAuctionsItems_Stage
		
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
		
		IF OBJECT_ID('tempdb..#TMP_PreBidAuctions') IS NOT NULL DROP TABLE #TMP_PreBidAuctions
		IF OBJECT_ID('tempdb..#TMP_PreBidAuctionsItems') IS NOT NULL DROP TABLE #TMP_PreBidAuctionsItems
		IF OBJECT_ID('tempdb..#TMP_PreBidAuctionsItems_Stage') IS NOT NULL DROP TABLE #TMP_PreBidAuctionsItems_Stage
		
		RETURN -1
	

END CATCH

END
