/* 
USE EDW
GO 
==============================================================================
Author		: Matt Stelter
Create date	: 4/10/2012
Description	: Populate the EDW DimAuctionItem table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number
5/2/2012	Matt Stelter		Added delete
5/4/2012	Matt Stelter		Changed to use ony auction if Bulk or IBF
5/23/2012	Matt Stelter		Changed to use DimAuctionItem and Added code for new Auction Item objects 
6/14/2012   Venkata Vempali     Updated logic for IsNoValidBidAmount
6/28/2012	Matt Stelter		Changed to outer join from Auction Entry
9/27/2012   Venkata Vempali     Considered the StockID's, based on the #AuctionItemsToDelete.
10/02/2012  Somesh V            Added AuctionLane
10/10/2012  Juan X Jacome		--DBA Code Review added Nolock Hint
01/29/2013  Somesh V            Added update for MinimumBidAmount
2/14/2013	MAtt Stelter		Added @TheDate per DBA
5/15/2014	Matt Stelter		Added Live Rep Update		
03/03/2015  Bruce Kutnick		Additional code created to remove some duplicate records
==============================================================================
Usage Example:
Exec usp_ETLDimAuctionItem 
--SELECT * FROM dbo.DimAuctionItem WHERE ETLLoadUpdateID = (SELECT MAX(ETLLoadUpdateID) FROM dbo.DimAuctionItem) order by 3,2
--SELECT * FROM dbo.DimAuctionItem WHERE ETLLoadID = (SELECT MAX(ETLLoadID) FROM dbo.DimAuctionItem) order by 3,2
==============================================================================
*/

CREATE PROCEDURE [dbo].[usp_ETLDimAuctionItem]
AS     
BEGIN

DECLARE @ETLLoadID INT,
		@ExtractRowCount_sp INT,
		@InsertRowCount INT,
		@UpdateRowCount INT,
		@DeleteRowCount INT,
		@StartLSN BINARY(10),
		@EndLSN BINARY(10),
		@MinAllowableDate		Datetime,
		@MaxAllowableDate		Datetime,
		@TheDate	Datetime

SET NOCOUNT ON;
BEGIN TRY

--Run the Audit begin
--Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimAuctionItem'
SELECT @MinAllowableDate = MIN(DayDate), @MaxAllowableDate = MAX(DayDate) from DimDay WITH (NOLOCK) WHERE DayID > 19000101 and DayID < 29991231

--Get the start and end LSNs

SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN,@TheDate = MinDatetime-3 FROM dbo.ETLRun WITH (NOLOCK) WHERE ETLJobName = 'DailyETLJob'

/*********************************************************************************************
Create the temp tables 
*********************************************************************************************/

CREATE TABLE #temp_AuctionRecordCount(ChangeType VARCHAR(30)) 

CREATE TABLE #TMPStockID (StockID INT)

CREATE TABLE #DimAuctionItem
  ([AuctionItemID]                    [INT] NOT NULL,
   [AuctionID]                        [INT] NOT NULL,
   [StockID]                          [INT] NOT NULL,
   [BuyerID]                          [INT] NULL,
   [AuctionItemSequenceNumber]        [SMALLINT] NOT NULL,
   [AuctionNumber]                    [SMALLINT] NOT NULL,
   [IsLiveInternetBid]                [TINYINT] NOT NULL,
   [IsPreviousSalePending]            [TINYINT] NOT NULL,
   [IsRunandDrive]                    [TINYINT] NOT NULL,
   [IsSealedBid]                      [TINYINT] NOT NULL,
   [DoesVehicleStart]                 [TINYINT] NULL,
   [FinalBid]                         [MONEY] NULL,
   [NextHighestHistoricalBid]         [MONEY] NULL,
   [PreviousHistoricalBid]            [MONEY] NULL,
   [IsMostRecentAuction]              [TINYINT] NOT NULL,
   [AuctionSequenceNumber]            [INT] NULL,
   [IsNoValidBidAmount]               [TINYINT] NOT NULL,
   [IsSetForSale]                     [TINYINT] NOT NULL,
   [IsProviderAuction]                [TINYINT] NOT NULL,
   [IsMostRecentAuctionProvider]      [TINYINT] NOT NULL,
   [NextHighestHistoricalBidProvider] [MONEY] NULL,
   [PreviousHistoricalBidProvider]    [MONEY] NULL,
   [OriginalBid]						[MONEY] NULL,
   [OriginalBidProvider]				[MONEY] NULL,
   [AuctionDateTime]					[DATETIME] NULL,
   [LatestAuctionDateTime]				[DATETIME] NULL,
   [OriginalAuctionDateTime]			[DATETIME] NULL,
	[IsProviderAuctionNoRenegeAfterSettle] [TINYINT] NULL,
	[PreBidWinningAmount]				[MONEY] NULL,
	[AuctionLane]                       VARCHAR(1)NULL )

CREATE TABLE #LastProviderEvent
  (StockID                INT,
   MinNonProviderDatetime DATETIME)
   
CREATE TABLE #BuyerRenegeCancelEvent
  (StockID             INT,
   CancelEventDatetime DATETIME)

CREATE TABLE #RenegedAfterSettlement (StockID INT, EventDateTime Datetime, EventCode VARCHAR(8), IsSettlementVoided tinyint, IsRenegedAfterCancelled tinyint)

CREATE TABLE #TMP_Auction_Reneged_Canceled
  (StockID             INT,
   Auction_DateTime    DATETIME,
   CancelEventDatetime DATETIME)

CREATE TABLE #TempSoldAuction(StockID int,MAXAuctionDateTime datetime)

CREATE TABLE #TempLastAuction(StockID int,MAXAuctionDateTime datetime, MAXBidAmount MONEY)

CREATE TABLE #PreviousBid(StockID int,PreviousHistoricalBid MONEY, NextHighestHistoricalBid MONEY)

CREATE TABLE #TempSoldAuctionP(StockID int,MAXAuctionDateTime datetime)

CREATE TABLE #TempLastAuctionP(StockID int,MAXAuctionDateTime datetime, MAXBidAmount MONEY)

CREATE TABLE #PreviousBidP(StockID int,PreviousHistoricalBid MONEY, NextHighestHistoricalBid MONEY)

CREATE TABLE #AuctionItemsToDelete (AuctionItemID INT)

--Tables for MinBidAmount

CREATE TABLE #TMP_FBD (StockID INT NOT NULL, ASAPUpdateDate Datetime NULL, AuctionDatetime Datetime NULL)
 
CREATE TABLE #TMP_MinBid(StockID INT NOT NULL, AuctionDatetime Datetime, ASAPUpdateDate Datetime, MinimumBidCurrentAmount money ) 

-- Added 3-3-2015 to remove ancillary duplicates
CREATE TABLE #MoreDups
		(
		AuctionItemID int,
		AuctionNumber smallint
		)

----DECLARE @StartLSN Binary(10)
----	,@EndLSN Binary (10)
----SELECT @StartLSN = BI_ASAP_Rep.sys.fn_cdc_get_min_lsn('dbo_Auction_Item')
----SELECT @EndLSN  = BI_ASAP_Rep.sys.fn_cdc_get_max_lsn()

/*1. Get Auction Item Stocks to Update */
--Changes to Auction_Item
INSERT INTO #AuctionItemsToDelete
            (AuctionItemID)
  SELECT AI1.Auction_Item_ID
		FROM (SELECT * FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Auction_Item](@StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 1) AI1
		INNER JOIN dbo.DimStock DS WITH (NOLOCK)
			ON AI1.Salvage_ID = DS.ASAPSalvageID
		INNER JOIN BI_ASAP_Rep.dbo.Auction_Schedule AS1 WITH (NOLOCK)
			ON AI1.Auction_Schedule_ID = AS1.Auction_Schedule_ID  
		INNER JOIN [BI_ASAP_REP].dbo.Salvage S2 WITH (NOLOCK)
			ON S2.Salvage_ID = DS.ASAPSalvageID
		LEFT OUTER JOIN [BI_ASAP_REP].dbo.Auction_Item AI2 WITH (NOLOCK)
			ON AI2.Auction_Item_ID = AI1.Auction_Item_ID
			AND AI2.Auction_Schedule_ID = AS1.Auction_Schedule_ID
		LEFT OUTER JOIN
			[BI_ASAP_REP].dbo.Salvage_Sale SS WITH (NOLOCK)
			ON AI1.Auction_Item_ID = SS.Auction_Item_ID
		LEFT OUTER JOIN
			[BI_ASAP_REP].dbo.Auction_Entry AE WITH (NOLOCK)
			ON AI1.Auction_Item_ID = AE.Auction_Item_ID
		WHERE AI2.Auction_Item_ID is null
		  AND AI2.Auction_Schedule_ID is null
			AND SS.Auction_Item_ID is null
			AND AE.Auction_Item_ID is null



/* StockID's which should be considered for ETL, Even the AuctionItem records will be Most Recent Auction (Provider) indicator needs to be updated*/

INSERT INTO #TMPStockID(StockID)
SELECT DISTINCT AI.StockID
FROM dbo.DimAuctionItem AI WITH (NOLOCK)
	INNER JOIN #AuctionItemsToDelete RTD WITH (NOLOCK) 
		ON AI.AuctionItemID = RTD.AuctionItemID


/*1. Get Auction Item Stocks to Update */
--Changes to Auction_Item
INSERT INTO #TMPStockID
            (StockID)
SELECT DISTINCT StockID
FROM (SELECT Salvage_ID FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Auction_Item](@StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5) AI
INNER JOIN dbo.DimStock DS WITH (NOLOCK)
	ON AI.Salvage_ID = DS.ASAPSalvageID

--Changes to Auction_Entry
INSERT INTO #TMPStockID
            (StockID)
SELECT DISTINCT StockID
FROM (SELECT Auction_Item_ID FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Auction_Entry](@StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5) AE
INNER JOIN [BI_ASAP_Rep].[dbo].Auction_Item AI WITH (NOLOCK)
	ON AI.Auction_Item_ID = AE.Auction_Item_ID
INNER JOIN dbo.DimStock DS WITH (NOLOCK)
	ON AI.Salvage_ID = DS.ASAPSalvageID

--Changes to Auction_Schedule
INSERT INTO #TMPStockID
            (StockID)
SELECT DISTINCT StockID
FROM (SELECT Auction_Schedule_ID FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Auction_Schedule](@StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5) ASched
INNER JOIN [BI_ASAP_Rep].[dbo].Auction_Item AI WITH (NOLOCK)
	ON AI.Auction_Schedule_ID = ASched.Auction_Schedule_ID
INNER JOIN dbo.DimStock DS WITH (NOLOCK)
	ON AI.Salvage_ID = DS.ASAPSalvageID

/* 2.  Get the StockIDs that have the event code 'BYRE', 'BYCN','FSEN','STLV' */
INSERT INTO #TMPStockID
            (StockID)
SELECT DISTINCT S.StockID
FROM (SELECT Salvage_ID, Salvage_Event_Code FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_History](@StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5) FSE
       INNER JOIN dbo.DimStock S WITH (NOLOCK)
         ON FSE.Salvage_ID=S.ASAPSalvageID
       LEFT OUTER JOIN #TMPStockID TSID WITH (NOLOCK)
         ON S.StockID=TSID.StockID
WHERE  Salvage_Event_Code IN ('BYRE', 'BYCN', 'FSEN', 'STLV')
   AND TSID.StockID IS NULL

--SELECT Distinct Stockid from #TMPStockID

/*3.  Get all auctions for Auction Items to Update into Temp table */
INSERT INTO #DimAuctionItem
            (AuctionItemID,
             AuctionID,
             StockID,
             BuyerID,
             AuctionItemSequenceNumber,
             AuctionNumber,
             IsLiveInternetBid,
             IsPreviousSalePending,
             IsRunandDrive,
             IsSealedBid,
             DoesVehicleStart,
             FinalBid,
             NextHighestHistoricalBid,
             PreviousHistoricalBid,
             IsMostRecentAuction,
             AuctionSequenceNumber,
             IsNoValidBidAmount,
             IsSetForSale,
             IsProviderAuction,
             IsMostRecentAuctionProvider,
             NextHighestHistoricalBidProvider,
             PreviousHistoricalBidProvider,
			 AuctionDateTime,
			 IsProviderAuctionNoRenegeAfterSettle
)
SELECT ASAP_AI.Auction_Item_ID,
       ASAP_AI.Auction_Schedule_ID,
       DS.StockID,
       CASE
         WHEN AE.Min_Buyer_Id<>AE.Max_Buyer_ID THEN
           CASE
             WHEN (AE.Total_Auction_Entry_ID=AE.Total_Declined_Entry) THEN 0
             WHEN AE.Total_Pending_Entry=AE.Total_Auction_Entry_ID THEN 0
             WHEN AE.Total_Accepted_Negotiated_Entry=1 THEN AE.Negotiated_Sale_Buyer_Id
           END
         WHEN AE.Min_Buyer_Id=AE.Max_Buyer_ID
              AND AE.Total_Pending_Entry=AE.Total_Auction_Entry_ID THEN 0
         ELSE ISNULL(AE.Max_Buyer_ID,0)
       END AS BuyerId,
       ISNULL(ASAP_AI.Auction_Item_Sequence_Number,0),
       ISNULL(ASAP_AI.Auction_Number,0),
       ISNULL(ASAP_AI.Live_Internet_Bid_Ind,0),
       ISNULL(ASAP_AI.Previous_Sale_Pending_Ind,0),
       ISNULL(AE.Run_and_Drive_Ind,0),
       ISNULL(ASAP_AI.Sealed_Bid_Ind,0),
       ISNULL(AE.Vehicle_Starts_IND,0),
       CASE
         WHEN AE.Lowest_Auction_Entry_Bid_Amount<>AE.Highest_Auction_Entry_Bid_Amount THEN
           CASE
             WHEN (AE.Total_Auction_Entry_ID=AE.Total_Declined_Entry) THEN NULL
             WHEN AE.Total_Pending_Entry=AE.Total_Auction_Entry_ID THEN NULL
             WHEN AE.Total_Accepted_Negotiated_Entry=1 THEN AE.Negotiated_Sale_Amount
           END
         WHEN AE.Lowest_Auction_Entry_Bid_Amount=AE.Highest_Auction_Entry_Bid_Amount
              AND AE.Total_Pending_Entry=AE.Total_Auction_Entry_ID THEN NULL
         ELSE AE.Highest_Auction_Entry_Bid_Amount
       END AS FinalBid,
       NULL AS [NextHighestHistoricalBid],--Need to Calculate this
       NULL AS [PreviousHistoricalBid],--Need to Calculate this
       0 AS [IsMostRecentAuction],--Next Sprint
       Row_number() OVER ( PARTITION BY DS.ASAPSalvageID ORDER BY ASAP_AS.Auction_Datetime) AS AuctionSequenceNumber,
       --CASE
       --  WHEN Isnull(AE.Lowest_Auction_Entry_Bid_Amount, 0)<>Isnull(AE.Highest_Auction_Entry_Bid_Amount, 0) THEN 1
       --  WHEN Isnull(AE.Lowest_Auction_Entry_Bid_Amount, 0)=Isnull(AE.Highest_Auction_Entry_Bid_Amount, 0)
       --       AND Isnull(AE.Lowest_Auction_Entry_Bid_Amount, 0)=0 THEN 1
       --  WHEN AE.Total_Pending_Entry=AE.Total_Auction_Entry_ID THEN 1
       --  WHEN Isnull(AE.Lowest_Auction_Entry_Bid_Amount, 0)=Isnull(AE.Highest_Auction_Entry_Bid_Amount, 0)
       --       AND AE.Total_Pending_Entry=AE.Total_Auction_Entry_ID THEN 0
       --  ELSE 0
       --END AS IsNoValidBidAmount,
       0 AS IsNoValidBidAmount, -- Will Be based updated based on the FinalBid.
       1 AS [IsSetForSale],--Always 1
       0 AS [IsProviderAuction],--Need to Calculate this
       0 AS [IsMostRecentAuctionProvider],
       NULL AS [NextHighestHistoricalBidProvider],--Need to Calculate this
       NULL AS [PreviousHistoricalBidProvider], --Need to Calculate this
		CASE WHEN Isnull(ASAP_AS.Auction_DateTime, '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN ISNULL(ASAP_AS.Auction_DateTime, '12/31/2999') ELSE '12/31/2999' END AS Auction_DateTime,
       0 as IsProviderAuctionNoRenegeAfterSettle
FROM   [BI_ASAP_REP].dbo.Auction_Item AS ASAP_AI WITH (NOLOCK)
       INNER JOIN [BI_ASAP_REP].dbo.Auction_Schedule AS ASAP_AS WITH (NOLOCK)
         ON ASAP_AI.Auction_Schedule_ID=ASAP_AS.Auction_Schedule_ID
       INNER JOIN dbo.DimStock AS DS WITH (NOLOCK)
         ON ASAP_AI.Salvage_ID=DS.ASAPSalvageID
       INNER JOIN (SELECT DISTINCT StockID FROM #TMPStockID WITH (NOLOCK)) TSID
         ON DS.StockID=TSID.StockID
       LEFT OUTER JOIN (SELECT AE.Auction_Schedule_ID,
						AE.Auction_Item_ID,
						Max(Cast(Isnull(Run_and_Drive_IND, 0) AS INTEGER)) AS Run_and_Drive_Ind,
						CASE WHEN Max(Cast(Isnull(Run_and_Drive_IND, 0) AS INTEGER))=1 THEN 1 
							ELSE Max(Cast(Isnull(Vehicle_Starts_IND, 0) AS INTEGER))
							END AS Vehicle_Starts_IND,
                        Min(AE.Bid_Amount) AS Lowest_Auction_Entry_Bid_Amount,
                        Max(AE.Bid_Amount) AS Highest_Auction_Entry_Bid_Amount,
                        Max(CASE WHEN AE.Auction_Entry_Reason_Code IN ('NSA', 'NSC') THEN Isnull(AE.Bid_Amount, 0) ELSE NULL END) AS Negotiated_Sale_Amount,
                        Count(AE.Auction_Entry_ID) AS Total_Auction_Entry_ID,
                        Sum(CASE WHEN AE.Accept_Bid_Status_Code='PND' THEN 1 ELSE 0 END) AS Total_Pending_Entry,
                        Sum(CASE WHEN AE.Accept_Bid_Status_Code='DEC' THEN 1 ELSE 0 END) AS Total_Declined_Entry,
                        Sum(CASE WHEN AE.Accept_Bid_Status_Code='ACP' AND AE.Auction_Entry_Reason_Code='NSC 'THEN 1 ELSE 0 END) AS Total_Accepted_Negotiated_Entry,
                        MIN(AE.Buyer_ID) AS Min_Buyer_Id,
						MAX(AE.Buyer_ID) AS Max_Buyer_ID,
						MAX(CASE WHEN AE.Auction_Entry_Reason_Code IN ('NSA','NSC') THEN ISNULL(AE.Buyer_ID,0) ELSE NULL END) AS Negotiated_Sale_Buyer_Id
                   FROM   BI_ASAP_REP.dbo.Auction_Entry AS AE WITH (NOLOCK)
                   WHERE  AE.Auction_Entry_Reason_Code<>'DUP'
                   GROUP  BY AE.Auction_Schedule_ID, AE.Auction_Item_ID) AS AE
         ON ASAP_AI.Auction_Item_ID=AE.Auction_Item_ID
            AND ASAP_AI.Auction_Schedule_ID=AE.Auction_Schedule_ID

/* Updating IsNoValidBidAmount */

	UPDATE AI SET
	--SELECT AI.AuctionItemID,AI.StockID,
		IsNoValidBidAmount = CASE WHEN ISNULL(FinalBid,0) = 0 THEN 1 ELSE 0 END
	FROM #DimAuctionItem AI

            
/*--4.  Update IsProviderAuction indicator*/
--Get date for first NON Provider Event
INSERT INTO #LastProviderEvent
            (StockID,
             MinNonProviderDatetime)
SELECT S.StockID,
       Min(Isnull(EventDatetime, '12/31/2999')) AS MinNonProviderDatetime
FROM   dbo.FactStockEvent FSE WITH (NOLOCK)
       INNER JOIN dbo.DimStock S WITH (NOLOCK)
         ON FSE.ASAPSalvageID=S.ASAPSalvageID
       INNER JOIN #TMPStockID TSID WITH (NOLOCK)
         ON S.StockID=TSID.StockID
WHERE  IsProviderReported=0
GROUP  BY S.StockID

--Mark any auction prior to the first NON Provider Event as provider
UPDATE AI
SET    [IsProviderAuction]=CASE
                             WHEN AuctionDateTime<Isnull(LPE.MinNonProviderDatetime, '12/31/2999') THEN 1
                             ELSE 0
                           END,
[IsProviderAuctionNoRenegeAfterSettle]=CASE
                             WHEN AuctionDateTime<Isnull(LPE.MinNonProviderDatetime, '12/31/2999') THEN 1
                             ELSE 0
                           END 
FROM   #DimAuctionItem AI WITH (NOLOCK)
LEFT OUTER JOIN #LastProviderEvent LPE WITH (NOLOCK)
         ON AI.StockID=LPE.StockID

--Mark any auction just preceding a Buyer Renege/Cancel as not provider 
/*Get the rows for a particular stock that have an event_Code of 'BYRE', 'BYCN'*/
INSERT INTO #BuyerRenegeCancelEvent
            (StockID,
             CancelEventDatetime)
SELECT S.StockID,
       EventDatetime
FROM   dbo.FactStockEvent FSE WITH (NOLOCK)
       INNER JOIN dbo.DimStock S WITH (NOLOCK)
         ON FSE.ASAPSalvageID=S.ASAPSalvageID
       INNER JOIN dbo.DimStockEventType DSET WITH (NOLOCK)
         ON FSE.EventTypeID=DSET.EventTypeID
       INNER JOIN #TMPStockID TSID WITH (NOLOCK)
         ON S.StockID=TSID.StockID         
WHERE  EventCode IN ('BYRE', 'BYCN')

--Need to exclude any Reneges/Cancels after settlement.  That auction counts as Provider.
INSERT INTO #RenegedAfterSettlement
            (StockID, EventDateTime, EventCode, IsSettlementVoided, IsRenegedAfterCancelled)
SELECT DISTINCT S.StockID, EventDateTime, EventCode, 0, 0
FROM   edw.dbo.FactStockEvent FSE WITH (NOLOCK)
       INNER JOIN edw.dbo.DimStock S WITH (NOLOCK)
         ON FSE.ASAPSalvageID=S.ASAPSalvageID
       INNER JOIN edw.dbo.DimStockEventType DSET WITH (NOLOCK)
         ON FSE.EventTypeID=DSET.EventTypeID
       INNER JOIN #TMPStockID TSID WITH (NOLOCK)
         ON S.StockID=TSID.StockID  
WHERE  EventCode IN ('BYRE', 'BYCN', 'STLV', 'FSEN')

--Mark all voided settlements as such
UPDATE RAS SET IsSettlementVoided = 1
FROM #RenegedAfterSettlement RAS WITH (NOLOCK)
INNER JOIN (SELECT StockID, EventDateTime FROM #RenegedAfterSettlement WITH (NOLOCK) WHERE  EventCode IN ('STLV')) SV
	ON RAS.StockID = SV.StockID and RAS.EventDateTime < SV.EventDateTime
WHERE  EventCode IN ('FSEN')

DELETE FROM #RenegedAfterSettlement WHERE EventCode IN ('STLV') OR IsSettlementVoided = 1

--Update [IsProviderAuctionNoRenegeAfterSettle] field
INSERT INTO #TMP_Auction_Reneged_Canceled
            (StockID,
             Auction_DateTime,
             CancelEventDatetime)
SELECT ai.StockID,
       Max(auctionDateTime) Auction_DateTime,
       t.CancelEventDatetime
FROM   #DimAuctionItem ai WITH (NOLOCK)
       INNER JOIN #BuyerRenegeCancelEvent t WITH (NOLOCK)
         ON t.StockID=ai.StockID
WHERE  auctionDateTime<=t.CancelEventDatetime
GROUP  BY ai.StockID,
          t.CancelEventDatetime

UPDATE ai
SET    [IsProviderAuctionNoRenegeAfterSettle]=CASE
                           WHEN Isnull(t.Auction_DateTime, '2999/12/31')<>'2999/12/31' THEN 0
                           ELSE 1
                         END
FROM   #DimAuctionItem ai
       INNER JOIN #TMP_Auction_Reneged_Canceled t WITH (NOLOCK)
         ON ai.StockID=t.StockID
            AND ai.AuctionDateTime=t.Auction_DateTime

DELETE FROM #TMP_Auction_Reneged_Canceled

--Delete all Renege/Cancelled after settlement from list of Renege/Cancelled
DELETE FROM BRCE
FROM #BuyerRenegeCancelEvent BRCE
INNER JOIN (SELECT RAS.StockID, MIN(RAS.EventDateTime) AS MINEventDateTime
	FROM #RenegedAfterSettlement RAS WITH (NOLOCK) 
	INNER JOIN (SELECT StockID, EventDateTime FROM #RenegedAfterSettlement WITH (NOLOCK) WHERE  EventCode IN ('FSEN')) ST
		ON RAS.StockID = ST.StockID and RAS.EventDateTime > ST.EventDateTime
	WHERE  EventCode IN ('BYRE', 'BYCN')
	GROUP BY RAS.StockID) RCAS
ON BRCE.StockID = RCAS.StockID AND BRCE.CancelEventDatetime = RCAS.MINEventDateTime

INSERT INTO #TMP_Auction_Reneged_Canceled
            (StockID,
             Auction_DateTime,
             CancelEventDatetime)
SELECT ai.StockID,
       Max(auctionDateTime) Auction_DateTime,
       t.CancelEventDatetime
FROM   #DimAuctionItem ai WITH (NOLOCK)
       INNER JOIN #BuyerRenegeCancelEvent t WITH (NOLOCK)
         ON t.StockID=ai.StockID
WHERE  auctionDateTime<=t.CancelEventDatetime
GROUP  BY ai.StockID,
          t.CancelEventDatetime

UPDATE ai
SET    IsProviderAuction=CASE
                           WHEN Isnull(t.Auction_DateTime, '2999/12/31')<>'2999/12/31' THEN 0
                           ELSE 1
                         END
FROM   #DimAuctionItem ai
       INNER JOIN #TMP_Auction_Reneged_Canceled t WITH (NOLOCK)
         ON ai.StockID=t.StockID
            AND ai.AuctionDateTime=t.Auction_DateTime

/*Last Auction & Last Provider Auction*/
UPDATE FAI SET IsMostRecentAuction = 1, [LatestAuctionDateTime] = [AuctionDateTime]
FROM #DimAuctionItem FAI 
INNER JOIN (SELECT StockID, MAX(AuctionDateTime) as MAXAuctionDateTime FROM #DimAuctionItem WITH (NOLOCK) GROUP BY StockID) MaxAuction
	ON FAI.StockID = MaxAuction.StockID AND FAI.AuctionDateTime = MaxAuction.MAXAuctionDateTime

UPDATE FAI SET IsMostRecentAuctionProvider = 1
FROM #DimAuctionItem FAI
INNER JOIN (SELECT StockID, MAX(AuctionDateTime) as MAXAuctionDateTime FROM #DimAuctionItem WITH (NOLOCK) WHERE IsProviderAuction = 1 GROUP BY StockID) MaxAuction
	ON FAI.StockID = MaxAuction.StockID AND FAI.AuctionDateTime = MaxAuction.MAXAuctionDateTime

/* 5.  Update Original Bid & Original Bid Provider */
UPDATE ai 
SET OriginalBid = CASE WHEN MA.StockID IS NOT NULL THEN ROUND(MA.FinalBid,0) ELSE NULL END
FROM   #DimAuctionItem ai WITH (NOLOCK)
       INNER JOIN dbo.DimStock DS WITH (NOLOCK)
         ON AI.StockID=DS.StockID
       LEFT OUTER JOIN (SELECT FAI.StockID,
                               FAI.FinalBid
                        FROM   #DimAuctionItem Fai WITH (NOLOCK)
                               INNER JOIN (SELECT ai2.StockID,
                                                  Min(ai2.AuctionDatetime) AS MinAuctionDateTime
                                           FROM   #DimAuctionItem ai2 WITH (NOLOCK)
                                                  LEFT OUTER JOIN (SELECT ai3.StockID,
                                                                          Max(ai3.AuctionDatetime) AS MaxAuctionDateTime
                                                                   FROM   #DimAuctionItem ai3 WITH (NOLOCK)
                                                                          INNER JOIN dbo.DimStock DS WITH (NOLOCK)
                                                                            ON AI3.StockID=DS.StockID
                                                                   WHERE  ai3.FinalBid>0
                                                                      AND IsBulkSale=0
                                                                      AND IsIBFSold=0
                                                                   GROUP  BY ai3.StockID) SaleAuctions
                                                    ON ai2.StockID=SaleAuctions.StockID
                                                       AND ai2.AuctionDateTime=SaleAuctions.MaxAuctionDateTime
                                           WHERE  ai2.FinalBid>0
                                              AND SaleAuctions.StockID IS NULL
                                           GROUP  BY ai2.StockID) MADT
                                 ON FAI.StockID=MADT.StockID
                                    AND Fai.AuctionDateTime=MADT.MinAuctionDateTime) MA
         ON AI.StockID=MA.StockID
WHERE  ai.IsMostRecentAuction=1

UPDATE ai
SET    OriginalAuctionDateTime=CASE 
                                 WHEN MA.StockID IS NOT NULL 
                               THEN MinAuctionDateTime
                                 ELSE '12/31/2999'
                               END
FROM   #DimAuctionItem ai WITH (NOLOCK)
       INNER JOIN dbo.DimStock DS WITH (NOLOCK)
         ON AI.StockID=DS.StockID
                               INNER JOIN (SELECT ai2.StockID,
                                                  Min(ai2.AuctionDatetime) AS MinAuctionDateTime
                                           FROM   #DimAuctionItem ai2 WITH (NOLOCK) GROUP BY ai2.StockID) MA
         ON AI.StockID=MA.StockID
WHERE  ai.IsMostRecentAuction=1

UPDATE ai SET OriginalBidProvider = CASE 
								 WHEN MA.StockID IS NOT NULL 
								 THEN ROUND(MA.FinalBid,0) 
								 ELSE NULL END
FROM   #DimAuctionItem ai WITH (NOLOCK)
       INNER JOIN dbo.DimStock DS WITH (NOLOCK)
         ON AI.StockID=DS.StockID
       LEFT OUTER JOIN (SELECT FAI.StockID,
                               FAI.FinalBid
                        FROM   #DimAuctionItem Fai WITH (NOLOCK)
                               INNER JOIN (SELECT ai2.StockID,
                                                  Min(ai2.AuctionDatetime) AS MinAuctionDateTime
                                           FROM   #DimAuctionItem ai2 WITH (NOLOCK)
                                                  LEFT OUTER JOIN (SELECT ai3.StockID,
                                                                          Max(ai3.AuctionDatetime) AS MaxAuctionDateTime
                                                                   FROM   #DimAuctionItem ai3 WITH (NOLOCK)
                                                                          INNER JOIN dbo.DimStock DS WITH (NOLOCK)
                                                                            ON AI3.StockID=DS.StockID
                                                                   WHERE  ai3.IsProviderAuction=1
                                                                      AND ai3.FinalBid>0
                                                                      AND IsBulkSale=0
                                                                      AND IsIBFSold=0
                                                                   GROUP  BY ai3.StockID) SaleAuctions
                                                    ON ai2.StockID=SaleAuctions.StockID
                                                       AND ai2.AuctionDateTime=SaleAuctions.MaxAuctionDateTime
                                           WHERE  ai2.IsProviderAuctionNoRenegeAfterSettle=1
                                              AND SaleAuctions.StockID IS NULL
                                              AND ai2.FinalBid>0
                                           GROUP  BY ai2.StockID) MADT
                                 ON FAI.StockID=MADT.StockID
                                    AND Fai.AuctionDateTime=MADT.MinAuctionDateTime) MA
         ON AI.StockID=MA.StockID
WHERE  ai.IsMostRecentAuctionProvider=1 

/*6.  Update NextHighestHistoricalBid and NextHighestHistoricalBidProvider */
/*Get Previous Auction*/
INSERT INTO #TempSoldAuction(StockID,MAXAuctionDateTime)
SELECT AI.StockID, MAX(AI.AuctionDateTime) as MAXAuctionDateTime
FROM #DimAuctionItem AI WITH (NOLOCK)
INNER JOIN dbo.DimStock DS  WITH (NOLOCK)
	ON AI.StockID = DS.StockID
WHERE IsBulkSale = 0 AND IsIBFSold = 0 AND ISNULL(AI.FinalBid,0) > 0
GROUP BY AI.StockID

INSERT INTO #TempLastAuction(StockID,MAXAuctionDateTime, MAXBidAmount)
SELECT DS.StockID, MAX(AuctionDateTime) as MAXAuctionDateTime, MAX(AI.FinalBid) as MAXBidAmount 
from #DimAuctionItem AI WITH (NOLOCK)
INNER JOIN dbo.DimStock DS  WITH (NOLOCK)
	ON AI.StockID = DS.StockID
LEFT OUTER JOIN #TempSoldAuction TSA WITH (NOLOCK)
ON 	TSA.StockID = DS.StockID AND TSA.MAXAuctionDateTime = AI.AuctionDateTime
WHERE TSA.StockID IS NULL AND ISNULL(AI.FinalBid,0) > 0
GROUP BY DS.StockID 

INSERT INTO #PreviousBid(StockID,PreviousHistoricalBid, NextHighestHistoricalBid)
SELECT TFA.StockID, MAX(AI.FinalBid) as PreviousHistoricalBid, MAX(MAXBidAmount) AS NextHighestHistoricalBid 
from #DimAuctionItem AI WITH (NOLOCK)
INNER JOIN #TempLastAuction TFA  WITH (NOLOCK)
	ON AI.StockID = TFA.StockID AND AI.AuctionDateTime = TFA.MaxAuctionDateTime	
WHERE  ISNULL(AI.FinalBid,0) > 0
GROUP BY TFA.StockID

UPDATE AI
SET PreviousHistoricalBid = PB.PreviousHistoricalBid, NextHighestHistoricalBid = PB.NextHighestHistoricalBid
FROM 
#DimAuctionItem AI WITH (NOLOCK)
LEFT OUTER JOIN #PreviousBid PB WITH (NOLOCK)
	ON AI.StockID = PB.StockID 
WHERE ai.IsMostRecentAuction = 1

/*Provider Objects*/
INSERT INTO #TempSoldAuctionP(StockID,MAXAuctionDateTime)
SELECT AI.StockID, MAX(AI.AuctionDateTime) as MAXAuctionDateTime
FROM #DimAuctionItem AI WITH (NOLOCK)
INNER JOIN dbo.DimStock DS  WITH (NOLOCK)
	ON AI.StockID = DS.StockID
WHERE IsBulkSale = 0 AND IsIBFSold = 0  AND ISNULL(AI.FinalBid,0) > 0 AND AI.IsProviderAuction = 1
GROUP BY AI.StockID

INSERT INTO #TempLastAuctionP(StockID,MAXAuctionDateTime, MAXBidAmount)
SELECT DS.StockID, MAX(AI.AuctionDateTime) as MAXAuctionDateTime, MAX(AI.FinalBid) as MAXBidAmount 
from #DimAuctionItem AI WITH (NOLOCK)
INNER JOIN dbo.DimStock DS  WITH (NOLOCK)
	ON AI.StockID = DS.StockID
LEFT OUTER JOIN #TempSoldAuctionP TSA WITH (NOLOCK)
ON 	TSA.StockID = DS.StockID AND TSA.MAXAuctionDateTime = AI.AuctionDateTime
WHERE (TSA.StockID IS NULL)  AND ISNULL(AI.FinalBid,0) > 0 AND AI.IsProviderAuctionNoRenegeAfterSettle = 1
GROUP BY DS.StockID 

INSERT INTO #PreviousBidP(StockID,PreviousHistoricalBid, NextHighestHistoricalBid)
SELECT TFA.StockID, MAX(AI.FinalBid) as PreviousHistoricalBid, MAX(MAXBidAmount) AS NextHighestHistoricalBid 
from #DimAuctionItem AI WITH (NOLOCK)
INNER JOIN #TempLastAuctionP TFA  WITH (NOLOCK)
	ON AI.StockID = TFA.StockID AND AI.AuctionDateTime = TFA.MaxAuctionDateTime	
WHERE ISNULL(AI.FinalBid,0) > 0 AND AI.IsProviderAuctionNoRenegeAfterSettle = 1
GROUP BY TFA.StockID

UPDATE AI
SET PreviousHistoricalBidProvider = PB.PreviousHistoricalBid, NextHighestHistoricalBidProvider = PB.NextHighestHistoricalBid
FROM #DimAuctionItem AI WITH (NOLOCK)
LEFT OUTER JOIN #PreviousBidP PB WITH (NOLOCK)
	ON AI.StockID = PB.StockID 
WHERE ai.IsMostRecentAuctionProvider = 1

UPDATE DAI
SET PreBidWinningAmount = A.lastBidAmount
--SELECT PreBidWinningAmount , A.lastBidAmount
 FROM #DimAuctionItem DAI
INNER JOIN dbo.DimStock DS  WITH (NOLOCK)
	ON DAI.StockID = DS.StockID
INNER JOIN [BI_Auction_Rep].[dbo].items Items
	ON Items.Salvage_ID = DS.ASAPSalvageID
		AND CONVERT(CHAR(8),Items.livedate,112) = CONVERT(CHAR(8),DAI.AuctionDateTime,112)
INNER JOIN [BI_Auction_Rep].[dbo].[auctions] A
	ON A.itemid = Items.itemid	

-- Auction Lane
UPDATE DAI 
SET 
AuctionLane = ALM.Auction_Lane

FROM #DimAuctionItem DAI 
 INNER JOIN BI_ASAP_Rep.dbo.Auction_Lane_Mapping ALM 
  ON DAI.AuctionNumber = ALM.Auction_Number

/*********************************************************************************************
MERGE the data from the temp table into the EDW DimAuction table
This does Update and Insert only.  No Deletes
*********************************************************************************************/

BEGIN TRANSACTION 
  
    MERGE dbo.DimAuctionItem AS AI
    USING #DimAuctionItem AS A
          ON A.AuctionItemID = AI.AuctionItemID
    WHEN MATCHED THEN 
    UPDATE SET 
		AuctionItemID=A.AuctionItemID
		,AuctionID=A.AuctionID
		,StockID=A.StockID
		,BuyerID=A.BuyerID
		,AuctionItemSequenceNumber=A.AuctionItemSequenceNumber
		,AuctionNumber=A.AuctionNumber
		,IsLiveInternetBid=A.IsLiveInternetBid
		,IsPreviousSalePending=A.IsPreviousSalePending
		,IsRunandDrive=A.IsRunandDrive
		,IsSealedBid=A.IsSealedBid
		,DoesVehicleStart=A.DoesVehicleStart
		,FinalBid=A.FinalBid
		,NextHighestHistoricalBid=A.NextHighestHistoricalBid
		,PreviousHistoricalBid=A.PreviousHistoricalBid
		,IsMostRecentAuction=A.IsMostRecentAuction
		,AuctionSequenceNumber=A.AuctionSequenceNumber
		,IsNoValidBidAmount=A.IsNoValidBidAmount
		,IsSetForSale=A.IsSetForSale
		,IsProviderAuction=A.IsProviderAuction
		,IsMostRecentAuctionProvider=A.IsMostRecentAuctionProvider
		,NextHighestHistoricalBidProvider=A.NextHighestHistoricalBidProvider
		,PreviousHistoricalBidProvider=A.PreviousHistoricalBidProvider
		,AuctionDateTime=A.AuctionDateTime
		,OriginalBid=A.OriginalBid
		,OriginalBidProvider=A.OriginalBidProvider
		,PreBidWinningAmount = A.PreBidWinningAmount
		,AuctionLane = A.AuctionLane
		,ETLLoadUpdateID=@ETLLoadID	
   WHEN NOT MATCHED THEN
   INSERT([AuctionItemID]
		,[AuctionID]
		,[StockID]
		,[BuyerID]
		,[AuctionItemSequenceNumber]
		,[AuctionNumber]
		,[IsLiveInternetBid]
		,[IsPreviousSalePending]
		,[IsRunandDrive]
		,[IsSealedBid]
		,[DoesVehicleStart]
		,[FinalBid]
		,[NextHighestHistoricalBid]
		,[PreviousHistoricalBid]
		,[IsMostRecentAuction]
		,[AuctionSequenceNumber]
		,[IsNoValidBidAmount]
		,[IsSetForSale]
		,[IsProviderAuction]
		,[IsMostRecentAuctionProvider]
		,[NextHighestHistoricalBidProvider]
		,[PreviousHistoricalBidProvider]
		,[AuctionDateTime]
		,[OriginalBid]
		,[OriginalBidProvider]
		,PreBidWinningAmount
		,AuctionLane
		,[ETLLoadID]
		,[ETLLoadUpdateID])	 
	VALUES ([AuctionItemID]
			,[AuctionID]
			,[StockID]
			,[BuyerID]
			,[AuctionItemSequenceNumber]
			,[AuctionNumber]
			,[IsLiveInternetBid]
			,[IsPreviousSalePending]
			,[IsRunandDrive]
			,[IsSealedBid]
			,[DoesVehicleStart]
			,[FinalBid]
			,[NextHighestHistoricalBid]
			,[PreviousHistoricalBid]
			,[IsMostRecentAuction]
			,[AuctionSequenceNumber]
			,[IsNoValidBidAmount]
			,[IsSetForSale]
			,[IsProviderAuction]
			,[IsMostRecentAuctionProvider]
			,[NextHighestHistoricalBidProvider]
			,[PreviousHistoricalBidProvider]
			,[AuctionDateTime]
			,[OriginalBid]
			,[OriginalBidProvider]
			,PreBidWinningAmount
			,AuctionLane
			,@ETLLoadID
			,@ETLLoadID)
	     
OUTPUT $action INTO #temp_AuctionRecordCount;

/*********************************************************************************************
Get the counts from the MERGE operation
*********************************************************************************************/
SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #temp_AuctionRecordCount WITH (NOLOCK)




INSERT INTO #MoreDups
		(
		AuctionItemID ,
		AuctionNumber 
		)
SELECT  edw.dbo.DimAuctionItem.AuctionItemID, AuctionNUmber
 
FROM  edw.dbo.DimAuctionItem   WITH (NOLOCK)
INNER JOIN (SELECT AuctionID,StockID, COUNT(*) as cnt 
FROM edw.dbo.DimAuctionItem  WITH (NOLOCK)
WHERE AuctionDateTime > '1/1/2014' 
GROUP BY AuctionID,StockID 
HAVING COUNT(*) > 1) Dups
ON DimAuctionItem.AuctionID = Dups.AuctionID AND DimAuctionItem.StockID = Dups.StockID
order by DimAuctionItem.StockID,DimAuctionItem.AuctionID
 
DELETE	
FROM	#MoreDups
WHERE	AuctionNumber <> 0

-- select * from #MoreDUps  TRUNCATE TABLE #MOreDups
INSERT INTO #AuctionItemsToDelete
(
AuctionItemID
)
SELECT	AuctionItemID 
FROM	#MoreDups b
WHERE	b.AuctionNumber = 0 
AND		b.AuctionItemID NOT	IN
(
SELECT AuctionITemID
FROM	#AuctionItemsToDelete WITH (NOLOCK)
)


DELETE FROM AI 
FROM dbo.DimAuctionItem AI 
inner join #AuctionItemsToDelete RTD WITH (NOLOCK) 
ON AI.AuctionItemID = RTD.AuctionItemID

SET @DeleteRowCount = @@ROWCOUNT


--MinimumBid Amount

INSERT INTO #TMP_FBD
(StockID,ASAPUpdateDate,AuctionDatetime)
SELECT  FBD.StockID, MAX(ASAPUPdateDate) AS ASAPUPdateDate, AuctionDatetime
FROM EDW.dbo.FactMinimumBidDetail FBD WITH (NOLOCK)
INNER JOIN EDW.dbo.DimAuctionItem DAi WITH (NOLOCK)
ON DAI.StockID = FBD.StockID AND   DAI.AuctionDateTime > FBD.ASAPUPdateDate
WHERE AuctionDatetime >=  @TheDate--( SELECT MinDatetime-3 FROM EDW.dbo.ETLRun ER WITH (NOLOCK) WHERE ETLJobName = 'DailyETLJob') 
GROUP BY FBD.StockID, AuctionDatetime

INSERT INTO #TMP_MINBid
(StockID,AuctionDatetime,ASAPUpdateDate,MinimumBidCurrentAmount)
SELECT
DAI.StockID,DAI. AuctionDatetime , FBD.ASAPUPdateDate,FB.MinimumBidCurrentAmount
FROM 
EDW.dbo.DimAuctionItem DAI WITH (NOLOCK)
INNER JOIN #TMP_FBD FBD
   ON DAI.StockID = FBD.StockID AND   DAI.AuctionDateTime > FBD.ASAPUPdateDate  AND FBD.AuctionDatetime = DAI.AuctionDatetime
INNER JOIN EDW.dbo.FactMinimumBidDetail FB WITH (NOLOCK)
ON FB.StockID = FBD.StocKID  AND FB.ASAPUPdateDate = FBD.ASAPUPdateDate 

UPDATE 
DAI SET 
--SELECT COUNT(*) 
MinimumBidAmount = MinimumBidCurrentAmount
FROM 
EDW.dbo.DimAuctionItem DAI
INNER JOIN #TMP_MINBid T 
ON T.StocKID = DAI.StockID AND T.Auctiondatetime = DAI.AuctionDatetime

/***********Update Live Rep Fields*****************/

UPDATE DAI SET IsHadLiveRepQualified = 1
--SELECT * 
FROM EDW.dbo.DimStock DS
INNER JOIN EDW.dbo.DimAuctionItem DAI WITH (NOLOCK)
ON DS.StockID = DAI.StockID
INNER JOIN (SELECT Distinct [AuctionItemId]
			--FROM [BI_ASAP_Rep].[dbo].[LiveBidHistory]) Q
			FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_LiveBidHistory](@StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5) Q
ON DAI.AuctionItemID = q.AuctionItemID

SET @UpdateRowCount += @@ROWCOUNT	  

/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount, @DeleteRowCount=@DeleteRowCount

		COMMIT TRAN
			DROP TABLE #PreviousBidP
			DROP TABLE #LastProviderEvent
			DROP TABLE #TempLastAuctionP
			DROP TABLE #TempSoldAuctionP
			DROP TABLE #PreviousBid
			DROP TABLE #TempLastAuction
			DROP TABLE #TempSoldAuction
			DROP TABLE #TMP_Auction_Reneged_Canceled
			DROP TABLE #BuyerRenegeCancelEvent
			DROP TABLE #DimAuctionItem
			DROP TABLE #TMPStockID
			DROP TABLE #temp_AuctionRecordCount
			DROP TABLE #AuctionItemsToDelete
			DROP TABLE #RenegedAfterSettlement
		RETURN 0
	END TRY     

	BEGIN CATCH
		SET NOCOUNT OFF
		IF @@TRANCOUNT > 0 
			ROLLBACK TRAN
		
			IF OBJECT_ID('TEMPDB..#PreviousBidP') IS NOT NULL DROP TABLE #PreviousBidP
			IF Object_id('TEMPDB..#LastProviderEvent') IS NOT NULL DROP TABLE #LastProviderEvent
			IF OBJECT_ID('TEMPDB..#TempLastAuctionP') IS NOT NULL DROP TABLE #TempLastAuctionP
			IF OBJECT_ID('TEMPDB..#TempSoldAuctionP') IS NOT NULL DROP TABLE #TempSoldAuctionP
			IF OBJECT_ID('TEMPDB..#PreviousBid') IS NOT NULL DROP TABLE #PreviousBid
			IF OBJECT_ID('TEMPDB..#TempLastAuction') IS NOT NULL DROP TABLE #TempLastAuction
			IF OBJECT_ID('TEMPDB..#TempSoldAuction') IS NOT NULL DROP TABLE #TempSoldAuction
			IF Object_id('TEMPDB..#TMP_Auction_Reneged_Canceled') IS NOT NULL DROP TABLE #TMP_Auction_Reneged_Canceled
			IF Object_id('TEMPDB..#BuyerRenegeCancelEvent') IS NOT NULL DROP TABLE #BuyerRenegeCancelEvent
			IF Object_id('tempdb..#DimAuctionItem') IS NOT NULL DROP TABLE #DimAuctionItem
			IF Object_id('tempdb..#TMPStockID') IS NOT NULL DROP TABLE #TMPStockID
			IF OBJECT_ID('tempdb..#temp_AuctionRecordCount') IS NOT NULL DROP TABLE #temp_AuctionRecordCount
			IF OBJECT_ID('tempdb..#AuctionItemsToDelete') IS NOT NULL DROP TABLE #AuctionItemsToDelete
			IF OBJECT_ID('tempdb..#RenegedAfterSettlement') IS NOT NULL DROP TABLE #RenegedAfterSettlement
		EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ErrorCode = @@ERROR, @UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

		RETURN -1
	END CATCH
	END
