
/*
USE EDW
GO 
============================================================================================================
Author		: Somesh Velupalli 
Create date	: 09/17/2011
Description	: Populate the EDW FactBidSummary table 
===========================================================================================================
REVISION History
ChangeDate	Developer	    Release/Problem Number
09/26/2012	Matt Stelter	Removed un-needed columns and added AuctionItemID
10/01/2012  Somesh V        Removed StockID, AuctionID from all temp tables and used AuctionItemID 
10/05/2012  Somesh V        Used AuctionItemID instead of BidSummaryID which doesnt exist anymore
10/5/2012	Matt Stelter	BidIncrementDetail and BidIncrement Summary are in Temp Tables now
10/16/2012	Matt Stelter	Changed Winning Pre-Bidder to ProxyBidderID
10/30/2012  MAtt Stelter	Changed Bid String
10/31/2012	Matt Stelter	Changed from v_DimBidDetailSummary
11/6/2012	Matt Stelter	Fixed Bid String
11/30/2012	Matt Stelter	Fixed Proxy Bid
4/17/2014   Matt Stelter	Added RingRep
4/18/2016	Matt Stelter	Changess to Runner Up Bid
04/27/2016  Pratyusha Koduru Added RunnerUpBidderId
09/06/2016 Yogitha Alwarsetty Added WinningBidderId
09/22/2016 Pratyusha Koduru  Updated RunnerUpBidderId to Buyer_Bidder format
09/23/2016 Pratyusha Koduru  Added RunnerUpBidderType
============================================================================================================
Usage Example:
Exec usp_ETLFactBidSummary
===========================================================================================================
*/
Create PROCEDURE [dbo].[usp_ETLFactBidSummary]
AS
  DECLARE @ExtractRowCount_sp INT,
          @ETLLoadID_sp       INT,
          @InsertRowCount_sp  INT,
          @UpdateRowCount_sp  INT,
          @CurrentDatetime    DATETIME
          --,@ETLLoadUpdatedID   INT
  DECLARE @Auction_Item_ID   INT,
          @Bid_Amount_String VARCHAR(MAX)

  SET NOCOUNT ON;

  BEGIN TRY
      --Run the Audit Begin
      EXEC @ETLLoadID_sp = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLFactBidDetail'

      SET @CurrentDatetime = (SELECT ER.MaxDatetime
                              FROM   dbo.ETLRun ER WITH (NOLOCK)
                              WHERE  ETLJobName='DailyETLJob')

      --SELECT @ETLLoadUpdatedID=Min(ETLLoadID)
      --FROM   ETLLoadAudit ETLLA
      --WHERE  ETLLA.ExecuteStartDate>@CurrentDatetime-1

      /*******************************************************
      Create Temp Tables
      *******************************************************/
      CREATE TABLE #TMP_Bid_Start_End_Datetime_Source
        (Auction_Item_ID         INT,
         Bid_Event_Day_ID        INT,
         Bid_Item_Start_Datetime DATETIME,
         Bid_Item_End_Datetime   DATETIME,
         Bid_Received_Count      INT,
         Bid_Undo_Count          INT)

      CREATE TABLE #TMP_First_Last_Bid_Datetime
        (Auction_Item_ID     INT,
         Bid_Event_Day_ID    INT,
         First_Bid_Datetime  DATETIME,
         Last_Bid_Datetime   DATETIME,
         MIN_Sequence_Number INT)

      CREATE TABLE #TMP_Bid_Start_End_Datetime
        (Auction_Item_ID         INT,
         Bid_Event_Day_ID        INT,
         Bid_Item_Start_Datetime DATETIME,
         Bid_Item_End_Datetime   DATETIME,
         First_Bid_Datetime      DATETIME,
         Last_Bid_Datetime       DATETIME,
         Bid_Received_Count      INT,
         Bid_Undo_Count          INT,
         Min_Sequence_Number     INT)

      CREATE TABLE #TMP_Bidder_Bid_Count
        (Auction_Item_ID   INT,
         Bid_Event_Day_ID  INT,
         Buyer_Employee_ID INT,
         Bidder_Type       NCHAR(1),
         Bid_Type          NCHAR(1),
         Bidder_Bid_Count  INT)

      CREATE TABLE #TMP_Bidder_Count
        (Auction_Item_ID       INT,
         Bid_Event_Day_ID      INT,
         Total_Bids_Received   INT,
         Internet_Bidder_Count INT,
         Proxy_Bidder_Count    INT,
         Floor_Bidder_Count    INT,
         Rapid_Bidder_Count    INT,
         Total_Bidder_Count    INT)

      CREATE TABLE #TMP_Open_Bid
        (Auction_Item_ID  INT,
         Bid_Event_Day_ID INT,
         Open_Bid_Amount  MONEY)

      CREATE TABLE #TMP_Winning_Bid_Information
        (Auction_Item_ID             INT,
         Bid_Event_Day_ID            INT,
         Bid_End_Datetime            DATETIME,
         Winning_Bidder_Display_Name NVARCHAR(100),
         Winning_Bid_Amount          MONEY,
         Winning_Buyer_ID            INT)

      CREATE TABLE #TMP_No_Bid_Ind
        (Auction_Item_ID  INT,
         Bid_Event_Day_ID INT,
         No_Bid_Ind       INT)

      CREATE TABLE #TMP_Proxy_Bid_Info
        (Auction_Item_ID         INT,
         Proxy_Bid_Ind           INT,
         Proxy_Buyer_Employee_ID INT,
         Proxy_Bid_Amount        MONEY)

      CREATE TABLE #TMP_Bid_Increment_Summary
        ([Auction_Item_ID]            [INT],
         [Min_Bid_Increment_Amount]   [MONEY],
         [Max_Bid_Increment_Amount]   [MONEY],
         [Average_Bid_increment]      [MONEY],
         [Bid_Increment_Change_Count] [INT],
         [Runner_Up_Bid_Amount]       [MONEY],
         [Runner_Up_Bid_Type]         [NCHAR](1))

      CREATE TABLE #TMP_Build_Bid_Amount
        (Auction_Item_ID INT)

      CREATE TABLE #TMP_Stock_Bid_Amount
        (Auction_Item_ID   INT,
         Bid_Amount_String NVARCHAR(MAX))

      CREATE TABLE #TMP_Bid_Detail_Info
        (Auction_Item_ID INT,
         Open_Bid_Amount MONEY,
         No_Bid_Ind      INT,
         Bid_Amounts     VARCHAR(MAX))

      CREATE TABLE #TMP_BID_DETAIL_SUMMARY
        ([Auction_Item_ID]       [INT],
         [Stock_Duration]        [INT],
         [Bid_Duration]          [INT],
         [Bid_End_Datetime]      [DATETIME],
         [Bid_Start_Datetime]    [DATETIME],
         [First_Bid_Datetime]    [DATETIME],
         [Last_Bid_Datetime]     [DATETIME],
         [Internet_Bidder_Count] [INT],
         [Proxy_Bidder_Count]    [INT],
         [Floor_Bidder_Count]    [INT],
         [Rapid_Bidder_Count]    [INT],
         [Total_Bidder_Count]    [INT],
         [Open_Bid_Amount]       [MONEY],
         [Bid_Amounts]           [VARCHAR] (MAX),
         [No_Bid_Ind]            [INT])

      CREATE TABLE #TMP_Bid_Summary
        ([Auction_Sale_Amount]         [MONEY],
         [Auction_Sale_Datetime]       [DATETIME],
         [Auction_Sale_Day_ID]         [INT],
         [Auction_Sale_Status]         [NCHAR](1),
         [Average_Bid_Increment]       [MONEY],
         [Auction_Item_ID]             [INT],
         [Bid_Accepted_Count]          [INT],
         [Bid_Duration]                [DECIMAL](18, 4),
         [Bid_End_Datetime]            [DATETIME],
         [Bid_Received_Count]          [INT],
         [Bid_Proposed_Count]          [INT],
         [Bid_Rejected_Count]          [INT],
         [Bid_Start_Datetime]          [DATETIME],
         [Bidder_Count]                [INT],
         [Buyer_Employee_ID]           [INT],
         [Buyer_ID]                    [INT],
         [Floor_Bidder_Count]          [INT],
         [Internet_Bidder_Count]       [INT],
         [Open_Bid_Amount]             [MONEY],
         [Provider_ID]                 [INT],
         [Proxy_Bidder_Count]          [INT],
         [Rapid_Bidder_Count]          [INT],
         [Sold_Bid_Count]              [INT],
         [Viewer_Count]                [INT],
         [Display_Name]                [NVARCHAR](100),
         [Bid_Undo_Count]              [INT],
         [Accepted_Floor_Bid_Count]    [INT],
         [Accepted_Internet_Bid_Count] [INT],
         [Accepted_Proxy_Bid_Count]    [INT],
         [Accepted_Rapid_Bid_Count]    [INT],
         [Bid_Amounts]                 [VARCHAR](max),
         [Bid_Increment_Change_Ind]    [BIT],
         [Bid_Increment_Change_Count]  [INT],
         [Min_Bid_Increment]           [MONEY],
         [Max_Bid_Increment]           [MONEY],
         [Bid_Restart_Ind]             [BIT],
         [Proxy_Bid_Ind]               [BIT],
         [Proxy_Buyer_Employee_ID]     [INT],
         [Proxy_Bid_Amount]            [MONEY],
         [No_Bid_Ind]                  [BIT],
         [Runner_Up_Bid_Amount]        [MONEY],
         [Runner_Up_Bid_Type]          [CHAR](1),
         [Max_Internet_Bid_Amount]     [MONEY],
         [Max_Floor_Bid_Amount]        [MONEY],
         [Max_Rapid_Bid_Amount]        [MONEY],
         [Max_Proxy_Bid_Amount]        [MONEY],
         [First_Bid_Datetime]          [DATETIME],
         [Last_Bid_Datetime]           [DATETIME],
         [Stock_Duration]              [DECIMAL](18, 4),
         [Awarded_Bid_Count]           [INT],
			RingRep						NVARCHAR(50))

      CREATE TABLE #DimBidIncrementDetail
        (AuctionItemID        INT,
         CurrentBidAmount     MONEY,
         BidStatusCode        NCHAR(1),
         BidStatusDescription VARCHAR(100),
         BidderType           NCHAR(1),
         BidEventDatetime     DATETIME,
         BidEventDayID        INT,
         SequenceNumber       INT,
         SortOrder            INT,
         OccurenceCount       INT,
         BidIncrementAmount   MONEY,
         NewBidAmount         MONEY)

      CREATE TABLE #TMPBidIncrementFilter
        ([AuctionItemID]        [INT],
         [BuyerID]              [INT],
         [BuyerEmployeeID]      [INT],
         [BidLogItem]           [INT],
         [BidAmount]            [MONEY],
         [BidStatusCode]        [NCHAR] (1),
         [BidStatusDescription] [NVARCHAR] (100),
         [BidderType]           [NCHAR] (1),
         [BidEventDatetime]     [DATETIME],
         [BidEventDayID]        [INT],
         [SequenceNumber]       [INT],
         [SortOrder]            [INT])

      CREATE TABLE #TMPFinalBidOffsetResult
        ([AuctionItemID]        [INT],
         [BidAmount]            [MONEY],
         [BidStatusCode]        [NCHAR] (1),
         [BidStatusDescription] [NVARCHAR] (100),
         [BidderType]           [NCHAR] (1),
         [BidEventDatetime]     [DATETIME],
         [SequenceNumber]       [INT],
         [SortOrder]            [INT],
         [OccurenceCount]       [INT])

      CREATE TABLE #TMPFinalBidOffset
        ([AuctionItemID]               [INT],
         [BuyerID]                     [INT],
         [BuyerEmployeeID]             [INT],
         [BidAmount]                   [MONEY],
         [PreviousBidAmount]           [MONEY],
         [BidStatusCode]               [NCHAR] (1),
         [BidStatusDescription]        [NVARCHAR] (100),
         [BidderType]                  [NCHAR] (1),
         [PreviousBidderType]          [NCHAR] (1),
         [BidEventDatetime]            [DATETIME],
         [SequenceNumber]              [INT],
         [SortOrder]                   [INT],
         [OffsetBidAmount]             [MONEY],
         [OffsetBidStatusCode]         [NCHAR] (1),
         [OffsetPreviousBidStatusCode] [NCHAR] (1),
         [OffsetBidStatusDescription]  [NVARCHAR] (100),
         [OffsetBidderType]            [NCHAR] (1),
         [OffsetBidEventDatetime]      [DATETIME],
         [OffsetSequenceNumber]        [INT],
         [OffsetSortOrder]             [INT])

      CREATE TABLE #TMPOverrideUndoBidStatus
        ([AuctionItemID]         [INT],
         [BuyerID]               [INT],
         [BuyerEmployeeID]       [INT],
         [BidAmount]             [MONEY],
         [BidStatusCode]         [NCHAR] (1),
         [PreviousBidStatusCode] [NCHAR] (1),
         [BidStatusDescription]  [NVARCHAR] (100),
         [BidderType]            [NCHAR] (1),
         [PreviousBidderType]    [NCHAR] (1),
         [BidEventDatetime]      [DATETIME],
         [SequenceNumber]        [INT],
         [SortOrder]             [INT])

      CREATE TABLE #TMPReceivedBidStatus
        ([AuctionItemID]            [INT],
         [BuyerID]                  [INT],
         [BuyerEmployeeID]          [INT],
         [BidAmount]                [MONEY],
         [PreviousBidAmount]        [MONEY],
         [BidStatusCode]            [NCHAR] (1),
         [BidStatusDescription]     [NVARCHAR] (100),
         [BidderType]               [NCHAR] (1),
         [PreviousBidderType]       [NCHAR] (1),
         [BidEventDatetime]         [DATETIME],
         [PreviousBidEventDatetime] [DATETIME],
         [SequenceNumber]           [INT],
         [PreviousSequenceNumber]   [INT],
         [SortOrder]                [INT],
         [PreviousSortOrder]        [INT])

      /*********************************************************/
      CREATE TABLE #TMP_Runner_Up_Bid_Results
        ([Bid_Increment_Detail_ID] [INT],
         [Auction_Item_ID]         [INT],
         [Runner_Up_Bid_Amount]    [MONEY],
         [Bid_Status_Code]         [NCHAR] (1),
         [Bid_Status_Description]  [NVARCHAR] (100),
         [Runner_Up_Bid_Type]      [NCHAR] (1),
         [Bid_Event_Datetime]      [DATETIME],
         [Bid_Event_Day_ID]        [INT],
         [Sequence_Number]         [INT],
         [Sort_Order]              [INT],
         [Occurence_Count]         [INT],
         [Bid_Increment_Amount]    [MONEY],
         [New_Bid_Amount]          [MONEY])

      CREATE TABLE #TMP_Runner_Up_Bid_Information_Source
        (Auction_Item_ID            INT,
         Winning_Bid_Event_Datetime DATETIME)

      CREATE TABLE #TMP_Winning_Bid_Detail
        (Auction_Item_ID            INT,
         Winning_Bid_Event_Datetime DATETIME,
         Winning_Bid_Amount         MONEY,
         Winning_Bid_Sort_Order     INT)

      CREATE TABLE #TMP_Runner_Up_Bid_Datetime
        (Auction_Item_ID      INT,
         Runner_Up_Datetime   DATETIME,
         Runner_Up_Bid_Amount MONEY,
         Runner_Up_Sort_Order INT)

      CREATE TABLE #FactBidIncrementSummary
        ([AuctionItemID]           [INT] NULL,
         [BidIncrementAmount]      [MONEY] NULL,
         [TotalBidIncrementAmount] [MONEY] NULL,
         [IncrementCount]          [INT] NULL,
         [RunnerUpBidAmount]       [MONEY] NULL,
         [RunnerUpBidType]         [NCHAR](1) NULL,
         [ETLLoadID]               [INT] NULL,
         [ETLLoadUpdateID]         [INT] NULL)

      CREATE TABLE #DimSoldDetail
      ([SoldDetailID] [int] NULL,
		[SoldLogItem] [int] NULL,
		[SaleAmount] [money] NULL,
		[AuctionItemID] [int] NULL,
		[SaleEventDatetime] [datetime] NULL,
		[SaleEventDayID] [int] NULL,
		[SaleStatusCode] [nchar](1) NULL,
		[BuyerEmployeeID] [int] NULL,
		[BidderType] [nchar](1) NULL,
		[BuyerID] [int] NULL,
		[SoldLogRowID] [int] NULL,
	    RingRep nvarchar(50))
      
      CREATE TABLE #DimAttendeeDetail
	  (	[AttendeeDetailID] [int] NOT NULL,
		[AutoReconnect] [bit] NULL,
		[BuyerEmployeeID] [int] NULL,
		[BranchNumber] [int] NULL,
		[BuyerID] [int] NULL,
		[LoginDatetime] [datetime] NULL,
		[LoginDayID] [int] NULL,
		[LoginDuration] [decimal](38, 4) NULL,
		[LoginName] [varchar](50) NULL,
		[LoginStatusType] [char](1) NULL,
		[LogoutDatetime] [datetime] NULL,
		[LogoutDayID] [int] NULL,
		[ProperLogout] [bit] NULL,
		[AuctionID] [int] NULL,
		[IPAddress] [nvarchar](80) NULL,
		[DisplayLocation] [nvarchar](50) NULL,
		[IPNumber] [bigint] NULL,
		[LocID] [int] NULL,
		[LoginLogID] [int] NULL,
		[DeviceType] [nvarchar](50) NULL,
		[AuctionLane] [varchar](1) NULL)
		
      /*******************************************************
      GET BID DURATION
      *******************************************************/
      INSERT INTO #TMP_Bid_Start_End_Datetime_Source
                  (Auction_Item_ID,Bid_Event_Day_ID,Bid_Item_Start_Datetime,Bid_Item_End_Datetime,Bid_Received_Count,Bid_Undo_Count)
      SELECT BDF.AuctionItemID,
             BDF.BidEventDayID,
             Min(BDF.BidEventDatetime) AS Bid_Item_Start_Datetime,
             Max(BDF.BidEventDatetime) AS Bid_Item_End_Datetime,
             Sum(CASE
                   WHEN BDF.BidStatusCode='K' THEN 1
                   ELSE 0
                 END) AS Bid_Received_Count,
             Sum(CASE
                   WHEN BDF.BidStatusCode='U' THEN 1
                   ELSE 0
                 END) AS Bid_Undo_Count
      FROM   dbo.DimBidDetail AS BDF WITH (NOLOCK)
            WHERE  BDF.BideventDatetime>=Dateadd(D, -3, @CurrentDatetime) AND IsBidDetailShow = 1
      GROUP  BY BDF.AuctionItemID,
                BDF.BidEventDayID

      INSERT INTO #TMP_First_Last_Bid_Datetime
                  (Auction_Item_ID,Bid_Event_Day_ID,First_Bid_Datetime,Last_Bid_Datetime,MIN_Sequence_Number)
      SELECT BDF.AuctionItemID,
             BDF.BidEventDayID,
             Min(BDF.BidEventDatetime) AS First_Bid_Datetime,
             Max(BDF.BidEventDatetime) AS Last_Bid_Datetime,
             Min(BDF.SequenceNumber) AS MIN_Sequence_Number
      FROM   dbo.DimBidDetail AS BDF WITH (NOLOCK)
      WHERE  BDF.BidStatusCode='K'
             AND BDF.BideventDatetime>=Dateadd(D, -3, @CurrentDatetime) AND IsBidDetailShow = 1
      GROUP  BY BDF.AuctionItemID,
                BDF.BidEventDayID

      INSERT INTO #TMP_Bid_Start_End_Datetime
                  (Auction_Item_ID,Bid_Event_Day_ID,Bid_Item_Start_Datetime,Bid_Item_End_Datetime,First_Bid_Datetime,Last_Bid_Datetime,
                   Bid_Received_Count,Bid_Undo_Count,
                   Min_Sequence_Number)
      SELECT TBSEDS.Auction_Item_ID,
             TBSEDS.Bid_Event_Day_ID,
             TBSEDS.Bid_Item_Start_Datetime,
             TBSEDS.Bid_Item_End_Datetime,
             TFLBD.First_Bid_Datetime,
             TFLBD.Last_Bid_Datetime,
             TBSEDS.Bid_Received_Count,
             TBSEDS.Bid_Undo_Count,
             TFLBD.Min_Sequence_Number
      FROM   #TMP_Bid_Start_End_Datetime_Source AS TBSEDS WITH (NOLOCK)
             LEFT OUTER JOIN #TMP_First_Last_Bid_Datetime AS TFLBD WITH (NOLOCK)
               ON TBSEDS.Auction_Item_ID=TFLBD.Auction_Item_ID
                  AND TBSEDS.Bid_Event_Day_ID=TFLBD.Bid_Event_Day_ID



      /*******************************************************
      2. GET UNIQUE BID BIDDER COUNT
      *******************************************************/
      INSERT INTO #TMP_Bidder_Bid_Count
                  (Auction_Item_ID,Bid_Event_Day_ID,Buyer_Employee_ID,Bidder_Type,Bid_Type,Bidder_Bid_Count)
      SELECT BDF.AuctionItemID,
             BDF.BidEventDayID,
             BDF.BuyerEmployeeID,
             BDF.BidType,
             BDF.BidderType,
             Count(*) AS Bidder_Bid_Count
      FROM   dbo.DimBidDetail AS BDF WITH (NOLOCK)
      WHERE  BDF.BidderType IS NOT NULL AND IsBidDetailShow = 1 AND BDF.BideventDatetime>=Dateadd(D, -3, @CurrentDatetime)
      GROUP  BY BDF.AuctionItemID,
                BDF.BidEventDayID,
                BDF.BuyerEmployeeID,
                BDF.BidType,
                BDF.BidderType

      INSERT INTO #TMP_Bidder_Count
                  (Auction_Item_ID,Bid_Event_Day_ID,Total_Bids_Received,Internet_Bidder_Count,Proxy_Bidder_Count,Floor_Bidder_Count,Rapid_Bidder_Count
                   ,
                   Total_Bidder_Count)
      SELECT TBBC.Auction_Item_ID,
             TBBC.Bid_Event_Day_ID,
             Sum(TBBC.Bidder_Bid_Count) AS Total_Bids_Received,
             Sum(CASE
                   WHEN TBBC.Bidder_Type='I' THEN 1
                   ELSE 0
                 END) AS Internet_Bidder_Count,
             Sum(CASE
                   WHEN TBBC.Bidder_Type='P' THEN 1
                   ELSE 0
                 END) AS Proxy_Bidder_Count,
             Sum(CASE
                   WHEN TBBC.Bidder_Type='F' THEN 1
                   ELSE 0
                 END) AS Floor_Bidder_Count,
             Sum(CASE
                   WHEN TBBC.Bid_Type='R'
                        AND TBBC.Bidder_Type='I' THEN 1
                   ELSE 0
                 END) AS Rapid_Bidder_Count,
             Count(*) AS Total_Bidder_Count
      FROM   #TMP_Bidder_Bid_Count AS TBBC WITH (NOLOCK)
      GROUP  BY TBBC.Auction_Item_ID,
                TBBC.Bid_Event_Day_ID

      /*******************************************************
      4. GET OPEN BID
      *******************************************************/
      INSERT INTO #TMP_Open_Bid
                  (Auction_Item_ID,Bid_Event_Day_ID,Open_Bid_Amount)
      SELECT DISTINCT BDF.AuctionItemID,
                      BDF.BidEventDayID,
                      Sum(CASE
                            WHEN BDF.BidStatusCode='K'
                                 AND BDF.BidEventDatetime=Bid_Start.First_Bid_Datetime
                                 AND BDF.SequenceNumber=Bid_Start.Min_Sequence_Number THEN BDF.BidAmount
                            ELSE 0
                          END) AS Open_Bid_Amount
      FROM   dbo.DimBidDetail BDF WITH (NOLOCK)
             INNER JOIN #TMP_Bid_Start_End_Datetime AS Bid_Start WITH (NOLOCK)
               ON BDF.BidEventDayID=Bid_Start.Bid_Event_Day_ID
               AND BDF.AuctionItemID=Bid_Start.Auction_Item_ID
      WHERE  IsBidDetailShow = 1         
      GROUP  BY BDF.AuctionItemID,
                BDF.BidEventDayID

      /*******************************************************
      5. GET WINNING BID INFORMATION
      *******************************************************/
      INSERT INTO #TMP_Winning_Bid_Information
                  (Auction_Item_ID,Bid_Event_Day_ID,Bid_End_Datetime,Winning_Bidder_Display_Name,Winning_Bid_Amount,Winning_Buyer_ID)
      SELECT BDF.AuctionItemID,
             BDF.BidEventDayID,
             BDF.BidEventDatetime AS Bid_End_Datetime,
             BDF.DisplayName AS Winning_Bidder_Display_Name,
             BDF.BidAmount AS Winning_Bid_Amount,
             BDF.BuyerID AS Winning_Buyer_ID
      FROM   dbo.DimBidDetail BDF WITH (NOLOCK)
             INNER JOIN #TMP_Bid_Start_End_Datetime AS Bid_Start WITH (NOLOCK)
               ON BDF.AuctionItemID=Bid_Start.Auction_Item_ID
                  AND BDF.BidEventDatetime=Bid_Start.Bid_Item_End_Datetime
      WHERE  BDF.BidStatusCode='C' AND IsBidDetailShow = 1

      /*******************************************************
      6. GET NO BID INDICATOR
      *******************************************************/
      INSERT INTO #TMP_No_Bid_Ind
                  (Auction_Item_ID,Bid_Event_Day_ID,No_Bid_Ind)
      SELECT Auction_Item_ID,
             Bid_Event_Day_ID,
             CASE
               WHEN Isnull(Bid_Received_Count, 0)-Isnull(Bid_Undo_Count, 0)=0 THEN 1
               ELSE 0
             END AS No_Bid_Ind
      FROM   #TMP_Bid_Start_End_Datetime AS Get_No_Bid WITH (NOLOCK)

      /*******************************************************
      7. GET PROXY BID INDICATOR
      *******************************************************/
      --INSERT INTO #TMP_Proxy_Bid_Info
      --            (Auction_Item_ID,Proxy_Bid_Ind,Proxy_Buyer_Employee_ID,Proxy_Bid_Amount)
      --SELECT BDF.AuctionItemID,
      --       1 AS Proxy_Bid_Ind,
      --       BDF.BuyerEmployeeID AS Proxy_Buyer_Employee_ID,--BDF.BuyerID AS Proxy_Buyer_Employee_ID,
      --       Min(BDF.BidAmount) AS Proxy_Bid_Amount
      --FROM   dbo.DimBidDetail BDF WITH (NOLOCK)
      --WHERE  BDF.BidderType='P'
      --       AND BDF.BideventDatetime>=Dateadd(D, -3, @CurrentDatetime) AND IsBidDetailShow = 1
      --       --AND BDF.ETLLoadUpdateID>=@ETLLoadUpdatedID
      --GROUP  BY BDF.AuctionItemID,
      --          BDF.BuyerEmployeeID

      INSERT INTO #TMP_Proxy_Bid_Info
                  (Auction_Item_ID,Proxy_Bid_Ind,Proxy_Buyer_Employee_ID,Proxy_Bid_Amount)
      SELECT BDF.AuctionItemID,
             1 AS Proxy_Bid_Ind,
             MIN(BDF.BuyerEmployeeID) AS Proxy_Buyer_Employee_ID,
             Min(BDF.BidAmount) AS Proxy_Bid_Amount
      FROM   dbo.DimBidDetail BDF WITH (NOLOCK)
		INNER JOIN       (SELECT BDF.AuctionItemID,
             1 AS Proxy_Bid_Ind,
             Min(BDF.BidAmount) AS Proxy_Bid_Amount
      FROM   dbo.DimBidDetail BDF WITH (NOLOCK)
      WHERE  BDF.BidderType='P'
             AND BDF.BideventDatetime>=Dateadd(D, -3, @CurrentDatetime) AND IsBidDetailShow = 1
      GROUP  BY BDF.AuctionItemID) MinBid
      ON BDF.AuctionItemID = MinBid.AuctionItemID and BDF.BidAmount = MinBid.Proxy_Bid_Amount
      WHERE  BDF.BidderType='P'
             AND BDF.BideventDatetime>=Dateadd(D, -3, @CurrentDatetime) AND IsBidDetailShow = 1
      GROUP  BY BDF.AuctionItemID


      /************************************************************************************************************************
      8. BUILD BID INCREMENT INFORMATION
      ************************************************************************************************************************/
      INSERT INTO #TMPBidIncrementFilter
                  (AuctionItemID,BuyerID,BuyerEmployeeID,BidLogItem,BidAmount,BidStatusCode,BidStatusDescription,BidderType,BidEventDatetime,
                   BidEventDayID,SequenceNumber
                   ,SortOrder)
      SELECT BDF.AuctionItemID,
             BDF.BuyerID,
             BDF.BuyerEmployeeID,
             BDF.BidLogItem,
             BDF.BidAmount,
             BDF.BidStatusCode,
             BDF.BidStatusDescription,
             BDF.BidderType,
             BDF.BidEventDatetime,
             BDF.BidEventDayID,
             BDF.SequenceNumber,
             BDF.SortOrder
      FROM   dbo.DimBidDetail AS BDF WITH (NOLOCK)
      WHERE  BDF.BidStatusCode IN ('K', 'U', 'O')
         AND BDF.BidEventDatetime>=Dateadd(D, -3, @CurrentDatetime) AND IsBidDetailShow = 1
      ORDER  BY BDF.AuctionItemID,
                BDF.BidEventDatetime

      CREATE INDEX TMPIDXBidIncrement
        ON #TMPBidIncrementFilter(AuctionItemID, BidStatusCode, SortOrder)

      INSERT INTO #TMPReceivedBidStatus
                  (AuctionItemID,BuyerID,BuyerEmployeeID,BidAmount,PreviousBidAmount,BidStatusCode,BidStatusDescription,BidderType,PreviousBidderType,
                   BidEventDatetime,
                   PreviousBidEventDatetime,SequenceNumber,PreviousSequenceNumber,SortOrder,PreviousSortOrder)
      SELECT TBIS.AuctionItemID,
             TBIS.BuyerID,
             TBIS.BuyerEmployeeID,
             TBIS.BidAmount,
             TBISPreviousRecord.BidAmount AS PreviousBidAmount,
             TBIS.BidStatusCode,
             TBIS.BidStatusDescription,
             TBIS.BidderType,
             TBISPreviousRecord.BidderType AS PreviousBidderType,
             TBIS.BidEventDatetime,
             TBISPreviousRecord.BidEventDatetime AS PreviousBidEventDatetime,
             TBIS.SequenceNumber,
             TBISPreviousRecord.SequenceNumber AS PreviousSequenceNumber,
             TBIS.SortOrder,
             TBISPreviousRecord.SortOrder AS PreviousSortOrder
      FROM   #TMPBidIncrementFilter AS TBIS WITH (NOLOCK)
             LEFT OUTER JOIN #TMPBidIncrementFilter AS TBISPreviousRecord WITH (NOLOCK)
               ON TBIS.AuctionItemID=TBISPreviousRecord.AuctionItemID
                  AND TBISPreviousRecord.SortOrder=(SELECT Max(SortOrder)
                                                    FROM   #TMPBidIncrementFilter WITH (NOLOCK)
                                                    WHERE  SortOrder<TBIS.SortOrder
                                                       AND AuctionItemID=TBIS.AuctionItemID
                                                       AND BidStatusCode='K')
      WHERE  TBIS.BidStatusCode='K'
         AND TBIS.BidEventDatetime>=Dateadd(D, -3, @CurrentDatetime)
      ORDER  BY TBIS.AuctionItemID,
                TBIS.SortOrder

      INSERT INTO #TMPOverrideUndoBidStatus
                  (AuctionItemID,BuyerID,BuyerEmployeeID,BidAmount,BidStatusCode,PreviousBidStatusCode,BidStatusDescription,BidderType,
                   PreviousBidderType,
                   BidEventDatetime,SequenceNumber,SortOrder)
      SELECT TBIS.AuctionItemID,
             TBIS.BuyerID,
             TBIS.BuyerEmployeeID,
             TBIS.BidAmount,
             TBIS.BidStatusCode,
             TBISPreviousRecord.BidStatusCode AS PreviousBidStatusCode,
             TBIS.BidStatusDescription,
             TBIS.BidderType,
             TBISPreviousRecord.BidderType AS PreviousBidderType,
             TBIS.BidEventDatetime,
             TBIS.SequenceNumber,
             TBIS.SortOrder
      FROM   #TMPBidIncrementFilter AS TBIS WITH (NOLOCK)
             LEFT OUTER JOIN #TMPBidIncrementFilter AS TBISPreviousRecord WITH (NOLOCK)
               ON TBIS.AuctionItemID=TBISPreviousRecord.AuctionItemID
                  AND TBISPreviousRecord.SortOrder=(SELECT Max(SortOrder)
                                                    FROM   #TMPBidIncrementFilter WITH (NOLOCK)
                                                    WHERE  SortOrder<TBIS.SortOrder
                                                       AND AuctionItemID=TBIS.AuctionItemID
                                                       AND BidStatusCode IN ('O', 'U'))
      WHERE  TBIS.BidStatusCode IN ('O', 'U')
      ORDER  BY TBIS.AuctionItemID,
                TBIS.SortOrder

      INSERT INTO #TMPFinalBidOffset
                  (AuctionItemID,BuyerID,BuyerEmployeeID,BidAmount,PreviousBidAmount,BidStatusCode,BidStatusDescription,BidderType,PreviousBidderType,
                   BidEventDatetime,
                   SequenceNumber,SortOrder,OffsetBidAmount,OffsetBidStatusCode,OffsetPreviousBidStatusCode,OffsetBidStatusDescription,
                   OffsetBidderType,
                   OffsetBidEventDatetime,OffsetSequenceNumber,OffsetSortOrder)
      SELECT TRBS.AuctionItemID,
             TRBS.BuyerID,
             TRBS.BuyerEmployeeID,
             TRBS.BidAmount,
             TRBS.PreviousBidAmount,
             TRBS.BidStatusCode,
             TRBS.BidStatusDescription,
             TRBS.BidderType,
             TRBS.PreviousBidderType,
             TRBS.BidEventDatetime,
             TRBS.SequenceNumber,
             TRBS.SortOrder,
             TOUBS.BidAmount AS OffsetBidAmount,
             TOUBS.BidStatusCode AS OffsetBidStatusCode,
             TOUBS.PreviousBidStatusCode AS OffsetPreviousBidStatusCode,
             TOUBS.BidStatusDescription AS OffsetBidStatusDescription,
             TOUBS.BidderType AS OffsetBidderType,
             TOUBS.BidEventDatetime AS OffsetBidEventDatetime,
             TOUBS.SequenceNumber AS OffsetSequenceNumber,
             TOUBS.SortOrder AS OffsetSortOrder
      FROM   #TMPReceivedBidStatus AS TRBS WITH (NOLOCK)
             LEFT OUTER JOIN #TMPOverrideUndoBidStatus AS TOUBS WITH (NOLOCK)
               ON TRBS.AuctionItemID=TOUBS.AuctionItemID
                  AND TOUBS.SortOrder=(SELECT Min(SortOrder)
                                       FROM   #TMPOverrideUndoBidStatus WITH (NOLOCK)
                                       WHERE  AuctionItemID=TRBS.AuctionItemID
                                          AND SortOrder>TRBS.SortOrder
                                          AND BidAmount BETWEEN TRBS.PreviousBidAmount AND TRBS.BidAmount)
                  AND (CASE
                         WHEN TRBS.PreviousBidderType='I'
                              AND TOUBS.PreviousBidStatusCode='O' THEN 0
                         ELSE 1
                       END)=1

      INSERT INTO #TMPFinalBidOffsetResult
                  (AuctionItemID,BidAmount,BidStatusCode,BidStatusDescription,BidderType,BidEventDatetime,SequenceNumber,SortOrder,OccurenceCount)
      SELECT AuctionItemID,
             BidAmount,
             BidStatusCode,
             BidStatusDescription,
             BidderType,
             Max(BidEventDatetime) AS BidEventDatetime,
             Max(SequenceNumber) AS SequenceNumber,
             Max(SortOrder) AS SortOrder,
             Count(*) AS OccurenceCount
      FROM   #TMPFinalBidOffset WITH (NOLOCK)
      WHERE  OffsetSortOrder IS NULL
      GROUP  BY AuctionItemID,
                BidAmount,
                BidStatusCode,
                BidStatusDescription,
                BidderType

      INSERT INTO #DimBidIncrementDetail
                  (AuctionItemID,CurrentBidAmount,BidStatusCode,BidStatusDescription,BidderType,BidEventDatetime,BidEventDayID,SequenceNumber,
                   SortOrder,
                   OccurenceCount,
                   BidIncrementAmount,NewBidAmount)
      SELECT TFBOR.AuctionItemID,
             TFBOR.BidAmount AS CurrentBidAmount,
             TFBOR.BidStatusCode,
             TFBOR.BidStatusDescription,
             TFBOR.BidderType,
             TFBOR.BidEventDatetime,
             CONVERT(CHAR(8), TFBOR.BidEventDatetime, 112) AS BidEventDayID,
             TFBOR.SequenceNumber,
             TFBOR.SortOrder,
             TFBOR.OccurenceCount,
             CASE
               WHEN TFBORIncrement.BidAmount IS NOT NULL THEN TFBORIncrement.BidAmount-TFBOR.BidAmount
               ELSE 0
             END AS BidIncrementAmount,
             TFBORIncrement.BidAmount AS NewBidAmount
      FROM   #TMPFinalBidOffsetResult AS TFBOR WITH (NOLOCK)
             LEFT OUTER JOIN #TMPFinalBidOffsetResult AS TFBORIncrement WITH (NOLOCK)
               ON TFBOR.AuctionItemID=TFBORIncrement.AuctionItemID
                  AND TFBORIncrement.SortOrder=(SELECT Min(SortOrder)
                                                FROM   #TMPFinalBidOffsetResult WITH (NOLOCK)
                                                WHERE  AuctionItemID=TFBOR.AuctionItemID
                                                   AND SortOrder>TFBOR.SortOrder)
      ORDER  BY TFBOR.AuctionItemID,
                TFBOR.BidEventDatetime

      /****************************************************************************************************************************************
      BidIncrementSummary
      ****************************************************************************************************************************************/
      INSERT INTO #TMP_Runner_Up_Bid_Information_Source
                  (Auction_Item_ID,Winning_Bid_Event_Datetime)
      SELECT AuctionItemID,
             Max(BidEventDatetime) AS Winning_Bid_Event_Datetime
      FROM   #DimBidIncrementDetail AS BID WITH (NOLOCK)
      WHERE  BID.BidEventDatetime>=Dateadd(D, -3, @CurrentDatetime)
      GROUP  BY AuctionItemID

      INSERT INTO #TMP_Winning_Bid_Detail
                  (Auction_Item_ID,Winning_Bid_Event_Datetime,Winning_Bid_Amount,Winning_Bid_Sort_Order)
      SELECT BID.AuctionItemID,
             Max(BID.BidEventDatetime) AS Winning_Bid_Event_Datetime,
             Max(BID.CurrentBidAmount) AS Winning_Bid_Amount,
             Max(BID.SortOrder) AS Winning_Bid_Sort_Order
      FROM   #DimBidIncrementDetail AS BID WITH (NOLOCK)
             INNER JOIN #TMP_Runner_Up_Bid_Information_Source AS TRUBIS WITH (NOLOCK)
               ON BID.AuctionItemID=TRUBIS.Auction_Item_ID
      WHERE  BID.BidEventDatetime=Winning_Bid_Event_Datetime
      GROUP  BY BID.AuctionItemID

      INSERT INTO #TMP_Runner_Up_Bid_Datetime
                  (Auction_Item_ID,Runner_Up_Datetime,Runner_Up_Bid_Amount,Runner_Up_Sort_Order)
      SELECT BID.AuctionItemID,
             Max(BID.BidEventDatetime) AS Runner_Up_Datetime,
             Max(BID.CurrentBidAmount) AS Runner_Up_Bid_Amount,
             Max(BID.SortOrder) AS Runner_Up_Sort_Order
      FROM   #DimBidIncrementDetail AS BID WITH (NOLOCK)
             INNER JOIN #TMP_Winning_Bid_Detail AS TWBD WITH (NOLOCK)
               ON BID.AuctionItemID=TWBD.Auction_Item_ID
                  AND BID.BidEventDatetime<=TWBD.Winning_Bid_Event_Datetime
                  AND BID.CurrentBidAmount<TWBD.Winning_Bid_Amount
                  AND BID.SortOrder<TWBD.Winning_Bid_Sort_Order
      GROUP  BY BID.AuctionItemID

      INSERT INTO #TMP_Runner_Up_Bid_Results
                  (--Bid_Increment_Detail_ID, 
      Auction_Item_ID,Runner_Up_Bid_Amount,Bid_Status_Code,Bid_Status_Description,Runner_Up_Bid_Type,Bid_Event_Datetime,Bid_Event_Day_ID,
      Sequence_Number
      ,
      Sort_Order,Occurence_Count,Bid_Increment_Amount,New_Bid_Amount)
      SELECT --BID.BidIncrementDetailID,
      BID.AuctionItemID,
      BID.CurrentBidAmount AS Runner_Up_Bid_Amount,
      BID.BidStatusCode,
      BID.BidStatusDescription,
      BID.BidderType AS Runner_Up_Bid_Type,
      BID.BidEventDatetime,
      BID.BidEventDayID,
      BID.SequenceNumber,
      BID.SortOrder,
      BID.OccurenceCount,
      BID.BidIncrementAmount,
      BID.NewBidAmount
      FROM   #DimBidIncrementDetail AS BID WITH (NOLOCK)
             INNER JOIN #TMP_Runner_Up_Bid_Datetime AS TRUBD WITH (NOLOCK)
               ON BID.AuctionItemID=TRUBD.Auction_Item_ID
                  AND BID.SortOrder=TRUBD.Runner_Up_Sort_Order

      --UPDATE BD
      --SET    RunnerUpBidInd=1
      --FROM   dbo.DimBidDetail AS BD WITH (NOLOCK)
      --       INNER JOIN #TMP_Runner_Up_Bid_Results AS TRUBR WITH (NOLOCK)
      --         ON BD.AuctionItemID=TRUBR.Auction_Item_ID
      --            AND BD.SortOrder=TRUBR.Sort_Order
      --            AND BD.SequenceNumber=TRUBR.Sequence_Number
      --            AND BD.BidEventDatetime=TRUBR.Bid_Event_Datetime

      INSERT INTO #FactBidIncrementSummary
                  (AuctionItemID,BidIncrementAmount,TotalBidIncrementAmount,IncrementCount,RunnerUpBidAmount,RunnerUpBidType,ETLLoadID,ETLLoadUpdateID
      )
      SELECT BID.AuctionItemID,
             BID.BidIncrementAmount,
             Sum(BID.BidIncrementAmount) AS Total_Bid_Increment_Amount,
             Count(*) AS Increment_Count,
             Runner_Up_Bid.Runner_Up_Bid_Amount,
             Runner_Up_Bid.Runner_Up_Bid_Type,
             @ETLLoadID_sp,
             @ETLLoadID_sp
      FROM   #DimBidIncrementDetail BID WITH (NOLOCK)
             LEFT OUTER JOIN #FactBidIncrementSummary AS BIS WITH (NOLOCK)
               ON BID.AuctionItemID=BIS.AuctionItemID
             LEFT OUTER JOIN #TMP_Runner_Up_Bid_Results AS Runner_Up_Bid WITH (NOLOCK)
               ON BID.AuctionItemID=Runner_Up_Bid.Auction_Item_ID
      WHERE  BID.NewBidAmount IS NOT NULL
      GROUP  BY BID.AuctionItemID,
                BID.BidIncrementAmount,
                Runner_Up_Bid.Runner_Up_Bid_Amount,
                Runner_Up_Bid.Runner_Up_Bid_Type
      ORDER  BY BID.AuctionItemID

      INSERT INTO #TMP_Bid_Increment_Summary
                  (Auction_Item_ID,Min_Bid_Increment_Amount,Max_Bid_Increment_Amount,Average_Bid_increment,Bid_Increment_Change_Count,
                   Runner_Up_Bid_Amount,
                   Runner_Up_Bid_Type)
      SELECT BIS.AuctionItemID,
             Min(BIS.BidIncrementAmount) AS Min_Bid_Increment_Amount,
             Max(BIS.BidIncrementAmount) AS Max_Bid_Increment_Amount,
             Sum(BIS.TotalBidIncrementAmount)/(Sum(BIS.IncrementCount)) AS Average_Bid_increment,
             (Count(*)-1) AS Bid_Increment_Change_Count,
             BIS.RunnerUpBidAmount,
             BIS.RunnerUpBidType
      FROM   #FactBidIncrementSummary AS BIS WITH (NOLOCK)
             INNER JOIN #TMP_Bid_Start_End_Datetime AS TMP_Bid_Duration WITH (NOLOCK)
               ON BIS.AuctionItemID=TMP_Bid_Duration.Auction_Item_ID
      GROUP  BY BIS.AuctionItemID,
                BIS.RunnerUpBidAmount,
                BIS.RunnerUpBidType

      /************************************************************************************************************************
      9. BUILD STRING OF BID AMOUNTS
      ************************************************************************************************************************/
      INSERT INTO #TMP_Build_Bid_Amount
                  (Auction_Item_ID)
      SELECT DISTINCT AuctionItemID
      FROM   dbo.DimBidDetail AS BDF WITH (NOLOCK)
      WHERE  BDF.BidStatusCode='K'
             AND BDF.BideventDatetime>=Dateadd(D, -3, @CurrentDatetime) AND IsBidDetailShow = 1
      ORDER  BY AuctionItemID

      SET @Bid_Amount_String = ''

      DECLARE Stock_Bid_Amounts CURSOR FOR
        SELECT Auction_Item_ID
        FROM   #TMP_Build_Bid_Amount WITH (NOLOCK)
        ORDER  BY Auction_Item_ID

      OPEN Stock_Bid_Amounts;

      FETCH NEXT FROM Stock_Bid_Amounts INTO @Auction_Item_ID

      WHILE @@FETCH_STATUS=0 BEGIN
            SET @Bid_Amount_String = ''

            SELECT @Bid_Amount_String=@Bid_Amount_String+CASE
                                                           WHEN @Bid_Amount_String='' THEN '$'
                                                           ELSE ', $'
                                                         END+Cast(BD.BidAmount AS VARCHAR(50))
            FROM   dbo.DimBidDetail AS BD WITH (NOLOCK)
            WHERE  BD.AuctionItemID=@Auction_Item_ID
               AND BD.BidStatusCode='K' AND IsBidDetailShow = 1
            ORDER  BY BD.AuctionItemID,
                      BD.SortOrder DESC

            INSERT INTO #TMP_Stock_Bid_Amount
                        (Auction_Item_ID,Bid_Amount_String)
            SELECT @Auction_Item_ID,
                   @Bid_Amount_String

            FETCH NEXT FROM Stock_Bid_Amounts INTO @Auction_Item_ID
        END

      CLOSE Stock_Bid_Amounts;

      DEALLOCATE Stock_Bid_Amounts;

      /***************************************************************************************
      10. BUILD BID_DETAIL_INFO 
      ***************************************************************************************/
      INSERT INTO #TMP_Bid_Detail_Info
                  (Auction_Item_ID,Open_Bid_Amount,No_Bid_Ind,Bid_Amounts)
      SELECT Open_Bid.Auction_Item_ID,
             Open_Bid.Open_Bid_Amount,
             No_Bid_Ind.No_Bid_Ind,
             Isnull(TSBA.Bid_Amount_String, '$0.00') AS Bid_Amounts
      FROM   #TMP_Open_Bid AS Open_Bid WITH (NOLOCK)
             INNER JOIN #TMP_No_Bid_Ind AS No_Bid_Ind WITH (NOLOCK)
               ON Open_Bid.Auction_Item_ID=No_Bid_Ind.Auction_Item_ID
             LEFT OUTER JOIN #TMP_Stock_Bid_Amount AS TSBA WITH (NOLOCK)
               ON Open_Bid.Auction_Item_ID=TSBA.Auction_Item_ID

      /***************************************************************************************
      10a. BUILD BID_DETAIL_SUMMARY 
      ***************************************************************************************/
      INSERT INTO #TMP_BID_DETAIL_SUMMARY
                  (Auction_Item_ID,Stock_Duration,Bid_Duration,Bid_End_Datetime,Bid_Start_Datetime,First_Bid_Datetime,Last_Bid_Datetime,
                   Internet_Bidder_Count,
                   Proxy_Bidder_Count,Floor_Bidder_Count,Rapid_Bidder_Count,Total_Bidder_Count,Open_Bid_Amount,Bid_Amounts,No_Bid_Ind)
      SELECT TMP_Bid_Duration.Auction_Item_ID,
             (Datediff(SS, TMP_Bid_Duration.Bid_Item_Start_Datetime, TMP_Bid_Duration.Bid_Item_End_Datetime)) AS Stock_Duration,
             CASE
               WHEN TMP_Bid_Duration.First_Bid_Datetime IS NOT NULL
                    AND TMP_Bid_Duration.Last_Bid_Datetime IS NOT NULL THEN (Datediff(SS, TMP_Bid_Duration.First_Bid_Datetime,
                                                                             TMP_Bid_Duration.Last_Bid_Datetime))
               ELSE 0
             END AS Bid_Duration,
             (TMP_Bid_Duration.Bid_Item_End_Datetime) AS Bid_End_Datetime,
             (TMP_Bid_Duration.Bid_Item_Start_Datetime) AS Bid_Start_Datetime,
             (TMP_Bid_Duration.First_Bid_Datetime) AS First_Bid_Datetime,
             (TMP_Bid_Duration.Last_Bid_Datetime) AS Last_Bid_Datetime,
             Bidder_Count.Internet_Bidder_Count,
             Bidder_Count.Proxy_Bidder_Count,
             Bidder_Count.Floor_Bidder_Count,
             Bidder_Count.Rapid_Bidder_Count,
             Bidder_Count.Total_Bidder_Count,
             Isnull(TBDI.Open_Bid_Amount, 0) AS Open_Bid_Amount,
             Isnull(TBDI.Bid_Amounts, 0) AS Bid_Amounts,
             Isnull(TBDI.No_Bid_Ind, 1) AS No_Bid_Ind
      FROM   #TMP_Bid_Start_End_Datetime AS TMP_Bid_Duration WITH (NOLOCK)
             LEFT OUTER JOIN #TMP_Bid_Detail_Info AS TBDI WITH (NOLOCK)
               ON TMP_Bid_Duration.Auction_Item_ID=TBDI.Auction_Item_ID
             LEFT OUTER JOIN #TMP_Bidder_Count AS Bidder_Count WITH (NOLOCK)
               ON TMP_Bid_Duration.Auction_Item_ID=Bidder_Count.Auction_Item_ID

      CREATE INDEX TMP_IDX_BID_DETAIL_SUMMARY
        ON #TMP_BID_DETAIL_SUMMARY(Auction_Item_ID)

INSERT INTO #DimAttendeeDetail(AttendeeDetailID,
       AutoReconnect,
       BuyerEmployeeID,
       BranchNumber,
       BuyerID,
       LoginDatetime,
       LoginDayID,
       LoginDuration,
       LoginName,
       LoginStatusType,
       LogoutDatetime,
       LogoutDayID,
       ProperLogout,
       AuctionID,
       IPAddress,
       DisplayLocation,
       IPNumber,
       LocID,
       LoginLogID,
       DeviceType,
       AuctionLane)
SELECT AttendeeDetailID,
       AutoReconnect,
       BuyerEmployeeID,
       BranchNumber,
       BuyerID,
       LoginDatetime,
       LoginDayID,
       LoginDuration,
       LoginName,
       LoginStatusType,
       LogoutDatetime,
       LogoutDayID,
       ProperLogout,
       AuctionID,
       IPAddress,
       DisplayLocation,
       IPNumber,
       LocID,
       LoginLogID,
       DeviceType,
       AuctionLane
FROM   dbo.DimAttendeeDetail WITH (NOLOCK)
WHERE  (LoginDatetime>=Dateadd(D, -3, @CurrentDatetime)) 

CREATE INDEX TMP_IDX_DimAttendeeDetail ON #DimAttendeeDetail(BuyerEmployeeID,LoginDatetime,LogoutDatetime)

INSERT INTO #DimSoldDetail(SoldDetailID,
       SoldLogItem,
       SaleAmount,
       AuctionItemID,
       SaleEventDatetime,
       SaleEventDayID,
       SaleStatusCode,
       BuyerEmployeeID,
       BidderType,
       BuyerID,
       SoldLogRowID,
	   RingRep) 
SELECT SoldDetailID,
       SoldLogItem,
       SaleAmount,
       AuctionItemID,
       SaleEventDatetime,
       SaleEventDayID,
       SaleStatusCode,
       BuyerEmployeeID,
       BidderType,
       BuyerID,
       SoldLogRowID,
	   RingRep
FROM   dbo.DimSoldDetail WITH (NOLOCK)
WHERE  (SaleEventDatetime>=Dateadd(D, -3, @CurrentDatetime)) 
 
CREATE INDEX TMP_IDX_DimSoldDetail ON #DimSoldDetail(AuctionItemID)

      CREATE INDEX TMP_IDX_TMP_Bid_Increment_Summary ON #TMP_Bid_Increment_Summary (Auction_Item_ID)
      CREATE INDEX TMP_IDX_TMP_Proxy_Bid_Info ON #TMP_Proxy_Bid_Info (Auction_Item_ID)
      CREATE INDEX TMP_IDX_TMP_Winning_Bid_Information ON #TMP_Winning_Bid_Information (Auction_Item_ID)
                            
      /*******************************************************
      11. INSERT INTO Bid_Summary
      *******************************************************/
      INSERT INTO #TMP_Bid_Summary
                  (Auction_Sale_Amount,Auction_Sale_Datetime,--Auction_Sale_Day_ID,
                   Auction_Sale_Status,Average_Bid_Increment,--Auction_ID,
                   Bid_Accepted_Count,Bid_Duration,Stock_Duration,Bid_End_Datetime,Bid_Received_Count,Bid_Proposed_Count,Bid_Rejected_Count,
                   Bid_Start_Datetime,First_Bid_Datetime,Last_Bid_Datetime,Bidder_Count,Buyer_Employee_ID,--Branch_Number,
                   Buyer_ID,Floor_Bidder_Count,Internet_Bidder_Count,Open_Bid_Amount,--Provider_ID,
                   Proxy_Bidder_Count,Rapid_Bidder_Count,Sold_Bid_Count,Awarded_Bid_Count,--Stock_ID,
                   Viewer_Count,Display_Name,Bid_Undo_Count,Accepted_Floor_Bid_Count,Accepted_Internet_Bid_Count,Accepted_Proxy_Bid_Count,
                   Accepted_Rapid_Bid_Count,Bid_Amounts,Bid_Increment_Change_Ind,Bid_Increment_Change_Count,Min_Bid_Increment,Max_Bid_Increment,
                   Bid_Restart_Ind,Proxy_Bid_Ind,Proxy_Buyer_Employee_ID,Proxy_Bid_Amount,No_Bid_Ind,Runner_Up_Bid_Amount,Runner_Up_Bid_Type,
                   Max_Internet_Bid_Amount,Max_Floor_Bid_Amount,Max_Rapid_Bid_Amount,Max_Proxy_Bid_Amount,Auction_Item_ID,RingRep)
      SELECT  SD.SaleAmount AS Auction_Sale_Amount,
                      SD.SaleEventDatetime AS Auction_Sale_Datetime,
                      --CONVERT(CHAR(8), SD.SaleEventDatetime, 112) AS Auction_Sale_Day_Id,
                      SD.SaleStatusCode AS Auction_Sale_Status,
                      Isnull(Bid_Increment.Average_Bid_increment, 0) AS Average_Bid_Increment,
                      --BDS.AuctionID,
                      Sum(CASE
                            WHEN BDS.BidStatusCode='K' THEN 1
                            ELSE 0
                          END) AS Bid_Accepted_Count,
                      TBDS.Bid_Duration AS Bid_Duration,
                      TBDS.Stock_Duration AS Stock_Duration,
                      Max(TBDS.Bid_End_Datetime) AS Bid_End_Datetime,
                      Sum(CASE
                            WHEN BDS.BidStatusCode='K' THEN 1
                            ELSE 0
                          END) AS Bid_Received_Count,
                      Sum(CASE
                            WHEN BDS.BidStatusCode='P' THEN 1
                            ELSE 0
                          END) AS Bid_Proposed_Count,
                      Sum(CASE
                            WHEN BDS.BidStatusCode='R' THEN 1
                            ELSE 0
                          END) AS Bid_Rejected_Count,
                      Min(TBDS.Bid_Start_Datetime) AS Bid_Start_Datetime,
                      Min(TBDS.First_Bid_Datetime) AS First_Bid_Datetime,
                      Max(TBDS.Last_Bid_Datetime) AS Last_Bid_Datetime,
                      TBDS.Total_Bidder_Count AS Bidder_Count,
                      SD.BuyerEmployeeID,
                      --BDS.BranchNumber,
                      SD.BuyerID AS Buyer_ID,
                      TBDS.Floor_Bidder_Count,
                      TBDS.Internet_Bidder_Count,
                      TBDS.Open_Bid_Amount,
                      --BDS.ProviderID,
                      TBDS.Proxy_Bidder_Count,
                      TBDS.Rapid_Bidder_Count,
                      Sum(CASE
                            WHEN BDS.BidStatusCode='S' THEN 1
                            ELSE 0
                          END) AS Sold_Bid_Count,
                      Sum(CASE
                            WHEN BDS.BidStatusCode='C' THEN 1
                            ELSE 0
                          END) AS Awarded_Bid_Count,
                      --BDS.StockID,
                      Sum(CASE
                            WHEN AD.LoginStatusType='V' THEN 1
                            ELSE 0
                          END) AS Viewer_Count,
                      Winning_Bid_Info.Winning_Bidder_Display_Name AS Display_Name,
                      Sum(CASE
                            WHEN BDS.BidStatusCode='U' THEN 1
                            ELSE 0
                          END) AS Bid_Undo_Count,
                      Sum(CASE
                            WHEN BDS.BidStatusCode='K'
                                 AND BDS.BidderType='F' THEN 1
                            ELSE 0
                          END) AS Accepted_Floor_Bid_Count,
                      Sum(CASE
                            WHEN BDS.BidStatusCode='K'
                                 AND BDS.BidderType='I' THEN 1
                            ELSE 0
                          END) AS Accepted_Internet_Bid_Count,
                      Sum(CASE
                            WHEN BDS.BidStatusCode='K'
                                 AND BDS.BidderType='P' THEN 1
                            ELSE 0
                          END) AS Accepted_Proxy_Bid_Count,
                      Sum(CASE
                            WHEN BDS.BidStatusCode='K'
                                 AND BDS.BidderType='R' THEN 1
                            ELSE 0
                          END) AS Accepted_Rapid_Bid_Count,
                      TBDS.Bid_Amounts,
                      CASE
                        WHEN Bid_Increment.Bid_Increment_Change_Count>0 THEN 1
                        ELSE 0
                      END AS Bid_Increment_Change_Ind,
                      (Isnull(Bid_Increment.Bid_Increment_Change_Count, 0)) AS Bid_Increment_Change_Count,
                      Isnull(Bid_Increment.Min_Bid_Increment_Amount, 0) AS Min_Bid_Increment,
                      Isnull(Bid_Increment.Max_Bid_Increment_Amount, 0) AS Max_Bid_Increment,
                      Sum(CASE
                            WHEN BDS.BidStatusCode='^' THEN 1
                            ELSE 0
                          END)-1 AS Bid_Restart_Ind,
                      Isnull(Proxy_Bid_Info.Proxy_Bid_Ind, 0) AS Proxy_Bid_Ind,
                      Proxy_Bid_Info.Proxy_Buyer_Employee_ID,
                      Proxy_Bid_Info.Proxy_Bid_Amount AS Proxy_Bid_Amount,
                      TBDS.No_Bid_Ind,
                      Bid_Increment.Runner_Up_Bid_Amount AS Runner_Up_Bid_Amount,
                      Bid_Increment.Runner_Up_Bid_Type AS Runner_Up_Bid_Type,
                      Max(CASE
                            WHEN BDS.BidStatusCode='K'
                                 AND BDS.BidderType='I' THEN BDS.BidAmount
                            ELSE 0
                          END) AS MAX_Internet_Bid_Amount,
                      Max(CASE
                            WHEN BDS.BidStatusCode='K'
                                 AND BDS.BidderType='F' THEN BDS.BidAmount
                            ELSE 0
                          END) AS MAX_Floor_Bid_Amount,
                      Max(CASE
                            WHEN BDS.BidStatusCode='K'
                                 AND BDS.BidderType='R' THEN BDS.BidAmount
                            ELSE 0
                          END) AS MAX_Rapid_Bid_Amount,
                      Max(CASE
                            WHEN BDS.BidStatusCode='K'
                                 AND BDS.BidderType='P' THEN BDS.BidAmount
                            ELSE 0
                          END) AS MAX_Proxy_Bid_Amount,
                      TBDS.Auction_Item_ID,
						SD.RingRep
      FROM   dbo.DimBidDetail AS BDS WITH (NOLOCK)
             --INNER JOIN dbo.DimStock AS STK WITH (NOLOCK)
             --  ON BDS.StockID=STK.StockID
             INNER JOIN #TMP_BID_DETAIL_SUMMARY AS TBDS WITH (NOLOCK)
               ON BDS.AuctionItemID=TBDS.Auction_Item_ID
             LEFT OUTER JOIN #TMP_Bid_Increment_Summary AS Bid_Increment WITH (NOLOCK)
               ON BDS.AuctionItemID=Bid_Increment.Auction_Item_ID
             LEFT OUTER JOIN #DimSoldDetail AS SD WITH (NOLOCK)
               ON BDS.AuctionItemID=SD.AuctionItemID
             LEFT OUTER JOIN #TMP_Winning_Bid_Information AS Winning_Bid_Info WITH (NOLOCK)
               ON BDS.AuctionItemID=Winning_Bid_Info.Auction_Item_ID
             LEFT OUTER JOIN #TMP_Proxy_Bid_Info AS Proxy_Bid_Info WITH (NOLOCK)
               ON BDS.AuctionItemID=Proxy_Bid_Info.Auction_Item_ID
             LEFT OUTER JOIN #DimAttendeeDetail AS AD WITH (NOLOCK) 
               ON BDS.BuyerEmployeeID=AD.BuyerEmployeeID
                  AND AD.LoginDatetime BETWEEN TBDS.Bid_Start_Datetime AND TBDS.Bid_End_Datetime
                  AND AD.LogoutDatetime BETWEEN TBDS.Bid_Start_Datetime AND TBDS.Bid_End_Datetime
      WHERE IsBidDetailShow = 1  AND BDS.BideventDatetime>=Dateadd(D, -3, @CurrentDatetime)            
      GROUP  BY SD.SaleAmount,
                SD.SaleEventDatetime,
                CONVERT(CHAR(8), SD.SaleEventDatetime, 112),
                SD.SaleStatusCode,
                Isnull(Bid_Increment.Average_Bid_increment, 0),
                BDS.AuctionItemID,
                TBDS.Bid_Duration,
                TBDS.Stock_Duration,
                TBDS.Total_Bidder_Count,
                SD.BuyerEmployeeID,
                SD.BuyerID,
                TBDS.Floor_Bidder_Count,
                TBDS.Internet_Bidder_Count,
                TBDS.Open_Bid_Amount,
                TBDS.Proxy_Bidder_Count,
                TBDS.Rapid_Bidder_Count,
                Winning_Bid_Info.Winning_Bidder_Display_Name,
                TBDS.Bid_Amounts,
                CASE
                  WHEN Bid_Increment.Bid_Increment_Change_Count>0 THEN 1
                  ELSE 0
                END,
                (Isnull(Bid_Increment.Bid_Increment_Change_Count, 0)),
                Isnull(Bid_Increment.Min_Bid_Increment_Amount, 0),
                Isnull(Bid_Increment.Max_Bid_Increment_Amount, 0),
                Isnull(Proxy_Bid_Info.Proxy_Bid_Ind, 0),
                Proxy_Bid_Info.Proxy_Buyer_Employee_ID,
                Proxy_Bid_Info.Proxy_Bid_Amount,
                TBDS.No_Bid_Ind,
                Bid_Increment.Runner_Up_Bid_Amount,
                Bid_Increment.Runner_Up_Bid_Type,
                TBDS.Auction_Item_ID,
						SD.RingRep

UPDATE FBS SET 
Accepted_Floor_Bid_Count=AcceptedBidsFloor,
Accepted_Internet_Bid_Count=AcceptedBidsInternet,
Accepted_Proxy_Bid_Count=AcceptedBidsProxy,
Accepted_Rapid_Bid_Count=AcceptedBidsRapid,
Bid_Accepted_Count=AcceptedBidsTotal,
Bid_Proposed_Count=BidsProposed,
Bid_Rejected_Count=BidsRejected,
Bid_Undo_Count=BidsUndo 
FROM #TMP_Bid_Summary FBS
INNER JOIN (
	SELECT AuctionItemID, 
	SUM(CASE WHEN IsBidDetailShow = 1 AND BidStatusCode = 'K' and BidType = 'F' then 1 ELSE 0 END) as AcceptedBidsFloor,
	SUM(CASE WHEN IsBidDetailShow = 1 AND BidStatusCode = 'K' and BidType = 'I' then 1 ELSE 0 END) as AcceptedBidsInternet,
	SUM(CASE WHEN IsBidDetailShow = 1 AND BidStatusCode = 'K' and BidType = 'P' then 1 ELSE 0 END) as AcceptedBidsProxy,
	SUM(CASE WHEN IsBidDetailShow = 1 AND BidStatusCode = 'K' and BidType = 'R' then 1 ELSE 0 END) as AcceptedBidsRapid,
	SUM(CASE WHEN IsBidDetailShow = 1 AND BidStatusCode = 'K' then 1 ELSE 0 END) as AcceptedBidsTotal,
	SUM(CASE WHEN IsBidDetailShow = 1 AND BidStatusCode = 'P' then 1 ELSE 0 END) as BidsProposed,
	SUM(CASE WHEN IsBidDetailShow = 1 AND BidStatusCode = 'R' then 1 ELSE 0 END) as BidsRejected,
	SUM(CASE WHEN IsBidDetailShow = 1 AND BidStatusCode = 'U' then 1 ELSE 0 END) as BidsUndo
	FROM EDW.DBO.DimBidDetail  WITH (NOLOCK)
	GROUP BY AuctionItemID) Counts
ON FBS.Auction_Item_ID = Counts.AuctionItemID	

      CREATE TABLE #temp_RecordCount
        (ChangeType VARCHAR(10))

      BEGIN TRAN

      INSERT INTO dbo.FactBidSummary
                  (AuctionSaleAmount,AuctionSaleStatus,AverageBidIncrement,BidAcceptedCount,BidDuration,BidEndDatetime,BidReceivedCount,
                   BidProposedCount
                   ,BidRejectedCount
                   ,BidStartDatetime,BidderCount,BuyerEmployeeID,BuyerID,FloorBidderCount,InternetBidderCount,OpenBidAmount,ProxyBidderCount,
                   RapidBidderCount,SoldBidCount,--StockID,
                   ViewerCount,DisplayName,BidUndoCount,AcceptedFloorBidCount,AcceptedInternetBidCount,AcceptedProxyBidCount,AcceptedRapidBidCount,
                   BidAmounts,BidIncrementChangeInd,BidIncrementChangeCount,MinBidIncrement,MaxBidIncrement,BidRestartInd,ProxyBidInd,ProxyBidderId,
                   ProxyBidAmount,NoBidInd,RunnerUpBidAmount,RunnerUpBidType,MaxInternetBidAmount,MaxFloorBidAmount,MaxRapidBidAmount,
                   MaxProxyBidAmount,
                   FirstBidDatetime,LastBidDatetime,StockDuration,AwardedBidCount,AuctionItemID,ETLLoadID,ETLLoadUpdateID,RingRep)
      SELECT TBS.Auction_Sale_Amount,
             TBS.Auction_Sale_Status,
             TBS.Average_Bid_Increment,
             TBS.Bid_Accepted_Count,
             TBS.Bid_Duration,
             TBS.Bid_End_Datetime,
             TBS.Bid_Received_Count,
             TBS.Bid_Proposed_Count,
             TBS.Bid_Rejected_Count,
             TBS.Bid_Start_Datetime,
             TBS.Bidder_Count,
             TBS.Buyer_Employee_ID,
             TBS.Buyer_ID,
             TBS.Floor_Bidder_Count,
             TBS.Internet_Bidder_Count,
             TBS.Open_Bid_Amount,
             TBS.Proxy_Bidder_Count,
             TBS.Rapid_Bidder_Count,
             TBS.Sold_Bid_Count,
             TBS.Viewer_Count,
             TBS.Display_Name,
             TBS.Bid_Undo_Count,
             TBS.Accepted_Floor_Bid_Count,
             TBS.Accepted_Internet_Bid_Count,
             TBS.Accepted_Proxy_Bid_Count,
             TBS.Accepted_Rapid_Bid_Count,
             TBS.Bid_Amounts,
             TBS.Bid_Increment_Change_Ind,
             TBS.Bid_Increment_Change_Count,
             TBS.Min_Bid_Increment,
             TBS.Max_Bid_Increment,
             TBS.Bid_Restart_Ind,
             TBS.Proxy_Bid_Ind,
             TBS.Proxy_Buyer_Employee_ID,
             TBS.Proxy_Bid_Amount,
             TBS.No_Bid_Ind,
             TBS.Runner_Up_Bid_Amount,
             TBS.Runner_Up_Bid_Type,
             TBS.Max_Internet_Bid_Amount,
             TBS.Max_Floor_Bid_Amount,
             TBS.Max_Rapid_Bid_Amount,
             TBS.Max_Proxy_Bid_Amount,
             TBS.First_Bid_Datetime,
             TBS.Last_Bid_Datetime,
             TBS.Stock_Duration,
             TBS.Awarded_Bid_Count,
             TBS.Auction_Item_ID,
             @ETLLoadID_sp,
             @ETLLoadID_sp,
				TBS.RingRep
      FROM   #TMP_Bid_Summary AS TBS WITH (NOLOCK)
             LEFT OUTER JOIN dbo.FactBidSummary AS Bid_Summary
               ON TBS.Auction_Item_ID=Bid_Summary.AuctionItemID
      WHERE  Bid_Summary.AuctionItemID IS NULL

      SELECT @InsertRowCount_sp=Count(*)
      FROM   #temp_RecordCount WITH (NOLOCK)
      WHERE  ChangeType='INSERT'

UPDATE FBS SET 
AcceptedFloorBidCount=AcceptedBidsFloor,
AcceptedInternetBidCount=AcceptedBidsInternet,
AcceptedProxyBidCount=AcceptedBidsProxy,
AcceptedRapidBidCount=AcceptedBidsRapid,
BidAcceptedCount=AcceptedBidsTotal,
BidProposedCount=BidsProposed,
BidRejectedCount=BidsRejected,
BidUndoCount=BidsUndo 
FROM dbo.FactBidSummary FBS
INNER JOIN (
	SELECT AuctionItemID, 
	SUM(CASE WHEN IsBidDetailShow = 1 AND BidStatusCode = 'K' and BidType = 'F' then 1 ELSE 0 END) as AcceptedBidsFloor,
	SUM(CASE WHEN IsBidDetailShow = 1 AND BidStatusCode = 'K' and BidType = 'I' then 1 ELSE 0 END) as AcceptedBidsInternet,
	SUM(CASE WHEN IsBidDetailShow = 1 AND BidStatusCode = 'K' and BidType = 'P' then 1 ELSE 0 END) as AcceptedBidsProxy,
	SUM(CASE WHEN IsBidDetailShow = 1 AND BidStatusCode = 'K' and BidType = 'R' then 1 ELSE 0 END) as AcceptedBidsRapid,
	SUM(CASE WHEN IsBidDetailShow = 1 AND BidStatusCode = 'K' then 1 ELSE 0 END) as AcceptedBidsTotal,
	SUM(CASE WHEN IsBidDetailShow = 1 AND BidStatusCode = 'P' then 1 ELSE 0 END) as BidsProposed,
	SUM(CASE WHEN IsBidDetailShow = 1 AND BidStatusCode = 'R' then 1 ELSE 0 END) as BidsRejected,
	SUM(CASE WHEN IsBidDetailShow = 1 AND BidStatusCode = 'U' then 1 ELSE 0 END) as BidsUndo
	FROM EDW.DBO.DimBidDetail  WITH (NOLOCK)
	GROUP BY AuctionItemID) Counts
ON FBS.AuctionItemID = Counts.AuctionItemID	
INNER JOIN DimAuctionItem DAI WITH (NOLOCK)
	ON FBS.AuctionItemID = DAI.AuctionItemID
WHERE CONVERT(DATE,AuctionDateTime)>=CONVERT(DATE,Dateadd(D, -3, @CurrentDatetime))


      /*******************************************************
      RunnerUpBidderId Logic
      *******************************************************/
	
	 CREATE TABLE #TMP_RunnerUp_Bid_Info
        (AuctionItemID         INT,
         RunnerUpBidInd INT,
		  RunnerUpBuyerId INT,
         RunnerUpBidderID Varchar(50))
    
        
    INSERT INTO #TMP_RunnerUp_Bid_Info
                  (AuctionItemID,RunnerUpBidInd,RunnerUpBuyerId,RunnerUpBidderID)
	  SELECT BDF.AuctionItemID,
	         BDF.RunnerUpBidInd,
			  BDF.BuyerID AS RunnerUpBuyerId,
			 Concat(BDF.BuyerID,'_', BDF.BuyerEmployeeID) AS RunnerUpBidderID
	         --BDF.BuyerEmployeeID AS RunnerUpBidderID
	  FROM   dbo.DimBidDetail BDF WITH (NOLOCK)
      WHERE RunnerUpBidInd = 1 AND BDF.BidderType <> 'F' 
	  AND IsBidDetailShow = 1 AND BDF.BideventDatetime>=Dateadd(D, -3, @CurrentDatetime)            
	

	UPDATE FBS
	SET FBS.RunnerUpBidderID = ISNULL(RBI.RunnerUpBidderID,0)
	FROM #TMP_RunnerUp_Bid_Info RBI WITH(NoLock)
	LEFT OUTER JOIN [dbo].[FactBidSummary] FBS WITH(NoLock)
	ON FBS.AuctionItemId = RBI.AuctionItemId
	 WHERE  FBS.AuctionItemID IS NOT NULL

	       UPDATE FBS
	 SET FBS.RunnerUpBidderType = ISNULL(DB.BuyerTypeDescription,'')
	 FROM #TMP_RunnerUp_Bid_Info AS RBI WITH(NoLock) 
	 LEFT OUTER JOIN EDW.dbo.FactBidSummary FBS WITH (NOLOCK)
	     ON FBS.AuctionItemID = RBI.AuctionItemID
     LEFT OUTER JOIN EDW.dbo.DimBuyer DB WITH (NOLOCK) 
	    ON RBI.RunnerUpBuyerID = DB.BuyerID

	   /*******************************************************
      Winning BidderId Logic
      *******************************************************/
	
	 CREATE TABLE #TMP_Winning_Bid_Info
        (AuctionItemID         INT,
         WinningBidderID Varchar(50))
    
        
    INSERT INTO #TMP_Winning_Bid_Info
                  (AuctionItemID,WinningBidderID)
	 		 SELECT BDF.AuctionItemID,
	         (Case when BidderType <> 'F' then  Concat(BDF.BuyerID,'_', BDF.BuyerEmployeeID) else '0' end) AS WinningBidderID
	  FROM   edw.dbo.DimBidDetail BDF WITH (NOLOCK)
	  Join (Select AuctionItemID,
	        Max(BidEventDatetime) AS MAXBidEventDatetime from edw.dbo.DimBidDetail
			where  BidStatusCode='K' 
	  AND IsBidDetailShow = 1   AND BideventDatetime>=Dateadd(D, -3, @CurrentDatetime)    
			Group by AuctionItemID)a
			On a.AuctionItemID=BDF.AuctionItemID
			and a.MAXBidEventDatetime=BDF.BidEventDatetime
			
      WHERE BidStatusCode='K' 
	  AND IsBidDetailShow = 1 
	  AND BDF.BideventDatetime>=Dateadd(D, -3, @CurrentDatetime)            

	UPDATE FBS
	SET FBS.WinningBidderID = ISNULL(WBI.WinningBidderID,0)
	FROM #TMP_Winning_Bid_Info WBI WITH(NoLock)
	LEFT OUTER JOIN [dbo].[FactBidSummary] FBS WITH(NoLock)
	ON FBS.AuctionItemId = WBI.AuctionItemId
	 WHERE  FBS.AuctionItemID IS NOT NULL
	 
  COMMIT TRAN

      EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID_sp,@ExtractRowCount = @ExtractRowCount_sp,@InsertRowCount =
      @InsertRowCount_sp,@UpdateRowCount = @UpdateRowCount_sp

      IF Object_id('tempdb..#TMP_BID_DETAIL_SUMMARY') IS NOT NULL
        DROP TABLE #TMP_BID_DETAIL_SUMMARY

      IF Object_id('tempdb..#TMP_Bid_Detail_Info') IS NOT NULL
        DROP TABLE #TMP_Bid_Detail_Info

      IF Object_id('tempdb..#TMP_Bid_Increment_Summary') IS NOT NULL
        DROP TABLE #TMP_Bid_Increment_Summary

      IF Object_id('tempdb..#TMP_Bid_Start_End_Datetime') IS NOT NULL
        DROP TABLE #TMP_Bid_Start_End_Datetime

      IF Object_id('tempdb..#TMP_Bid_Start_End_Datetime_Source') IS NOT NULL
        DROP TABLE #TMP_Bid_Start_End_Datetime_Source

      IF Object_id('tempdb..#TMP_Bid_Summary') IS NOT NULL
        DROP TABLE #TMP_Bid_Summary

      IF Object_id('tempdb..#TMP_Bidder_Bid_Count') IS NOT NULL
        DROP TABLE #TMP_Bidder_Bid_Count

      IF Object_id('tempdb..#TMP_Bidder_Count') IS NOT NULL
        DROP TABLE #TMP_Bidder_Count

      IF Object_id('tempdb..#TMP_Build_Bid_Amount') IS NOT NULL
        DROP TABLE #TMP_Build_Bid_Amount

      IF Object_id('tempdb..#TMP_First_Last_Bid_Datetime') IS NOT NULL
        DROP TABLE #TMP_First_Last_Bid_Datetime

      IF Object_id('tempdb..#TMP_No_Bid_Ind') IS NOT NULL
        DROP TABLE #TMP_No_Bid_Ind

      IF Object_id('tempdb..#TMP_Open_Bid') IS NOT NULL
        DROP TABLE #TMP_Open_Bid

      IF Object_id('tempdb..#TMP_Proxy_Bid_Info') IS NOT NULL
        DROP TABLE #TMP_Proxy_Bid_Info

      IF Object_id('tempdb..#TMP_Stock_Bid_Amount') IS NOT NULL
        DROP TABLE #TMP_Stock_Bid_Amount

      IF Object_id('tempdb..#TMP_Winning_Bid_Information') IS NOT NULL
        DROP TABLE #TMP_Winning_Bid_Information

      IF Object_id('tempdb..#temp_RecordCount') IS NOT NULL
        DROP TABLE #temp_RecordCount

      IF Object_id('tempdb..#DimBidIncrementDetail') IS NOT NULL
        DROP TABLE #DimBidIncrementDetail

      IF Object_id('tempdb..#TMPFinalBidOffsetResult') IS NOT NULL
        DROP TABLE #TMPFinalBidOffsetResult

      IF Object_id('tempdb..#TMPFinalBidOffset') IS NOT NULL
        DROP TABLE #TMPFinalBidOffset

      IF Object_id('tempdb..#TMPOverrideUndoBidStatus') IS NOT NULL
        DROP TABLE #TMPOverrideUndoBidStatus

      IF Object_id('tempdb..#TMPReceivedBidStatus') IS NOT NULL
        DROP TABLE #TMPReceivedBidStatus

      IF Object_id('tempdb..#TMPBidIncrementFilter') IS NOT NULL
        DROP TABLE #TMPBidIncrementFilter

      IF Object_id('tempdb..#FactBidIncrementSummary') IS NOT NULL
        DROP TABLE #FactBidIncrementSummary

      IF Object_id('tempdb..#TMP_Runner_Up_Bid_Datetime') IS NOT NULL
        DROP TABLE #TMP_Runner_Up_Bid_Datetime

      IF Object_id('tempdb..#TMP_Runner_Up_Bid_Information_Source') IS NOT NULL
        DROP TABLE #TMP_Runner_Up_Bid_Information_Source

      IF Object_id('tempdb..#TMP_Runner_Up_Bid_Results') IS NOT NULL
        DROP TABLE #TMP_Runner_Up_Bid_Results

      IF Object_id('tempdb..#TMP_Winning_Bid_Detail') IS NOT NULL
        DROP TABLE #TMP_Winning_Bid_Detail
        
      IF Object_id('tempdb..#DimAttendeeDetail') IS NOT NULL
        DROP TABLE #DimAttendeeDetail
      IF Object_id('tempdb..#DimSoldDetail') IS NOT NULL
        DROP TABLE #DimSoldDetail

          IF Object_id('tempdb..#TMP_RunnerUp_Bid_Info') IS NOT NULL
        DROP TABLE #TMP_RunnerUp_Bid_Info
        
		   IF Object_id('tempdb..#TMP_Winning_Bid_Info') IS NOT NULL
        DROP TABLE #TMP_Winning_Bid_Info
        
      RETURN 0
  END TRY

  BEGIN CATCH
      SET NOCOUNT OFF

      IF @@TRANCOUNT>0
        ROLLBACK TRAN

      EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID_sp,@ExtractRowCount = @ExtractRowCount_sp,@InsertRowCount =
      @InsertRowCount_sp,@UpdateRowCount = @UpdateRowCount_sp,@ErrorCode = @@ERROR

      DECLARE @ErrorMessage  NVARCHAR(4000),
              @ErrorSeverity INT,
              @ErrorState    INT;

      SELECT @ErrorMessage=Error_message(),
             @ErrorSeverity=Error_severity(),
             @ErrorState=Error_state();

      RAISERROR (@ErrorMessage,
                 @ErrorSeverity,
                 @ErrorState);

      IF Object_id('tempdb..#TMP_BID_DETAIL_SUMMARY') IS NOT NULL
        DROP TABLE #TMP_BID_DETAIL_SUMMARY

      IF Object_id('tempdb..#TMP_Bid_Detail_Info') IS NOT NULL
        DROP TABLE #TMP_Bid_Detail_Info

      IF Object_id('tempdb..#TMP_Bid_Increment_Summary') IS NOT NULL
        DROP TABLE #TMP_Bid_Increment_Summary

      IF Object_id('tempdb..#TMP_Bid_Start_End_Datetime') IS NOT NULL
        DROP TABLE #TMP_Bid_Start_End_Datetime

      IF Object_id('tempdb..#TMP_Bid_Start_End_Datetime_Source') IS NOT NULL
        DROP TABLE #TMP_Bid_Start_End_Datetime_Source

      IF Object_id('tempdb..#TMP_Bid_Summary') IS NOT NULL
        DROP TABLE #TMP_Bid_Summary

      IF Object_id('tempdb..#TMP_Bidder_Bid_Count') IS NOT NULL
        DROP TABLE #TMP_Bidder_Bid_Count

      IF Object_id('tempdb..#TMP_Bidder_Count') IS NOT NULL
        DROP TABLE #TMP_Bidder_Count

      IF Object_id('tempdb..#TMP_Build_Bid_Amount') IS NOT NULL
        DROP TABLE #TMP_Build_Bid_Amount

      IF Object_id('tempdb..#TMP_First_Last_Bid_Datetime') IS NOT NULL
        DROP TABLE #TMP_First_Last_Bid_Datetime

      IF Object_id('tempdb..#TMP_No_Bid_Ind') IS NOT NULL
        DROP TABLE #TMP_No_Bid_Ind

      IF Object_id('tempdb..#TMP_Open_Bid') IS NOT NULL
        DROP TABLE #TMP_Open_Bid

      IF Object_id('tempdb..#TMP_Proxy_Bid_Info') IS NOT NULL
        DROP TABLE #TMP_Proxy_Bid_Info

      IF Object_id('tempdb..#TMP_Stock_Bid_Amount') IS NOT NULL
        DROP TABLE #TMP_Stock_Bid_Amount

      IF Object_id('tempdb..#TMP_Winning_Bid_Information') IS NOT NULL
        DROP TABLE #TMP_Winning_Bid_Information

      IF Object_id('tempdb..#temp_RecordCount') IS NOT NULL
        DROP TABLE #temp_RecordCount

      IF Object_id('tempdb..#DimBidIncrementDetail') IS NOT NULL
        DROP TABLE #DimBidIncrementDetail

      IF Object_id('tempdb..#TMPFinalBidOffsetResult') IS NOT NULL
        DROP TABLE #TMPFinalBidOffsetResult

      IF Object_id('tempdb..#TMPFinalBidOffset') IS NOT NULL
        DROP TABLE #TMPFinalBidOffset

      IF Object_id('tempdb..#TMPOverrideUndoBidStatus') IS NOT NULL
        DROP TABLE #TMPOverrideUndoBidStatus

      IF Object_id('tempdb..#TMPReceivedBidStatus') IS NOT NULL
        DROP TABLE #TMPReceivedBidStatus

      IF Object_id('tempdb..#TMPBidIncrementFilter') IS NOT NULL
        DROP TABLE #TMPBidIncrementFilter

      IF Object_id('tempdb..#FactBidIncrementSummary') IS NOT NULL
        DROP TABLE #FactBidIncrementSummary

      IF Object_id('tempdb..#TMP_Runner_Up_Bid_Datetime') IS NOT NULL
        DROP TABLE #TMP_Runner_Up_Bid_Datetime

      IF Object_id('tempdb..#TMP_Runner_Up_Bid_Information_Source') IS NOT NULL
        DROP TABLE #TMP_Runner_Up_Bid_Information_Source

      IF Object_id('tempdb..#TMP_Runner_Up_Bid_Results') IS NOT NULL
        DROP TABLE #TMP_Runner_Up_Bid_Results

      IF Object_id('tempdb..#TMP_Winning_Bid_Detail') IS NOT NULL
        DROP TABLE #TMP_Winning_Bid_Detail

      IF Object_id('tempdb..#DimAttendeeDetail') IS NOT NULL
        DROP TABLE #DimAttendeeDetail
      IF Object_id('tempdb..#DimSoldDetail') IS NOT NULL
        DROP TABLE #DimSoldDetail

          IF Object_id('tempdb..#TMP_RunnerUp_Bid_Info') IS NOT NULL
        DROP TABLE #TMP_RunnerUp_Bid_Info

		    IF Object_id('tempdb..#TMP_Winning_Bid_Info') IS NOT NULL
        DROP TABLE #TMP_Winning_Bid_Info
		
        
         RETURN -1
  END CATCH


GO


