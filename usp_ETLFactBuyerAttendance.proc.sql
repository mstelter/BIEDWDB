/* 
USE EDW
GO 
================================================================================================
Author		: Preetham Duvva
Create date	: 6/29/2012
Description	: Populate the EDW FactBuyerAttendance table from CDC	
================================================================================================
REVISION History
ChangeDate	   Developer	
7/25/2012      Preetham Duvva       Added a logic to match BuyerAttendance with OutofCountryBuyerAttendance 
									for Out Of Country Buyers
7/30/2012       Preetham Duvva      Updated the logic for Out Of Country Buyers when IBIDAttendees = Onsite Attendees
8/10/2012       Preetham Duvva      Added AuctionDayID and AuctionDateTime to the table and removed IsSuccesful.
3/4/2013		Matt Stelter		Added COALESCE for Buyer ID in case ASAP does not have the ID
3/13/2013		Juan X. Jacome		Added changes from DBA Code Review
11/22/2016    Yogitha Alwarsetty    Added ProxyAttendeesNotCheckedIn
================================================================================================
Usage Example:
Exec usp_ETLFactBuyerAttendance
================================================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLFactBuyerAttendance]
AS     
BEGIN

DECLARE @ExtractRowCount int
		,@ETLLoadID int
		,@InsertRowCount int
		,@UpdateRowCount int
		,@MinLSN Binary(10)
		,@MaxLSN Binary (10)
		,@MinAllowableDate		Datetime
		,@MaxAllowableDate		Datetime
		
		
				
SET NOCOUNT ON;

BEGIN TRY
         
--Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLFactBuyerAttendance'

SELECT @MinAllowableDate = MIN(DayDate), @MaxAllowableDate = MAX(DayDate) FROM DimDay WITH (NOLOCK) WHERE DayID > 19000101 and DayID < 29991231


--Get the start and end LSNs
SELECT   @MinLSN = MinLSN, @MaxLSN = MaxLSN FROM dbo.ETLRun WITH (NOLOCK) WHERE ETLJobName = 'DailyETLJob'


IF OBJECT_ID('tempdb..#TMP_FactBuyerAttendance') IS NOT NULL DROP TABLE #TMP_FactBuyerAttendance
CREATE TABLE #TMP_FactBuyerAttendance
			(	BuyerEmployeeID INT NOT NULL,
				BuyerID INT NOT NULL,
				AuctionID INT NOT NULL,
				AuctionDayID INT NULL,
				AuctionDateTime Datetime NULL,
				BranchNumber INT NULL,
				IsIBID TINYINT NULL,
				IsFloor TINYINT NULL,
				OnsiteAttendees INT NULL,
				IBIDAttendees INT NULL,
				BuyerAttendance INT NULL,
				GuestAttendance INT NULL,
				OutOfCountryBuyerAttendance INT NULL,
				ProxyAttendeesNotCheckedIn INT NULL
			)
			

IF OBJECT_ID('tempdb..#T_Buyer_Not_INAttendance') IS NOT NULL DROP TABLE #T_Buyer_Not_INAttendance
CREATE TABLE #T_Buyer_Not_INAttendance
			(	AuctionID INT NOT NULL,
				BuyerID   INT NOT NULL
			)


IF OBJECT_ID('tempdb..#TMP_FactBuyerAttendanceRecordCount') IS NOT NULL DROP TABLE #TMP_FactBuyerAttendanceRecordCount
CREATE TABLE #TMP_FactBuyerAttendanceRecordCount(ChangeType VARCHAR(10)) 

INSERT INTO #TMP_FactBuyerAttendance
(	BuyerEmployeeID,
	BuyerID,
	AuctionID,
	AuctionDayID,
	AuctionDateTime,
	BranchNumber,
	IsIBID,
	IsFloor,
	OnsiteAttendees,
	IBIDAttendees,
	BuyerAttendance,
	GuestAttendance,
	OutOfCountryBuyerAttendance		   
)
SELECT DISTINCT      
       BA.Buyer_Employee_ID AS BuyerEmployeeID
       ,B.Buyer_ID AS BuyerID
       ,ASD.Auction_Schedule_ID AS AuctionID
       ,CASE WHEN Isnull(Auction_DateTime, '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8),ASD.Auction_DateTime, 112)), 29991231) ELSE 29991231 END AS AuctionDayID
       ,CASE WHEN Isnull(Auction_DateTime, '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(ASD.Auction_DateTime, '2999-12-31') ELSE '2999-12-31' END AS AuctionDateTime
       ,ASD.Branch_Number As BranchNumber
       ,CASE WHEN BA.Update_User_ID = 1  THEN 1 ELSE 0 END AS IsIBID
       ,CASE WHEN BA.Update_User_ID <> 1 THEN 1 ELSE 0 END AS IsFloor
       ,0 AS OnsiteAttendees
       ,0 AS IBIDAttendees
       ,0 AS BuyerAttendance
       ,ISNULL(BA.Visitor_Count,0) AS GuestAttendance
       ,CASE WHEN (B.Country_Abbreviation <> 'US' or (B.Country_Abbreviation='US' and B.State_Abbreviation IN ('GU','PR','VI'))) THEN 1 ELSE 0 END AS OutOfCountryBuyerAttendance
FROM   [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Buyer_Attendance](@MinLSN,@MaxLSN, 'all with merge') BA 
INNER JOIN BI_ASAP_Rep.dbo.Auction_Schedule ASD WITH (NOLOCK) 
ON BA.Auction_Schedule_ID = ASD.Auction_Schedule_ID
INNER JOIN BI_ASAP_Rep.dbo.Buyer B 
ON BA.Buyer_ID = B.Buyer_ID
WHERE [__$operation] = 5


--Insert for the new buyers which are not in buyer attendance but participate in the auction
INSERT INTO #T_Buyer_Not_INAttendance
(	AuctionID ,
    BuyerID   
)

SELECT DISTINCT SS.Auction_Schedule_ID,SS.Buyer_ID
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Auction_Schedule] (@MinLSN, @MaxLSN, 'all with merge')  ASD
INNER JOIN BI_ASAP_Rep.dbo.Salvage_Sale SS WITH (NOLOCK) 
ON ASD.Auction_Schedule_ID = SS.Auction_Schedule_ID
WHERE SS.Buyer_ID NOT IN 
				(SELECT BA.Buyer_ID FROM BI_ASAP_Rep.dbo.Buyer_Attendance BA WITH (NOLOCK) WHERE BA.Auction_Schedule_ID = ASD.Auction_Schedule_ID)
AND [__$operation] = 5


INSERT INTO #TMP_FactBuyerAttendance
(	BuyerEmployeeID,
	BuyerID,
	AuctionID,
	AuctionDayID,
	AuctionDateTime,
	BranchNumber,
	IsIBID,
	IsFloor,
	OnsiteAttendees,
	IBIDAttendees,
	BuyerAttendance,
	GuestAttendance,
	OutOfCountryBuyerAttendance		   
)
SELECT COALESCE((SELECT MAX(BE.Buyer_Employee_ID) FROM BI_ASAP_Rep.dbo.buyer_employee BE WITH (NOLOCK) 
					WHERE BE.Buyer_ID = TNA.BuyerID ),(SELECT MAX(BE.BuyerEmployeeID) FROM EDW.dbo.Dimbuyeremployee BE WITH (NOLOCK)
					WHERE BE.BuyerID = TNA.BuyerID ),0) AS BuyerEmployeeID
		,TNA.BuyerID AS BuyerID
		,TNA.AuctionID AS AuctionID
		,CASE WHEN Isnull(Auction_DateTime, '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8),ASD.Auction_DateTime, 112)), 29991231) ELSE 29991231 END AS AuctionDayID
        ,CASE WHEN Isnull(Auction_DateTime, '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(ASD.Auction_DateTime, '2999-12-31') ELSE '2999-12-31' END AS AuctionDateTime
        ,ASD.Branch_Number AS BranchNumber
		,0 AS IsIBID
		,1 AS IsFloor
		,0 AS OnsiteAttendees
		,0 AS IBIDAttendees
		,0 AS BuyerAttendance
		,0 AS GuestAttendance
		,CASE WHEN (B.Country_Abbreviation <> 'US' or (B.Country_Abbreviation='US' and B.State_Abbreviation IN ('GU','PR','VI'))) THEN 1 ELSE 0 END AS OutOfCountryBuyerAttendance
FROM #T_Buyer_Not_INAttendance TNA
INNER JOIN BI_ASAP_Rep.dbo.Auction_Schedule ASD WITH (NOLOCK) 
ON TNA.AuctionID = ASD.Auction_Schedule_ID
INNER JOIN BI_ASAP_Rep.dbo.Buyer B WITH (NOLOCK) 
ON TNA.BuyerID = B.Buyer_ID

	
--We are going to count Proxy win as IBID.
UPDATE FBA
SET IsIBID = 1
FROM #TMP_FactBuyerAttendance AS FBA WITH (NOLOCK)
INNER JOIN BI_Auction_Rep.dbo.IBR_Results AS IB 
ON FBA.BuyerID = IB.Buyer_Id 
 AND FBA.BranchNumber = IB.Branch_Code 
 AND FBA.AuctionDateTime = IB.Auction_Date
 AND FBA.BuyerEmployeeID = IB.Buyer_Employee_ID
WHERE Won='Yes' 
AND IsIBID = 0

---Update for OnsiteAttendees, IBIDAttendees, BuyerAttendance
UPDATE FBA
SET OnsiteAttendees = CASE WHEN FBA.IsFloor = 1 THEN 1 ELSE  0 END,
    IBIDAttendees = CASE WHEN FBA.IsIBID=1 THEN 1 ELSE 0 END ,
    BuyerAttendance = (CASE WHEN FBA.IsFloor = 1 THEN 1 ELSE  0 END) + (CASE WHEN FBA.IsIBID=1 THEN 1 ELSE 0 END)
FROM #TMP_FactBuyerAttendance FBA


UPDATE FBA
SET OutOfCountryBuyerAttendance = BuyerAttendance
FROM #TMP_FactBuyerAttendance FBA WITH(NOLOCK)
WHERE OnsiteAttendees = IBIDAttendees 
AND OutOfCountryBuyerAttendance <> 0
AND BuyerAttendance <> OutOfCountryBuyerAttendance
									  
--Get Buyer Employees by auction number and auction date
select DISTINCT DBD.BuyerEmployeeID, DAI.AuctionDateTime, DAI.AuctionID 
Into #tempppppproxy
FROM [EDW].[dbo].[DimBidDetail] DBD with(Nolock)
INNER JOIN EDW.dbo.DimAuctionItem DAI WITH (NOLOCK)
       ON DBD.AuctionItemID = DAI.AuctionItemID
where BidderType='P'
and  Convert(Varchar(10),[BidEventDatetime] ,126)>= ( SELECT 
      Convert(Varchar(10),[MaxDatetime]-2,126)
     
  FROM [EDW].[dbo].[ETLRun]
  where
  ETLJobName = 'DailyETLJob')

DELETE FROM TP
--SELECT COUNT(*) 
FROM #tempppppproxy TP WITH (NOLOCK)
inner join [EDW].[dbo].[FactBuyerAttendance] FBA with(Nolock)
       on TP.AuctionDateTime = FBA.AuctionDateTime AND TP.AuctionID = FBA.AuctionID AND TP.BuyerEmployeeID = FBA.BuyerEmployeeID

INSERT INTO #TMP_FactBuyerAttendance
           ([BuyerEmployeeID]
           ,[BuyerID]
           ,[AuctionID]
           ,[BranchNumber]
           ,[IsIBID]
           ,[IsFloor]
           ,[OnsiteAttendees]
           ,[IBIDAttendees]
           ,[BuyerAttendance]
           ,[GuestAttendance]
           ,[OutOfCountryBuyerAttendance]
         
           ,[AuctionDayID]
           ,[AuctionDateTime]
           ,[ProxyAttendeesNotCheckedIn])
SELECT TP.[BuyerEmployeeID],
       dbe.BuyerID,
       TP.AuctionID,
       DA.BranchNumber,
       0,0,0,0,0,0,0,
       DA.AuctionDayID,
       DA.AuctionDateTime,
       1
FROM #tempppppproxy TP WITH (NOLOCK)
inner join EDW.dbo.DimBuyerEmployee dbe with (NOLOCK)
       ON TP.BuyerEmployeeID = DBE.BuyerEmployeeID
inner join EDW.dbo.DimAuction DA WITH (NOLOCK)
       ON TP.AuctionDateTime = DA.AuctionDateTime AND TP.AuctionID = DA.AuctionID


SELECT @ExtractRowCount=COUNT(*) FROM #TMP_FactBuyerAttendance


/*********************************************************************************************
MERGE the data from the temp table into the EDW FactBuyerAttendance table
This does Update and Insert only.  No Deletes
*********************************************************************************************/

BEGIN TRANSACTION 
--Synchronize source data with target
	MERGE dbo.FactBuyerAttendance FBA
	USING #TMP_FactBuyerAttendance  TFBA  
	ON  FBA.BuyerID = TFBA.BuyerID 
	AND FBA.BuyerEmployeeID = TFBA.BuyerEmployeeID 
	AND FBA.AuctionID = TFBA.AuctionID
		
	WHEN MATCHED THEN
	UPDATE SET
	FBA.BuyerEmployeeID = TFBA.BuyerEmployeeID
	,FBA.BuyerID = TFBA.BuyerID
    ,FBA.AuctionID = TFBA.AuctionID
    ,FBA.BranchNumber = TFBA.BranchNumber
    ,FBA.IsIBID = TFBA.IsIBID
    ,FBA.IsFloor = TFBA.IsFloor
    ,FBA.OnsiteAttendees = ISNULL(TFBA.OnsiteAttendees,0)
    ,FBA.IBIDAttendees = ISNULL(TFBA.IBIDAttendees,0)
    ,FBA.BuyerAttendance = ISNULL(TFBA.BuyerAttendance,0)
    ,FBA.GuestAttendance = ISNULL(TFBA.GuestAttendance,0)
    ,FBA.OutOfCountryBuyerAttendance = TFBA.OutOfCountryBuyerAttendance
    ,FBA.AuctionDayID = TFBA.AuctionDayID
    ,FBA.AuctionDateTime = TFBA.AuctionDateTime
	,FBA.[ProxyAttendeesNotCheckedIn]=TFBA.[ProxyAttendeesNotCheckedIn]
    ,ETLLoadUpdateID = @ETLLoadID
    
	WHEN NOT MATCHED  THEN 
	INSERT (
	BuyerEmployeeID
	,BuyerID
	,AuctionID
	,BranchNumber
	,IsIBID
	,IsFloor
	,OnsiteAttendees
	,IBIDAttendees
	,BuyerAttendance
	,GuestAttendance
	,OutOfCountryBuyerAttendance
	,AuctionDayID
	,AuctionDateTime
	,[ProxyAttendeesNotCheckedIn]
	,ETLLoadID
	,ETLLoadUpdateID
	)
	
	Values(   
    TFBA.BuyerEmployeeID
    ,TFBA.BuyerID
    ,TFBA.AuctionID
    ,TFBA.BranchNumber
    ,TFBA.IsIBID
    ,TFBA.IsFloor
    ,ISNULL(TFBA.OnsiteAttendees,0)
    ,ISNULL(TFBA.IBIDAttendees,0)
    ,ISNULL(TFBA.BuyerAttendance,0)
    ,ISNULL(TFBA.GuestAttendance,0)
    ,ISNULL(TFBA.OutOfCountryBuyerAttendance,0)
    ,TFBA.AuctionDayID
    ,TFBA.AuctionDateTime
	,ISNULL(TFBA.[ProxyAttendeesNotCheckedIn],0)
    ,@ETLLoadID
    ,@ETLLoadID
	)
OUTPUT $action INTO #TMP_FactBuyerAttendanceRecordCount;

SELECT @InsertRowCount = COUNT(*)  FROM #TMP_FactBuyerAttendanceRecordCount WHERE ChangeType = 'INSERT'
SELECT @UpdateRowCount = COUNT(*)  FROM #TMP_FactBuyerAttendanceRecordCount WHERE ChangeType = 'UPDATE'


	EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID,
		@ExtractRowCount = @ExtractRowCount,
		@InsertRowCount = @InsertRowCount,
		@UpdateRowCount = @UpdateRowCount

COMMIT TRAN
	
	IF OBJECT_ID('tempdb..#TMP_FactBuyerAttendance') IS NOT NULL DROP TABLE #TMP_FactBuyerAttendance
	IF OBJECT_ID('tempdb..#TMP_FactBuyerAttendanceRecordCount') IS NOT NULL DROP TABLE #TMP_FactBuyerAttendanceRecordCount
    IF OBJECT_ID('tempdb..#T_Buyer_Not_INAttendance') IS NOT NULL DROP TABLE #T_Buyer_Not_INAttendance
    IF OBJECT_ID('tempdb..#TMP_FactBuyerAttendanceRecordCount') IS NOT NULL DROP TABLE #TMP_FactBuyerAttendanceRecordCount
			
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
		
		IF OBJECT_ID('tempdb..#TMP_FactBuyerAttendance') IS NOT NULL DROP TABLE #TMP_FactBuyerAttendance
		IF OBJECT_ID('tempdb..#TMP_FactBuyerAttendanceRecordCount') IS NOT NULL DROP TABLE #TMP_FactBuyerAttendanceRecordCount
	    IF OBJECT_ID('tempdb..#T_Buyer_Not_INAttendance') IS NOT NULL DROP TABLE #T_Buyer_Not_INAttendance
        IF OBJECT_ID('tempdb..#TMP_FactBuyerAttendanceRecordCount') IS NOT NULL DROP TABLE #TMP_FactBuyerAttendanceRecordCount

		RETURN -1

END CATCH

END


