/* 
USE EDW
GO 
-- =============================================
-- Author:		Pratyusha Koduru
-- Create date: 01/06/2016 
-- Description:	Populate the LaborManagement table 
-- =============================================
-- REVISION History  
-- ChangeDate  Developer       Release/Problem Number  
   2/10/2016   Yalwarsetty     Added Dispatching Runs and DSC Payments
   02/16/2016 Pratyusha Koduru Added InterBranchTransfersCount, OwnerRetainsCount
   02/18/2016 Pratyusha Koduru Added ProblemReleaseCallsCount
   02/19/2016 Pratyusha Koduru Changed Logic for OwnersRetainCount to remove the Providergroupname changed counts
   03/25/2016 Pratyusha Koduru Added VINVerificationsCount
   03/28/2016 Yalwarsetty      Added NewAssignmentsCount ,NewBuyer/BidderRegistrationCount
   03/31/2016 Pratyusha Koduru Updated InterBranchTransfersCount logic
   04/08/2016 Yalwarsetty      Added WaitReleaseCallCount,FollowUponTitleProblemCount
   04/11/2016 Pratyusha Koduru Added TitleApplicationsProcessingCount
   04/12/2016 Pratyusha Koduru Added PaymentReceipt1PayPulloutsCount,PaymentReceipt2BuyerPayCount
   04/14/2016 Pratyusha Koduru Added IfCallsMinbidsCount
   04/14/2016 Yalwarsetty      Added VisitorPaymentCount ,RegistrationRenewal/UpdateCount
   04/19/2016 Pratyusha Koduru Added PullOutsorLoadOutsCount
   04/20/2016 Pratyusha Koduru Added NumberingtheSaleCount
   04/22/2016 Pratyusha Koduru Added BandageWrapsCount,DeIdLongCount,DeIdShortCount,FullShrinkWrapsCount,PatchWrapsCount
   04/23/2016 Pratyusha Koduru Added InspectionServicesCheckinsCount,LoaderMoveLocationsCount
   04/25/2016 Pratyusha Koduru Added HoldWrapsCount,PrepCheckinsStateFarmCount,ODOProbingcheckinsCount
   04/26/2016 Pratyusha Koduru Added ReImagesCount
   4/29/2016  Matt Stelter - Added Where clauses, commented out Order By
   5/18/2016  Pratyusha Koduru Changed Logic for ODOProbingcheckinsCount(Commented Old Logic)
   5/26/2016  Pratyusha Koduru  Added old logic back ODOProbingcheckinsCount 
   6/15/2016  Pratyusha Koduru Changed Logic for LoaderMoveLocationsCount
   6/20/2016  Pratyusha Koduru Added CheckinsCount
   6/21/2016  Yalwarsetty      Modified HoldWrapsCount Logic
   6/22/2016  Pratyusha Koduru Modified InspectionServicesCheckinsCount logic 
   11/30/2016 Pratyusha Koduru Modified CheckinsCount logic to use Staging table instead of going to Onyard database directly
   12/19/2016 MAtt Stelter	- Add KAR Employee ID
=============================================================================
Usage Example:
Exec usp_ETLLaborManagement
==============================================================================
*/
CREATE PROCEDURE  [dbo].[usp_ETLLaborManagement]  
AS
BEGIN
   DECLARE  @CurrentRunDateTime DateTime,
            @CurrentRunEndDateTime DateTime,
            @GetDate DATETIME, 
            @CurrentRunDayID INT,
		    @CurrentRunWeekID INT,
			@3weeksbackDatetime datetime,
		    @3weeksbackDayID int,
		    @3weeksbackWeekID int,
			@CurrentWeekLastDayOfWeekDayID INT	
SET NOCOUNT ON;

SET @CurrentRunDateTime = 
	(SELECT 
		CASE WHEN datepart(hh,MAXDatetime) between 0 and 14 THEN DateAdd(HOUR,23,CONVERT(DATETIME,CONVERT(varchar(8), MAXDatetime ,112),114))
		ELSE MAXDatetime
		END 
	 FROM EDW.dbo.ETLRUN with (Nolock)
	 WHERE ETLJobName = 'DailyETLJob'
	)
SET @CurrentRunDateTime =@CurrentRunDateTime-6
SET @CurrentRunEndDateTime = (SELECT DATEADD(HH, 20, DATEADD(HH, 00, CONVERT(CHAR(10), GETDATE(), 102))))

SET @GetDate = GETDATE()
select @3weeksbackDatetime = ( 
select dateadd(week,-3,@CurrentRunDateTime	))
--Populate @CurrentRunDayID
SELECT @CurrentRunDayID = CONVERT(varchar(8), @CurrentRunDateTime,112)
select @3weeksbackDayID = CONVERT(varchar(8), @3weeksbackDatetime,112)
--Populate @CurrentWeekID
SELECT @CurrentRunWeekID = FiscalWeekID FROM dbo.DimDay WITH (NOLOCK) WHERE DayID = @CurrentRunDayID

--Populate @CurrentWeekLastDayID
SELECT @CurrentWeekLastDayofWeekDayID = MAX(DayID) FROM dbo.DimDay WITH (NOLOCK) WHERE FiscalWeekID = @CurrentRunWeekID
SELECT @3weeksbackWeekID = FiscalWeekID FROM dbo.DimDay WITH (NOLOCK) WHERE DayID = @3weeksbackDayID

IF OBJECT_ID('tempdb..#TMP_RecordCount') IS NOT NULL DROP TABLE #TMP_RecordCount
CREATE TABLE #TMP_RecordCount(ChangeType VARCHAR(30)) 

--SELECT  CONVERT(char(10), @CurrentRunDateTime,126) , CONVERT(char(10), @CurrentRunEndDateTime,126)

/************************************************************************************
Create the temp table LaborManagement
************************************************************************************/
IF OBJECT_ID('tempdb..#TMP_LaborManagement') IS NOT NULL DROP TABLE #TMP_LaborManagement

CREATE TABLE #TMP_LaborManagement
( [BranchNumber] [int] NOT NULL
  ,[DayId] [int] NOT NULL
  ,[BidFastAssignmentsCount] [int] NULL
  ,[CancellationsCount] [int] NULL
  ,[BuyerCheckinsCount] [int] NULL
  ,[CleanTitlesCount] [int] NULL
  ,[SaleDocEntryCount] [int] NULL
  ,[PrintReportHandoutsCount] [int] NULL
  ,[NYMV907ACount] [int] NULL
  ,[SalesPrepReviewsCount] [int] NULL
  ,[VehiclePulloutsCount] [int] NULL
  ,[InputBillingforEnhancementsCount] [int] NULL
  ,[LoginScanTowerBillsCount] [int] NULL
  ,[ProviderPaymentReconcilationsCount] [int] NULL
  ,[DispatchingRunsCount]  INT NULL
  ,[DSCPaymentsCount] INT NULL
  ,[OwnerRetainsCount] [int] NULL
  ,[ProblemReleaseCallsCount] [int] NULL
  ,[InterBranchTransfersCount] [int] NULL
  ,[VINVerificationsCount] [int] NULL
  ,[NewAssignmentsCount] [int] NULL
  ,[NewBuyerOrBidderRegistrationCount] [int] NULL
  ,[WaitReleaseCallCount] [int] NULL
  ,[FollowUponTitleProblemCount] [int] NULL
  ,[TitleApplicationsProcessingCount] [int] NULL
  ,[PaymentReceipt1PayPulloutsCount] [int] NULL
  ,[PaymentReceipt2BuyerPayCount] [int] NULL
  ,[IfCallsMinbidsCount] [int] NULL
  ,[VisitorPaymentCount] INT NULL
  ,[RegistrationRenewalOrUpdateCount] INT NULL
  ,[PullOutsorLoadOutsCount] INT NULL
  ,[NumberingtheSaleCount] INT NULL
  ,[BandageWrapsCount] INT NULL
  ,[DeIdLongCount] INT NULL
  ,[DeIdShortCount] INT NULL
  ,[FullShrinkWrapsCount] INT NULL
  ,[HoldWrapsCount] INT NULL
  ,[PatchWrapsCount] INT NULL
  ,[InspectionServicesCheckinsCount] INT NULL
  ,[LoaderMoveLocationsCount] INT NULL
  ,[PrepCheckinsStateFarmCount] INT NULL
  ,[ODOProbingcheckinsCount] INT NULL
  ,[ReImagesCount] INT NULL
  ,[CheckinsCount] INT NULL
  ,OnYardUserID int NULL
  ,KAREmployeeID INT NULL
)


INSERT INTO #TMP_LaborManagement
(	[BranchNumber],
    [DayId],
	 OnYardUserID  
)
SELECT DISTINCT DB.BranchNumber,
		DD.[DayId],
		99999
FROM dbo.DimBranch DB WITH(NOLOCK)
CROSS JOIN EDW.dbo.DimDay DD with (nolock)
WHERE DayDate between  CONVERT(char(10), @CurrentRunDateTime,126) and CONVERT(char(10), @CurrentRunEndDateTime,126)

CREATE NONCLUSTERED INDEX [IX_LM_BranchNumber] ON #TMP_LaborManagement
([BranchNumber] ASC)WITH (FILLFACTOR = 100) ON [PRIMARY]

CREATE NONCLUSTERED INDEX [IX_LM_DayId] ON #TMP_LaborManagement
([DayId] ASC)WITH (FILLFACTOR = 100) ON [PRIMARY]

IF OBJECT_ID('tempdb..#TMP_AssignedCancelled') IS NOT NULL DROP TABLE #TMP_AssignedCancelled
CREATE TABLE #TMP_AssignedCancelled
(   [DayId] [int] NOT NULL
   ,[BranchNumber] [int] NOT NULL
   ,[BidFastAssignmentsCount] [int] NULL
   ,[CancellationsCount] [int] NULL
)

INSERT INTO #TMP_AssignedCancelled
(  [DayId]
  ,[BranchNumber] 
  ,[BidFastAssignmentsCount]
  ,[CancellationsCount]
)
SELECT LM.DayId 
       ,DS.AdministrativeBranchNumber
       ,SUM(CASE WHEN DS.AssignmentTypeCode In ('BFA') THEN FA.UnitsAssigned ELSE 0 END) BidFastAssignmentsCount
       ,ABS(SUM(CASE WHEN DS.AssignmentMethod In ('ASAP') THEN FA.CancelledAssignments ELSE 0 END)) CancellationsCount
FROM EDW.dbo.FactAssignment FA WITH(NOLOCK)  
INNER JOIN EDW.dbo.DimStock DS WITH(NOLOCK)
 ON FA.StockID = DS.StockID
INNER JOIN EDW.dbo.FactStockCycleTime FSCT WITH(NOLOCK)
 ON FSCT.StockID = DS.StockID
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON  FA.AssignedDayID = LM.DayID
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
WHERE DS.AssignmentTypeCode ='BFA' OR DS.AssignmentMethod = 'ASAP'
GROUP BY LM.DayId, DS.AdministrativeBranchNumber
--ORDER BY 1 DESC

IF OBJECT_ID('tempdb..#TMP_Attendance') IS NOT NULL DROP TABLE #TMP_Attendance 
CREATE TABLE #TMP_Attendance
(	[DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
	,[BuyerCheckinsCount] [int] NULL
	,[PrintReportHandoutsCount] [int] NULL
)

INSERT INTO #TMP_Attendance
(	[DayId]
    ,[BranchNumber] 
	,[BuyerCheckinsCount]
	,[PrintReportHandoutsCount]
)
SELECT  LM.DayId
       ,DA.BranchNumber
	   ,SUM(FBA.OnsiteAttendees) BuyerCheckinsCount
	   ,SUM(FBA.OnsiteAttendees)/4 PrintReportHandoutsCount
FROM EDW.dbo.FactBuyerAttendance FBA WITH (NOLOCK)  
INNER JOIN EDW.dbo.DimAuction DA WITH (NOLOCK) 
 ON FBA.AuctionID = DA.AuctionID 
 AND FBA.BranchNumber = DA.BranchNumber
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON DA.AuctionDayID  = LM.DayID
 AND DA.BranchNumber = LM.BranchNumber
GROUP BY LM.DayId, DA.BranchNumber
--ORDER BY 1 DESC


IF OBJECT_ID('tempdb..#TMP_SaleDoc') IS NOT NULL DROP TABLE #TMP_SaleDoc 
CREATE TABLE #TMP_SaleDoc
(	[DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[CleanTitlesCount] [int] NULL
    ,[SaleDocEntryCount] [int] NULL
	,[NYMV907ACount] [int] NULL
)

INSERT INTO #TMP_SaleDoc
(	[DayId]
    ,[BranchNumber] 
    ,[CleanTitlesCount]
    ,[SaleDocEntryCount]
	,[NYMV907ACount]
)
 SELECT  LM.DayID
       ,DS.AdministrativeBranchNumber
	   ,SUM(CASE WHEN DS.SaleDocumentTypeCategory = 'Clear'  THEN 1 ELSE 0 END) CleanTitlesCount
	   ,COUNT(DS.SaleDocNumber)  SaleDocEntryCount
       ,SUM(CASE WHEN DS.TitleState = 'NY' AND DS.TitleType IN ('MV-907A')  THEN 1 ELSE 0 END) NYMV907ACount
FROM EDW.[dbo].[FactStockCycleTime] FSCT WITH (NOLOCK)  
INNER JOIN EDW.[dbo].DimStock DS WITH (NOLOCK) 
 ON FSCT.StockID = DS.StockID
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON FSCT.SaleDocumentReceivedDayID  = LM.DayID
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
GROUP BY LM.DayId, DS.AdministrativeBranchNumber
--ORDER BY 1 DESC


IF OBJECT_ID('tempdb..#TMP_SalesPrepReviews') IS NOT NULL DROP TABLE #TMP_SalesPrepReviews
CREATE TABLE #TMP_SalesPrepReviews
(	[DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[SalesPrepReviewsCount] [int] NULL
)

INSERT INTO #TMP_SalesPrepReviews
(	[DayId]
    ,[BranchNumber] 
    ,[SalesPrepReviewsCount]
)
	SELECT  LM.DayId
       ,DS.AdministrativeBranchNumber
	   ,SUM(CASE WHEN ProcessStatusCode = 'K10' THEN 1 ELSE 0 END) AS SalesPrepReviewsCount
FROM EDW.[dbo].DimStock DS WITH (NOLOCK)  
INNER JOIN EDW.[dbo].[FactStockCycleTime] FSCT WITH (NOLOCK) 
 ON  DS.StockID = FSCT.StockID 
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON FSCT.CurrentProcessStatusStartDayID  = LM.DayID
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
WHERE ProcessStatusCode = 'K10'
GROUP BY LM.DayId, DS.AdministrativeBranchNumber
--ORDER BY 1 DESC


IF OBJECT_ID('tempdb..#TMP_Pullouts') IS NOT NULL DROP TABLE #TMP_Pullouts
CREATE TABLE #TMP_Pullouts
(	[DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[VehiclePulloutsCount] [int] NULL
)

INSERT INTO #TMP_Pullouts
(	[DayId]
    ,[BranchNumber] 
    ,[VehiclePulloutsCount]
)
	SELECT  LM.DayId
       ,DS.AdministrativeBranchNumber
	   ,SUM(CASE WHEN DSE.EventCode in ('BYPU')  THEN 1 ELSE 0 END) VehiclePulloutsCount
FROM EDW.[dbo].[FactStockCycleTime] FSCT WITH (NOLOCK)  
INNER JOIN EDW.[dbo].DimStock DS WITH (NOLOCK) 
 ON FSCT.StockID = DS.StockID
INNER JOIN EDW.dbo.FactStockEvent as FSE WITH (NOLOCK)
ON FSE.ASAPSalvageID = DS.ASAPSalvageID
AND FSE.EventTypeID =179
INNER JOIN EDW.dbo.DimStockEventType as DSE WITH (NOLOCK)
ON FSE.EventTypeID = DSE.EventTypeID    
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON FSCT.BuyerPickupDayID  = LM.DayID
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
GROUP BY LM.DayId, DS.AdministrativeBranchNumber
--ORDER BY 1 DESC

IF OBJECT_ID('tempdb..#TMP_Enhancements') IS NOT NULL DROP TABLE #TMP_Enhancements
CREATE TABLE #TMP_Enhancements
(	[DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[InputBillingforEnhancementsCount] [int] NULL
)
INSERT INTO #TMP_Enhancements
(	[DayId]
    ,[BranchNumber] 
    ,[InputBillingforEnhancementsCount]
)
SELECT  FF.TransactionDayID
       ,DS.AdministrativeBranchNumber
       ,SUM(TheCount) InputBillingforEnhancementsCount
FROM (SELECT  FF.TransactionDayID,
			   StockID, COUNT(*) AS TheCount
		FROM [EDW].[dbo].[FactFinancialTransaction] FF WITH (NOLOCK)
		INNER JOIN [EDW].[dbo].[DimFinancialTransactionType] DF  WITH(NOLOCK)
		  ON FF.FinancialTransactionTypeID = DF.TransactionID
		  AND DF.TransactionCategoryId = '21'
		INNER JOIN  (SELECT DISTINCT DayID FROM #TMP_LaborManagement WITH(NOLOCK)) LM 
		 ON FF.TransactionDayID = LM.DayID
		 GROUP BY FF.TransactionDayID, StockID) FF
INNER JOIN EDW.[dbo].DimStock DS WITH (NOLOCK) 
  ON FF.StockID = DS.StockID
GROUP BY FF.TransactionDayID, DS.AdministrativeBranchNumber

--SELECT  LM.DayId
--       ,DS.AdministrativeBranchNumber
--       ,SUM(CASE WHEN DF.TransactionCategoryId = '21' THEN 1 ELSE 0 END) InputBillingforEnhancementsCount
--FROM [EDW].[dbo].[FactFinancialTransaction] FF WITH (NOLOCK)
--INNER JOIN EDW.[dbo].DimStock DS WITH (NOLOCK) 
--  ON FF.StockID = DS.StockID
--INNER JOIN [EDW].[dbo].[DimFinancialTransactionType] DF  WITH(NOLOCK)
--  ON FF.FinancialTransactionTypeID = DF.TransactionID
--INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
-- ON FF.TransactionDayID = LM.DayID
-- AND DS.AdministrativeBranchNumber = LM.BranchNumber
--WHERE DF.TransactionCategoryId = '21'
--GROUP BY LM.DayId, DS.AdministrativeBranchNumber
----ORDER BY 1 DESC

IF OBJECT_ID('tempdb..#TMP_Logins') IS NOT NULL DROP TABLE #TMP_Logins
CREATE TABLE #TMP_Logins
(	[DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[LoginScanTowerBillsCount] [int] NULL
)

INSERT INTO #TMP_Logins
(	[DayId]
    ,[BranchNumber] 
    ,[LoginScanTowerBillsCount]
)
	SELECT  LM.DayId
       ,DS.AdministrativeBranchNumber
	   ,COUNT(DS.StockId) LoginScanTowerBillsCount
FROM EDW.[dbo].[FactStockCycleTime] FSCT WITH (NOLOCK)  
INNER JOIN EDW.[dbo].DimStock DS WITH (NOLOCK) 
 ON FSCT.StockID = DS.StockID
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON FSCT.LogInDayID   = LM.DayID
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
GROUP BY LM.DayId, DS.AdministrativeBranchNumber
--ORDER BY 1 DESC

IF OBJECT_ID('tempdb..#TMP_ProviderPaymentReconcilations') IS NOT NULL DROP TABLE #TMP_ProviderPaymentReconcilations
CREATE TABLE #TMP_ProviderPaymentReconcilations
(	[DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[ProviderPaymentReconcilationsCount] [int] NULL
)

INSERT INTO #TMP_ProviderPaymentReconcilations
(	[DayId]
    ,[BranchNumber] 
    ,[ProviderPaymentReconcilationsCount]
)
	SELECT  LM.DayId
       ,DS.AdministrativeBranchNumber
	   ,Count(DS.StockId) ProviderPaymentReconcilationsCount
FROM EDW.[dbo].[FactStockCycleTime] FSCT WITH (NOLOCK)  
INNER JOIN EDW.[dbo].DimStock DS WITH (NOLOCK) 
 ON FSCT.StockID = DS.StockID
INNER JOIN EDW.dbo.FactStockChargeExpense FSCE WITH(NOLOCK)
ON FSCE.StockId = DS.StockId
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON FSCT.StockClosedDayID = LM.DayID
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
WHERE FSCT.[SettlementDayID] <> 29991231
AND CASE WHEN (Isnull(DS.GrossReturn, 0) - Isnull(FSCE.AdvanceTotalCharge, 0) - Isnull(FSCE.ProviderTotalCharge, 0) - 
         Isnull(FSCE.AdvanceTaxCharge, 0) - Isnull(FSCE.ProviderTaxCharge, 0) + ( Isnull(DS.mInRemitDiscount, 0) + Isnull(DS.ZeroDeficitAdjustmentAmount, 0) ) + Isnull(DS. netsplitadjustment, 0)  = 0) 
     OR Isnull(DS.IsStockSold, 0) = 0 THEN NULL
	ELSE Isnull(DS.GrossReturn, 0) - Isnull(FSCE.AdvanceTotalCharge, 0) - Isnull(FSCE.ProviderTotalCharge, 0) 
	- Isnull(FSCE.AdvanceTaxCharge, 0) - Isnull(FSCE.ProviderTaxCharge, 0) + ( Isnull(DS.mInRemitDiscount, 0) 
	+ Isnull(DS.ZeroDeficitAdjustmentAmount, 0) ) + Isnull(DS. netsplitadjustment, 0) END < 0
GROUP BY LM.DayId, DS.AdministrativeBranchNumber
--ORDER BY 1 DESC


IF OBJECT_ID('tempdb..#TMP_DispatchingRuns') IS NOT NULL DROP TABLE #TMP_DispatchingRuns 
CREATE TABLE #TMP_DispatchingRuns
(	 [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[DispatchingRunsCount] [int] NULL
  
)

INSERT INTO #TMP_DispatchingRuns
(	[DayId]
   ,[BranchNumber] 
   ,[DispatchingRunsCount]
   
	
)
SELECT  LM.DayId
       ,DS.AdministrativeBranchNumber
	   ,SUM(Case when CONVERT(char(10),[DispatchDateTime],126) between  CONVERT(char(10), @CurrentRunDateTime,126) and CONVERT(char(10), @CurrentRunEndDateTime,126) then 1 else 0 end ) [DispatchingRunsCount]
FROM EDW.[dbo].[FactStockCycleTime] FSCT WITH (NOLOCK)  
INNER JOIN EDW.[dbo].DimStock DS WITH (NOLOCK) 
 ON FSCT.StockID = DS.StockID
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON FSCT.[DispatchDayID]   = LM.DayID
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
WHERE CONVERT(char(10),[DispatchDateTime],126) between  CONVERT(char(10), @CurrentRunDateTime,126) and CONVERT(char(10), @CurrentRunEndDateTime,126)
GROUP BY LM.DayId, DS.AdministrativeBranchNumber
--ORDER BY 1 DESC

IF OBJECT_ID('tempdb..#TMP_DSCPaymentsCount') IS NOT NULL DROP TABLE #TMP_DSCPaymentsCount
CREATE TABLE #TMP_DSCPaymentsCount
(	 [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[DSCPaymentsCount] [int] NULL
	
)

INSERT INTO #TMP_DSCPaymentsCount
(	 [DayId]
    ,[BranchNumber] 
    ,[DSCPaymentsCount]
	
)
SELECT  LM.DayId
       ,DS.AdministrativeBranchNumber
	   ,SUM(Case when DS.[BuyerFinanceCompanyCode]='DSC' and 
	    CONVERT(char(10),[BuyerFinanceDateTime],126) between  CONVERT(char(10), @CurrentRunDateTime,126) and CONVERT(char(10), @CurrentRunEndDateTime,126) then 1 else 0 end ) [DSCPaymentsCount]

FROM EDW.[dbo].[FactStockCycleTime] FSCT WITH (NOLOCK)  
INNER JOIN EDW.[dbo].DimStock DS WITH (NOLOCK) 
 ON FSCT.StockID = DS.StockID
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON ( CONVERT(VARCHAR(8),[BuyerFinanceDateTime] , 112) ) = LM.DayID
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
WHERE DS.[BuyerFinanceCompanyCode]='DSC'
GROUP BY LM.DayId, DS.AdministrativeBranchNumber
--ORDER BY 1 DESC


IF OBJECT_ID('tempdb..#TMP_OwnerRetainsCount') IS NOT NULL DROP TABLE #TMP_OwnerRetainsCount
CREATE TABLE #TMP_OwnerRetainsCount
(	 [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[OwnerRetainsCount] [int] NULL
)
INSERT INTO #TMP_OwnerRetainsCount
(	 [DayId]
    ,[BranchNumber] 
    ,[OwnerRetainsCount]
)
SELECT  LM.DayId
       ,DS.AdministrativeBranchNumber
      ,SUM(CASE WHEN DS.ProcessStatusDescription = 'Return Tow Complete' AND DS.StockCancelReasonCode = 'OWR' THEN 1
			 ELSE 0 END) AS OwnerRetainsCount
FROM EDW.[dbo].DimStock DS WITH (NOLOCK)  
INNER JOIN EDW.[dbo].[FactStockCycleTime] FSCT WITH (NOLOCK) 
 ON  DS.StockID = FSCT.StockID 
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON FSCT.CurrentProcessStatusStartDayID  = LM.DayID
  AND DS.AdministrativeBranchNumber = LM.BranchNumber
WHERE DS.ProcessStatusDescription = 'Return Tow Complete' AND DS.StockCancelReasonCode = 'OWR'
GROUP BY LM.DayId, DS.AdministrativeBranchNumber
--ORDER BY 1 DESC

IF OBJECT_ID('tempdb..#TMP_ProblemReleaseCallsCount') IS NOT NULL DROP TABLE #TMP_ProblemReleaseCallsCount
CREATE TABLE #TMP_ProblemReleaseCallsCount
(	 [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[ProblemReleaseCallsCount][int] NULL
)
INSERT INTO #TMP_ProblemReleaseCallsCount
(	 [DayId]
    ,[BranchNumber] 
    ,[ProblemReleaseCallsCount]
)
SELECT LM.DayId
      ,DS.AdministrativeBranchNumber
      ,SUM(CASE WHEN Salvage_Event_Category_Code = 'RLP'AND Note_Text like  '%Release Problem Created%' THEN 1
                WHEN Salvage_Event_Category_Code = 'RLP'AND Note_Text like  '%Release Problem Resolved%' THEN 1
                WHEN Salvage_Event_Category_Code = 'RLP'AND Note_Text like  '%Called%' AND Note_Text NOT LIKE '%From:%'THEN 1 
				ELSE 0 END) AS ProblemReleaseCallsCount
FROM BI_ASAP_Rep.dbo.Salvage_Note SN WITH(NOLOCK)
INNER JOIN EDW.dbo.DimStock DS WITH(NOLOCK)
ON SN.Salvage_ID = DS.ASAPSalvageID
INNER JOIN #TMP_LaborManagement LM WITH(NOLOCK)
ON CONVERT(INT, CONVERT(CHAR(8), SN.Update_DateTime, 112)) = LM.DayID
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
INNER JOIN EDW.dbo.FactReleaseProblem FSCT WITH(NOLOCK)
ON DS.StockID = FSCT.StockID
WHERE Salvage_Event_Category_Code = 'RLP'
GROUP BY LM.DayId, DS.AdministrativeBranchNumber
--ORDER BY 1 DESC

IF OBJECT_ID('tempdb..#TMP_SendingTransfers') IS NOT NULL DROP TABLE #TMP_SendingTransfers
IF OBJECT_ID('tempdb..#TMP_ReceivingTransfers') IS NOT NULL DROP TABLE #TMP_ReceivingTransfers
IF OBJECT_ID('tempdb..#TMP_SendingCounts') IS NOT NULL DROP TABLE #TMP_SendingCounts
IF OBJECT_ID('tempdb..#TMP_ReceivingCounts') IS NOT NULL DROP TABLE #TMP_ReceivingCounts


SELECT  DISTINCT DS.StockID ,LM.DayId
       ,DS.AdministrativeBranchNumber AS BranchNumber
INTO #TMP_SendingTransfers   	
FROM EDW.[dbo].DimStock DS WITH (NOLOCK)  
INNER JOIN EDW.[dbo].[FactStockCycleTime] FSCT WITH (NOLOCK) 
 ON  DS.StockID = FSCT.StockID 
INNER JOIN EDW.dbo.DimDay LM WITH(NoLock)
 ON LM.DayId = FSCT.DispatchDayID
 INNER JOIN EDW.dbo.FactStockTow FS WITH(NoLock)
 ON DS.StockID = FS.StockID
 WHERE FS.TowTypeCode = 'IB'


SELECT   DISTINCT DS.StockID ,LM.DayId
       ,DS.AdministrativeBranchNumber AS BranchNumber
INTO #TMP_ReceivingTransfers
FROM EDW.[dbo].DimStock DS WITH (NOLOCK)  
INNER JOIN EDW.[dbo].[FactStockCycleTime] FSCT WITH (NOLOCK) 
 ON  DS.StockID = FSCT.StockID 
INNER JOIN EDW.dbo.DimDay LM WITH(NoLock)
 ON LM.DayId = FSCT.LogInDayID 
 INNER JOIN EDW.dbo.FactStockTow FS WITH(NoLock)
 ON DS.StockID = FS.StockID
 WHERE FS.TowTypeCode = 'IB'


SELECT STC.DayId,STC.BranchNumber
      ,COUNT(STC.StockID) AS SendingCounts
INTO #TMP_SendingCounts
FROM  #TMP_SendingTransfers STC  WITH(NOLOCK)
GROUP BY STC.DayId,STC.BranchNumber


SELECT RTC.DayId,RTC.BranchNumber
      ,COUNT(RTC.StockID) AS ReceivingCounts
INTO #TMP_ReceivingCounts
FROM  #TMP_ReceivingTransfers RTC WITH(NOLOCK)
GROUP BY RTC.DayId,RTC.BranchNumber


IF OBJECT_ID('tempdb..#TMP_VINVerificationsCount') IS NOT NULL DROP TABLE #TMP_VINVerificationsCount
CREATE TABLE #TMP_VINVerificationsCount
(	 [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[VINVerificationsCount][int] NULL
)
INSERT INTO #TMP_VINVerificationsCount
(	 [DayId]
    ,[BranchNumber] 
    ,[VINVerificationsCount]
)
SELECT LM.DayId
      ,DS.AdministrativeBranchNumber
      ,SUM(CASE WHEN TitleProblemDescription Like '%VIN%' THEN 1 ELSE 0 END) AS VINVerificationsCount
FROM [EDW].[dbo].[FactTitleProblem] FT WITH(NOLOCK)
INNER JOIN EDW.[dbo].DimStock DS WITH (NOLOCK) 
 ON FT.StockID = DS.StockID
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON FT.TitleProblemEndDayID = LM.DayID
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
WHERE TitleProblemDescription Like '%VIN%'
GROUP BY LM.DayId, DS.AdministrativeBranchNumber
--ORDER BY 1 DESC


IF OBJECT_ID('tempdb..#TMP_NewAssignmentsCount') IS NOT NULL DROP TABLE #TMP_NewAssignmentsCount
CREATE TABLE #TMP_NewAssignmentsCount
(	 [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[NewAssignmentsCount] [int] NULL
  
)

Insert Into #TMP_NewAssignmentsCount
(
 [DayId] ,
 [BranchNumber] ,
 [NewAssignmentsCount]

)
SELECT  LM.DayId
       ,DS.AdministrativeBranchNumber
	   ,SUM(Case when DS.AssignmentMethod='ASAP' and 
	    CONVERT(char(10),[AssignmentDateTime],126) between  CONVERT(char(10), @CurrentRunDateTime,126) and CONVERT(char(10), @CurrentRunEndDateTime,126) then 1 else 0 end ) [NewAssignmentsCount]

FROM EDW.[dbo].[FactStockCycleTime] FSCT WITH (NOLOCK)  
INNER JOIN EDW.[dbo].DimStock DS WITH (NOLOCK) 
 ON FSCT.StockID = DS.StockID
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON FSCT.[AssignmentDayID]= LM.DayID
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
WHERE DS.AssignmentMethod='ASAP'
GROUP BY LM.DayId, DS.AdministrativeBranchNumber
--ORDER BY 1 DESC

IF OBJECT_ID('tempdb..#TMP_NewBidderRegistrationCount') IS NOT NULL DROP TABLE #TMP_NewBidderRegistrationCount
CREATE TABLE #TMP_NewBidderRegistrationCount
(	 [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[NewBuyerOrBidderRegistrationCount] [int] NULL
  
)

Insert Into #TMP_NewBidderRegistrationCount
(
 [DayId] ,
 [BranchNumber] ,
 [NewBuyerOrBidderRegistrationCount]

)
SELECT  LM.DayId
       ,FFT.BuyerChargeBranchNumber AdministrativeBranchNumber
	 ,sum (Case when [TransactionDescription] In ('Registration Fee-Annual','Additional Buyer Fee','Guest Fee')
                 and [TransactionClassDescription]='Registration'
                 and [ChargeStatus]='Booked and Paid'
                 and DFTT.TransactionCustomerID = 1
                 and COALESCE( FFT.OverrideAmount, FFT.Amount )>0
				 and CONVERT(char(10),UpdateDatetime,126) between  CONVERT(char(10), @CurrentRunDateTime,126) and CONVERT(char(10), @CurrentRunEndDateTime,126)
then 1 else 0 end)[NewBuyerOrBidderRegistrationCount]
 FROM [EDW].[dbo].[FactFinancialTransaction] FFT With(NOLOCK)
LEFT OUTER JOIN [EDW].[dbo].[DimFinancialTransactionType] DFTT With(NOLOCK)
ON FFT.FinancialTransactionTypeID=DFTT.FinancialTransactionTypeID
LEFT OUTER JOIN [EDW].[dbo].[DimFinancialTransactionSubType] DFTST With(NOLOCK)
on DFTST.FinancialTransactionSubTypeID=FFT.FinancialTransactionSubTypeID
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON FFT.[UpdateDayID]= LM.DayID
 AND FFT.BuyerChargeBranchNumber = LM.BranchNumber
WHERE [TransactionDescription] In ('Registration Fee-Annual','Additional Buyer Fee','Guest Fee')
                 and [TransactionClassDescription]='Registration'
                 and [ChargeStatus]='Booked and Paid'
                 and DFTT.TransactionCustomerID = 1
GROUP BY LM.DayId, FFT.BuyerChargeBranchNumber
--ORDER BY 1 DESC


IF OBJECT_ID('tempdb..#TMP_WaitReleaseCallCount') IS NOT NULL DROP TABLE #TMP_WaitReleaseCallCount
CREATE TABLE #TMP_WaitReleaseCallCount
(	 [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[WaitReleaseCallCount] [int] NULL
  
)

Insert Into #TMP_WaitReleaseCallCount
(
 [DayId] ,
 [BranchNumber] ,
 [WaitReleaseCallCount]

)
SELECT  LM.DayId
       ,DS.AdministrativeBranchNumber
	   ,SUM(Case when DS.ProcessStatusDescription IN ('Release Problem','Wait Dispatch','Wait Driver','Pickup En Route') and 
	    CONVERT(char(10),ReleaseDateTime,126) between  CONVERT(char(10), @CurrentRunDateTime,126) and CONVERT(char(10), @CurrentRunEndDateTime,126) then 1 else 0 end ) [WaitReleaseCallCount]

FROM EDW.[dbo].[FactStockCycleTime] FSCT WITH (NOLOCK)  
INNER JOIN EDW.[dbo].DimStock DS WITH (NOLOCK) 
 ON FSCT.StockID = DS.StockID
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON FSCT.[ReleaseDayID]= LM.DayID
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
WHERE DS.ProcessStatusDescription IN ('Release Problem','Wait Dispatch','Wait Driver','Pickup En Route')
GROUP BY LM.DayId, DS.AdministrativeBranchNumber
--ORDER BY 1 DESC



IF OBJECT_ID('tempdb..#TMP_FollowUponTitleProblemCount') IS NOT NULL DROP TABLE #TMP_FollowUponTitleProblemCount
CREATE TABLE #TMP_FollowUponTitleProblemCount
(	 [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[FollowUponTitleProblemCount] [int] NULL
  
)
Insert Into #TMP_FollowUponTitleProblemCount
(
 [DayId] ,
 [BranchNumber] ,
 [FollowUponTitleProblemCount]

)

Select 
LM.DayId,
DS.AdministrativeBranchNumber,
SUM(Case when CONVERT(char(10),FTP.TitleProblemStartDate,126) between  CONVERT(char(10), @CurrentRunDateTime,126) and CONVERT(char(10), @CurrentRunEndDateTime,126) then 1 else 0 end ) as [FollowUponTitleProblemCount]
from edw.dbo.dimstock ds With(NOLOCK)
Left outer join edw.[dbo].[FactTitleProblem] FTP With(NOLOCK)
on FTP.Stockid=ds.stockid
Join (select 
Distinct Stockid,
Max(TitleProblemStartDate) TitleProblemStartDate 

FROM  edw.[dbo].[FactTitleProblem]  With(NOLOCK)

Group by Stockid) a
On FTP.stockid=a.stockid
and FTP.TitleProblemStartDate=a.TitleProblemStartDate
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
ON FTP.TitleProblemStartDayID= LM.DayID
AND DS.AdministrativeBranchNumber = LM.BranchNumber
Group by LM.DayId,
DS.AdministrativeBranchNumber


IF OBJECT_ID('tempdb..#TMP_TitleApplicationsProcessingCount') IS NOT NULL DROP TABLE #TMP_TitleApplicationsProcessingCount
CREATE TABLE #TMP_TitleApplicationsProcessingCount
(	 [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[TitleApplicationsProcessingCount] [int] NULL
  
)
INSERT INTO #TMP_TitleApplicationsProcessingCount
(
 [DayId] ,
 [BranchNumber] ,
 [TitleApplicationsProcessingCount]
)
SELECT  LM.DayId
       ,DS.AdministrativeBranchNumber
	   ,SUM(CASE WHEN DS.PreviousProcessStatusDescription = 'Wait Title Application' and DS.ProcessStatusDescription in  ('Wait Sale Document','Title Problem')
	    THEN 1 ELSE 0 END ) AS TitleApplicationsProcessingCount
FROM EDW.[dbo].DimStock DS WITH (NOLOCK)  
INNER JOIN EDW.[dbo].[FactStockCycleTime] FSCT WITH (NOLOCK) 
 ON FSCT.StockID = DS.StockID
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON FSCT.[CurrentProcessStatusStartDayID]= LM.DayID
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
WHERE DS.PreviousProcessStatusDescription = 'Wait Title Application' and DS.ProcessStatusDescription in  ('Wait Sale Document','Title Problem')
GROUP BY LM.DayId, DS.AdministrativeBranchNumber
--ORDER BY 1 DESC

IF OBJECT_ID('tempdb..#TMP_PaymentReceiptsCount') IS NOT NULL DROP TABLE #TMP_PaymentReceiptsCount
CREATE TABLE #TMP_PaymentReceiptsCount
(	 [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[PaymentReceipt1PayPulloutsCount] [int] NULL
	,[PaymentReceipt2BuyerPayCount] [int] NULL
  
)
INSERT INTO #TMP_PaymentReceiptsCount
(
 [DayId] ,
 [BranchNumber] ,
 [PaymentReceipt1PayPulloutsCount],
 [PaymentReceipt2BuyerPayCount]
)
SELECT  LM.DayId
       ,DS.AdministrativeBranchNumber
	   ,SUM(CASE WHEN FSCT.BuyerPaymentDayID = FSCT.BuyerPickupDayID THEN 1 ELSE 0 END ) AS PaymentReceipt1PayPulloutsCount
	   ,SUM(CASE WHEN FSCT.BuyerPaymentDayID <> FSCT.BuyerPickupDayID THEN 1 ELSE 0 END ) AS PaymentReceipt2BuyerPayCount
FROM EDW.[dbo].DimStock DS WITH (NOLOCK)  
INNER JOIN EDW.[dbo].[FactStockCycleTime] FSCT WITH (NOLOCK) 
 ON FSCT.StockID = DS.StockID
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON FSCT.BuyerPaymentDayID = LM.DayID
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
GROUP BY LM.DayId, DS.AdministrativeBranchNumber
--ORDER BY 1 DESC


IF OBJECT_ID('tempdb..#TMP_IfCallsMinbidsCount') IS NOT NULL DROP TABLE #TMP_IfCallsMinbidsCount
CREATE TABLE #TMP_IfCallsMinbidsCount
(	 [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[IfCallsMinbidsCount] [int] NULL
  
)
Insert Into #TMP_IfCallsMinbidsCount
(
 [DayId] ,
 [BranchNumber] ,
 [IfCallsMinbidsCount] 
)
SELECT  LM.DayId
       ,DS.AdministrativeBranchNumber
	   ,SUM(CASE WHEN DS.PreviousProcessStatusDescription = 'Sale Pending' AND DS.ProcessStatusDescription in  ('Wait Buyer Payment','Ready for Sale')
	    THEN 1 ELSE 0 END ) AS IfCallsMinbidsCount
FROM EDW.[dbo].DimStock DS WITH (NOLOCK)  
INNER JOIN EDW.[dbo].[FactStockCycleTime] FSCT WITH (NOLOCK) 
 ON FSCT.StockID = DS.StockID
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON FSCT.[CurrentProcessStatusStartDayID]= LM.DayID
  AND DS.AdministrativeBranchNumber = LM.BranchNumber
WHERE DS.PreviousProcessStatusDescription = 'Sale Pending' AND DS.ProcessStatusDescription in  ('Wait Buyer Payment','Ready for Sale')
 GROUP BY LM.DayId, DS.AdministrativeBranchNumber
--ORDER BY 1 DESC


IF OBJECT_ID('tempdb..#TMP_VisitorPaymentCount') IS NOT NULL DROP TABLE #TMP_VisitorPaymentCount
CREATE TABLE #TMP_VisitorPaymentCount
(	 [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[VisitorPaymentCount] [int] NULL
  
)
Insert Into #TMP_VisitorPaymentCount
(
 [DayId] ,
 [BranchNumber] ,
 [VisitorPaymentCount]

)
SELECT  LM.DayId
       ,FFT.BuyerChargeBranchNumber  AdministrativeBranchNumber
	 ,sum (Case when [TransactionDescription] In ('Guest Fee')
                 and [ChargeStatus]='Booked and Paid'
                 and COALESCE( FFT.OverrideAmount, FFT.Amount )>0
				 and CONVERT(char(10),UpdateDatetime,126) between  CONVERT(char(10), @CurrentRunDateTime,126) and CONVERT(char(10), @CurrentRunEndDateTime,126)
then 1 else 0 end)[VisitorPaymentCount]
 FROM [EDW].[dbo].[FactFinancialTransaction] FFT With(NOLOCK)

LEFT OUTER JOIN [EDW].[dbo].[DimFinancialTransactionType] DFTT With(NOLOCK)
ON FFT.FinancialTransactionTypeID=DFTT.FinancialTransactionTypeID
LEFT OUTER JOIN [EDW].[dbo].[DimFinancialTransactionSubType] DFTST With(NOLOCK)
on DFTST.FinancialTransactionSubTypeID=FFT.FinancialTransactionSubTypeID
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON FFT.[UpdateDayID]= LM.DayID
 AND FFT.BuyerChargeBranchNumber  = LM.BranchNumber
 where [TransactionDescription] In ('Guest Fee')
and COALESCE( FFT.OverrideAmount, FFT.Amount )>0
and [ChargeStatus]='Booked and Paid'
GROUP BY LM.DayId, FFT.BuyerChargeBranchNumber 
--ORDER BY 1 DESC




IF OBJECT_ID('tempdb..#TMP_RegistrationRenewal') IS NOT NULL DROP TABLE #TMP_RegistrationRenewal
CREATE TABLE #TMP_RegistrationRenewal
(	 [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[RegistrationRenewalOrUpdateCount] [int] NULL
  
)
Insert Into #TMP_RegistrationRenewal
(
 [DayId] ,
 [BranchNumber] ,
 [RegistrationRenewalOrUpdateCount]

)

SELECT  LM.DayId
       ,FFT.BuyerChargeBranchNumber AdministrativeBranchNumber
	 ,sum (Case when [TransactionDescription] In ('Registration Fee-Annual','Additional Buyer Fee')
                 and [TransactionClassDescription]='Registration'
                 and [ChargeStatus]='Booked and Paid'
                 and DFTT.TransactionCustomerID = 1
                 and COALESCE( FFT.OverrideAmount, FFT.Amount )>0
				
				 and CONVERT(char(10),UpdateDatetime,126) between  CONVERT(char(10), @CurrentRunDateTime,126) and CONVERT(char(10), @CurrentRunEndDateTime,126)
then 1 else 0 end)[RegistrationRenewalOrUpdateCount]
 FROM 
[EDW].[dbo].[FactFinancialTransaction] FFT With(NOLOCK)

LEFT OUTER JOIN [EDW].[dbo].[DimFinancialTransactionType] DFTT With(NOLOCK)
ON FFT.FinancialTransactionTypeID=DFTT.FinancialTransactionTypeID
LEFT OUTER JOIN [EDW].[dbo].[DimFinancialTransactionSubType] DFTST With(NOLOCK)
on DFTST.FinancialTransactionSubTypeID=FFT.FinancialTransactionSubTypeID
LEFT OUTER Join (select buyerid,Max( InitialSetupDateTime)InitialSetupDateTime from  [EDW].[dbo].[DimBuyerEmployee] WITH(NOLOCK)
where CONVERT(char(10),InitialSetupDateTime,126) < CONVERT(char(10), @CurrentRunDateTime,126) 
Group by buyerid)a
On a.buyerid=FFT.buyerid
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON FFT.[UpdateDayID]= LM.DayID
 AND FFT.BuyerChargeBranchNumber = LM.BranchNumber
 where  [TransactionDescription] In ('Registration Fee-Annual','Additional Buyer Fee')
                 and [TransactionClassDescription]='Registration'
                 and [ChargeStatus]='Booked and Paid'
                 and DFTT.TransactionCustomerID = 1
                 and COALESCE( FFT.OverrideAmount, FFT.Amount )>0
GROUP BY LM.DayId,FFT.BuyerChargeBranchNumber 
--ORDER BY 1 DESC


IF OBJECT_ID('tempdb..#TMP_PullOutsorLoadOutsCount') IS NOT NULL DROP TABLE #TMP_PullOutsorLoadOutsCount
IF OBJECT_ID('tempdb..#TMP_Pulls') IS NOT NULL DROP TABLE #TMP_Pulls

SELECT DISTINCT PM.[Salvage_ID],
		ISNULL(CONVERT(INT, CONVERT(CHAR(8),PM.End_DateTime, 112)), '29991231') AS [EndDayId],
		OnYard_User_ID
INTO #TMP_Pulls
FROM  Staging.[dbo].[OnYardPulloutMetrics] PM WITH(NOLOCK)
--GROUP BY PM.[Salvage_ID] ,[End_DateTime],OnYard_User_ID

CREATE TABLE #TMP_PullOutsorLoadOutsCount
(	 [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[PullOutsorLoadOutsCount] [int] NULL
	,OnYard_User_ID INT NULL
 )
INSERT INTO #TMP_PullOutsorLoadOutsCount
(
 [DayId] ,
 [BranchNumber] ,
 [PullOutsorLoadOutsCount],
  OnYard_User_ID
)
SELECT  LM.DayId
       ,DS.AdministrativeBranchNumber
	  ,Count(PO.Salvage_ID) AS PullOutsorLoadOutsCount
	  ,OnYard_User_ID
FROM EDW.[dbo].DimStock DS WITH (NOLOCK)  
INNER JOIN #TMP_Pulls PO WITH (NOLOCK) 
  ON PO.Salvage_ID =  DS.ASAPSalvageID
INNER JOIN #TMP_LaborManagement LM WITH(NOLOCK)
 ON PO.EndDayId = LM.DayID
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
GROUP BY LM.DayId, DS.AdministrativeBranchNumber,OnYard_User_ID
--ORDER BY 1 DESC

--SELECT * FROM #TMP_PullOutsorLoadOutsCount WHERE DayID = 20161219 AND BranchNumber = 759 

IF OBJECT_ID('tempdb..#TMP_NumberingtheSaleCount') IS NOT NULL DROP TABLE #TMP_NumberingtheSaleCount
IF OBJECT_ID('tempdb..#TMP_SetforSale') IS NOT NULL DROP TABLE #TMP_SetforSale

SELECT DISTINCT SM.[Salvage_ID],
		ISNULL(CONVERT(INT, CONVERT(CHAR(8),SM.End_DateTime, 112)), '29991231') AS [EndDayId],
		SM.OnYard_User_ID
INTO #TMP_SetforSale
FROM  Staging.[dbo].[OnYardSetSaleMetrics] SM WITH(NOLOCK)
INNER JOIN (SELECT DISTINCT SM.[Salvage_ID],
					ISNULL(CONVERT(INT, CONVERT(CHAR(8),SM.End_DateTime, 112)), '29991231') AS [EndDayId],
					MAX(SM.End_DateTime) AS MAXEnd_DateTime
			FROM  Staging.[dbo].[OnYardSetSaleMetrics] SM WITH(NOLOCK)
			GROUP BY SM.[Salvage_ID] ,ISNULL(CONVERT(INT, CONVERT(CHAR(8),SM.End_DateTime, 112)), '29991231')) MaxDate
ON SM.Salvage_ID = MaxDate.Salvage_ID AND SM.End_DateTime = MaxDate.MAXEnd_DateTime

CREATE TABLE #TMP_NumberingtheSaleCount
 (	  
     [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[NumberingtheSaleCount] [int] NULL
	,OnYard_User_ID INT NULL  
)
INSERT INTO #TMP_NumberingtheSaleCount
(
 [DayId] ,
 [BranchNumber],
 [NumberingtheSaleCount],
  OnYard_User_ID
)
SELECT  LM.DayId
       ,DS.AdministrativeBranchNumber
	  ,Count(SS.Salvage_ID) AS NumberingtheSaleCount,
  OnYard_User_ID
FROM EDW.[dbo].DimStock DS WITH (NOLOCK)  
INNER JOIN #TMP_SetforSale SS WITH (NOLOCK) 
  ON SS.Salvage_ID =  DS.ASAPSalvageID
INNER JOIN #TMP_LaborManagement LM WITH(NOLOCK)
 ON SS.EndDayId = LM.DayID
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
GROUP BY LM.DayId, DS.AdministrativeBranchNumber,OnYard_User_ID
--ORDER BY 1 DESC

IF OBJECT_ID('tempdb..#TMP_EnhancementsCount') IS NOT NULL DROP TABLE #TMP_EnhancementsCount
CREATE TABLE #TMP_EnhancementsCount
(	 [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
	,[BandageWrapsCount] [INT] NULL
	,[DeIdLongCount] [INT] NULL
	,[DeIdShortCount] [INT] NULL
	,[FullShrinkWrapsCount] [INT] NULL
	,[PatchWrapsCount] [INT] NULL
	,OnYard_User_ID INT NULL
  
)
INSERT INTO #TMP_EnhancementsCount
(
 [DayId] ,
 [BranchNumber], 
 [BandageWrapsCount],
 [DeIdLongCount],
 [DeIdShortCount],
 [FullShrinkWrapsCount],
 [PatchWrapsCount],
  OnYard_User_ID
)
SELECT  LM.DayId
       ,DS.AdministrativeBranchNumber
	   ,SUM(CASE WHEN Enhancement_ID = '270' THEN 1 ELSE 0 END) AS BandageWrapsCount
	   ,SUM(CASE WHEN Enhancement_ID IN ('495','497') THEN 1 ELSE 0 END) AS DeIdLongCount
	   ,SUM(CASE WHEN Enhancement_ID IN ('491','493') THEN 1 ELSE 0 END) AS DeIdShortCount
	   ,SUM(CASE WHEN Enhancement_ID IN ('272') THEN 1 ELSE 0 END) AS FullShrinkWrapsCount
	   ,SUM(CASE WHEN Enhancement_ID IN ('271') THEN 1 ELSE 0 END) AS [PatchWrapsCount],
		OnYard_User_ID
FROM EDW.[dbo].DimStock DS WITH (NOLOCK)  
INNER JOIN  Staging.[dbo].[OnYardEnhancementMetrics] EM WITH (NOLOCK) 
 ON EM.Salvage_ID = DS.ASAPSalvageID
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON CONVERT(INT, CONVERT(CHAR(8),EM.End_DateTime, 112)) = LM.DayID 
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
WHERE [Enhancement_Status_Code] = 'WCP'
GROUP BY LM.DayId, DS.AdministrativeBranchNumber,  OnYard_User_ID
--ORDER BY 1 DESC

IF OBJECT_ID('tempdb..#TMP_InspectionServicesCheckinsCount') IS NOT NULL DROP TABLE #TMP_InspectionServicesCheckinsCount

CREATE TABLE #TMP_InspectionServicesCheckinsCount
 (	  
     [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[InspectionServicesCheckinsCount] [int] NULL
	--,OnYard_User_ID INT NULL
  
)
INSERT INTO #TMP_InspectionServicesCheckinsCount
(
 [DayId] ,
 [BranchNumber],
 InspectionServicesCheckinsCount
)
SELECT  LM.DayId
       ,DS.AdministrativeBranchNumber
	   ,COUNT(DS.StockId) InspectionServicesCheckinsCount
FROM EDW.[dbo].[FactStockCycleTime] FSCT WITH (NOLOCK)  
INNER JOIN EDW.[dbo].DimStock DS WITH (NOLOCK) 
 ON FSCT.StockID = DS.StockID
INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
 ON FSCT.TimeZonedInspectionServicesCompletedDayID = LM.DayID
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
GROUP BY LM.DayId, DS.AdministrativeBranchNumber
--SELECT  LM.DayId
--       ,DS.AdministrativeBranchNumber
--	   ,SUM(CASE WHEN Enhancement_ID IN ('540','542','560') THEN 1 ELSE 0 END) AS InspectionServicesCheckinsCount
--FROM EDW.[dbo].DimStock DS WITH (NOLOCK)  
--INNER JOIN Staging.[dbo].[OnYardEnhancementMetrics] E WITH (NOLOCK) 
--  ON E.Salvage_ID =  DS.ASAPSalvageID
--INNER JOIN  #TMP_LaborManagement LM WITH(NOLOCK)
-- ON CONVERT(INT, CONVERT(CHAR(8),E.End_DateTime, 112)) = LM.DayID
-- AND DS.AdministrativeBranchNumber = LM.BranchNumber
--WHERE Enhancement_ID IN ('540','542','560')
--GROUP BY LM.DayId, DS.AdministrativeBranchNumber
--ORDER BY 1 DESC

IF OBJECT_ID('tempdb..#TMP_LoaderMoveLocationsCount') IS NOT NULL DROP TABLE #TMP_LoaderMoveLocationsCount
--IF OBJECT_ID('tempdb..#TMP_MoveLocations') IS NOT NULL DROP TABLE #TMP_MoveLocations

--SELECT DISTINCT LM.[Salvage_ID],
--		ISNULL(CONVERT(INT, CONVERT(CHAR(8),LM.End_DateTime, 112)), '29991231') AS [EndDayId]
--INTO #TMP_MoveLocations
--FROM  Staging.[dbo].[OnYardLocationMetrics] LM WITH(NOLOCK)
--GROUP BY LM.[Salvage_ID] ,[End_DateTime]


CREATE TABLE #TMP_LoaderMoveLocationsCount
 (	  
     [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[LoaderMoveLocationsCount] [int] NULL
	,OnYard_User_ID  INT NULL
)
INSERT INTO #TMP_LoaderMoveLocationsCount
(
 [DayId] ,
 [BranchNumber],
 [LoaderMoveLocationsCount],
  OnYard_User_ID
)
SELECT  LM.DayId
       ,DS.AdministrativeBranchNumber
	  ,Count(ML.Salvage_ID) AS LoaderMoveLocationsCount
	  ,OnYard_User_ID
FROM EDW.[dbo].DimStock DS WITH (NOLOCK)  
INNER JOIN Staging.[dbo].[OnYardLocationMetrics] ML WITH (NOLOCK) 
  ON ML.Salvage_ID =  DS.ASAPSalvageID
INNER JOIN #TMP_LaborManagement LM WITH(NOLOCK)
 ON CONVERT(INT, CONVERT(CHAR(8), ML.End_DateTime, 112))= LM.DayID
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
GROUP BY LM.DayId, DS.AdministrativeBranchNumber,  OnYard_User_ID
--ORDER BY 1 DESC


IF OBJECT_ID('tempdb..#TMP_HoldWrapsCount') IS NOT NULL DROP TABLE #TMP_HoldWrapsCount
CREATE TABLE #TMP_HoldWrapsCount
(	 [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
	,[HoldWrapsCount] [INT] NULL
	,OnYard_User_ID INT NULL
  
)
INSERT INTO #TMP_HoldWrapsCount
(
 [DayId] ,
 [BranchNumber], 
 [HoldWrapsCount],
  OnYard_User_ID
)
SELECT LM.DayId
      ,DS.AdministrativeBranchNumber
	  , max(CASE WHEN ds.isonhold =1 and Enhancement_ID In ('270','271','272') and   CONVERT(char(10),EM.End_DateTime,126) between  CONVERT(char(10), @CurrentRunDateTime,126) and CONVERT(char(10), @CurrentRunEndDateTime,126)  then 1 else 0 END) AS HoldWrapsCount
   ,OnYard_User_ID
FROM EDW.dbo.DimStock DS WITH(NOLOCK)

INNER JOIN  Staging.[dbo].[OnYardEnhancementMetrics] EM WITH (NOLOCK) 
ON EM.Salvage_ID = DS.ASAPSalvageID
INNER JOIN #TMP_LaborManagement LM WITH(NOLOCK)
ON CONVERT(INT, CONVERT(CHAR(8), EM.End_DateTime, 112)) = LM.DayID
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
WHERE ds.IsOnHold=1 and  [Enhancement_Status_Code] = 'WCP'
GROUP BY LM.DayId, DS.AdministrativeBranchNumber, OnYard_User_ID
--ORDER BY 1 DESC


IF OBJECT_ID('tempdb..#TMP_PrepCheckinsStateFarmCount') IS NOT NULL DROP TABLE #TMP_PrepCheckinsStateFarmCount
CREATE TABLE #TMP_PrepCheckinsStateFarmCount
(	 [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
	,[PrepCheckinsStateFarmCount] [INT] NULL
)
INSERT INTO #TMP_PrepCheckinsStateFarmCount
(
 [DayId] ,
 [BranchNumber], 
 [PrepCheckinsStateFarmCount]
)
SELECT LM.DayId
      ,DS.AdministrativeBranchNumber
      ,SUM(CASE WHEN CONVERT(CHAR(10),FSCT.CheckInDateTime,126) 
	       BETWEEN  CONVERT(CHAR(10), @CurrentRunDateTime,126) 
		   AND CONVERT(CHAR(10), @CurrentRunEndDateTime,126) 
		   THEN 1 ELSE 0 END ) AS PrepCheckinsStateFarmCount
FROM EDW.dbo.DimStock DS WITH(NOLOCK)
INNER JOIN [dbo].[FactStockCycleTime] FSCT WITH(NOLOCK)
ON DS.StockId = FSCT.StockId
INNER JOIN EDW.dbo.DimProvider DP WITH(NOLOCK)
ON DS.CurrentProviderId = DP.ProviderId
INNER JOIN #TMP_LaborManagement LM WITH(NOLOCK)
ON FSCT.CheckinDayID = LM.DayID
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
WHERE DP.ProviderGroupName = 'State Farm'
GROUP BY LM.DayId, DS.AdministrativeBranchNumber
--ORDER BY 1 DESC

IF OBJECT_ID('tempdb..#TMP_ODOProbingcheckinsCount') IS NOT NULL DROP TABLE #TMP_ODOProbingcheckinsCount
IF OBJECT_ID('tempdb..#TMP_ODOCheckins') IS NOT NULL DROP TABLE #TMP_Checkins

SELECT DISTINCT CM.[Salvage_ID],
		ISNULL(CONVERT(INT, CONVERT(CHAR(8),CM.End_DateTime, 112)), '29991231') AS [EndDayId],OnYard_User_ID
INTO #TMP_ODOCheckins
FROM  Staging.[dbo].[OnYardCheckinMetrics] CM WITH(NOLOCK)
GROUP BY CM.[Salvage_ID] ,[End_DateTime],OnYard_User_ID

CREATE TABLE #TMP_ODOProbingcheckinsCount
(	 [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
	,[ODOProbingcheckinsCount] [INT] NULL
  ,OnYard_User_ID INT NULL
)
INSERT INTO #TMP_ODOProbingcheckinsCount
(
 [DayId] ,
 [BranchNumber], 
 [ODOProbingcheckinsCount],OnYard_User_ID
)
SELECT LM.DayId
      ,DS.AdministrativeBranchNumber
      ,Count(OC.Salvage_ID)  AS ODOProbingcheckinsCount,OnYard_User_ID
 FROM #TMP_ODOCheckins OC WITH(NOLOCK)
  INNER JOIN [BI_ASAP_REP].[dbo].[PAL_ECI_INPUT_DATA] PE WITH(NOLOCK)
ON OC.Salvage_ID = PE.Salvage_ID
INNER JOIN EDW.[dbo].DimStock DS WITH (NOLOCK)  
ON OC.Salvage_ID = DS.ASAPSalvageID
INNER JOIN #TMP_LaborManagement LM WITH(NOLOCK)
ON OC.EndDayId = LM.DayID 
 AND DS.AdministrativeBranchNumber = LM.BranchNumber
WHERE PE.Odo_Probed_Ind = 1
GROUP BY LM.DayId, DS.AdministrativeBranchNumber,OnYard_User_ID
--ORDER BY 1 DESC

--IF OBJECT_ID('tempdb..#TMP_ODOProbingcheckinsCount') IS NOT NULL DROP TABLE #TMP_ODOProbingcheckinsCount
--CREATE TABLE #TMP_ODOProbingcheckinsCount
--(	 [DayId] [int] NOT NULL
--    ,[BranchNumber] [int] NOT NULL
--	,[ODOProbingcheckinsCount] [INT] NULL
  
--)
--INSERT INTO #TMP_ODOProbingcheckinsCount
--(
-- [DayId] ,
-- [BranchNumber], 
-- [ODOProbingcheckinsCount]
--)
--SELECT LM.DayId
--      ,DS.AdministrativeBranchNumber
--      ,Count(DS.StockNumber) AS ODOProbingcheckinsCount
--FROM EDW.dbo.DimStock DS WITH(NOLOCK)
--INNER JOIN EDW.[dbo].[FactStockCycleTime] FSCT WITH(NOLOCK)
--ON DS.StockId = FSCT.StockId
--INNER JOIN #TMP_LaborManagement LM WITH(NOLOCK)
--ON FSCT.CheckinDayID = LM.DayID
--WHERE DS.IsOdoProbed = 1
--GROUP BY LM.DayId, DS.AdministrativeBranchNumber
--ORDER BY 1 DESC


IF OBJECT_ID('tempdb..#TMP_ReImagesCount') IS NOT NULL DROP TABLE #TMP_ReimagesCount
IF OBJECT_ID('tempdb..#TMP_Reimages') IS NOT NULL DROP TABLE #TMP_Reimages
IF OBJECT_ID('tempdb..#TMP_ReimagedStocks') IS NOT NULL DROP TABLE #TMP_ReimagedStocks

SELECT DISTINCT LM.[Salvage_ID],
		ISNULL(CONVERT(INT, CONVERT(CHAR(8),LM.End_DateTime, 112)), '29991231') AS [EndDayId]
		,ROW_NUMBER() OVER (PARTITION BY Salvage_ID,CONVERT(INT, CONVERT(CHAR(8),End_DateTime, 112)) ORDER BY Salvage_Id ) RN
		,OnYard_User_ID
INTO #TMP_Reimages
FROM  Staging.[dbo].[OnYardImagerMetrics] LM WITH(NOLOCK)
GROUP BY LM.[Salvage_ID] ,[End_DateTime],OnYard_User_ID

SELECT Salvage_ID,EndDayId,
     ROW_NUMBER() OVER (PARTITION BY Salvage_ID,RN ORDER BY Salvage_Id,EndDayId ) RANK,
  OnYard_User_ID
INTO #TMP_ReimagedStocks
FROM #TMP_Reimages WITH(NOLOCK)

CREATE TABLE #TMP_ReImagesCount
 (	  
     [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
    ,[ReImagesCount] [int] NULL
	,OnYard_User_ID INT NULL
  )
INSERT INTO #TMP_ReImagesCount
(
 [DayId] ,
 [BranchNumber],
 [ReImagesCount],
  OnYard_User_ID
)
SELECT  LM.DayId
       ,DS.AdministrativeBranchNumber
	  ,Count(RS.Salvage_ID) AS ReImagesCount
	  ,OnYard_User_ID
FROM EDW.[dbo].DimStock DS WITH (NOLOCK)  
INNER JOIN #TMP_ReimagedStocks RS WITH (NOLOCK) 
  ON RS.Salvage_ID =  DS.ASAPSalvageID
INNER JOIN EDW.dbo.DimDay LM WITH(NOLOCK)
 ON RS.EndDayId = LM.DayID
INNER JOIN #TMP_LaborManagement LMM WITH(NOLOCK)
	ON RS.EndDayId = LMM.DayID 
	AND DS.AdministrativeBranchNumber = LMM.BranchNumber
WHERE RS.RANK > 1
GROUP BY LM.DayId, DS.AdministrativeBranchNumber, OnYard_User_ID
--ORDER BY 1 DESC

IF OBJECT_ID('tempdb..#TMP_CheckinsCount') IS NOT NULL DROP TABLE #TMP_CheckinsCount
IF OBJECT_ID('tempdb..#TMP_Checkins') IS NOT NULL DROP TABLE #TMP_Checkins

SELECT DISTINCT CM.[Salvage_ID],Branch_Number,
		ISNULL(CONVERT(INT, CONVERT(CHAR(8),CM.End_DateTime, 112)), '29991231') AS [EndDayId],
		OnYard_User_ID
INTO #TMP_Checkins
FROM Staging.[dbo].[OnYardCheckinMetrics] CM WITH(NOLOCK)
--QASAPDB.[OnYard].[dbo].[Checkin_Metrics] CM WITH(NOLOCK)
GROUP BY CM.[Salvage_ID],Branch_Number ,[End_DateTime],OnYard_User_ID

CREATE TABLE #TMP_CheckinsCount
(	 [DayId] [int] NOT NULL
    ,[BranchNumber] [int] NOT NULL
	,[CheckinsCount] [INT] NULL
  ,OnYard_User_ID INT NULL
)
INSERT INTO #TMP_CheckinsCount
(
 [DayId] ,
 [BranchNumber], 
 [CheckinsCount],
  OnYard_User_ID
)
SELECT  LM.DayId
      ,Branch_Number --DS.AdministrativeBranchNumber
      ,Count(CM.Salvage_ID)  AS CheckinsCount
	  ,OnYard_User_ID
FROM EDW.[dbo].DimStock DS WITH (NOLOCK)  
INNER JOIN #TMP_Checkins CM WITH (NOLOCK) 
  ON CM.Salvage_ID =  DS.ASAPSalvageID
INNER JOIN #TMP_LaborManagement LM WITH(NOLOCK)
 ON CM.EndDayId= LM.DayID
 AND --DS.AdministrativeBranchNumber = LM.BranchNumber
CM.Branch_Number = LM.BranchNumber
GROUP BY LM.DayId,CM.Branch_Number-- DS.AdministrativeBranchNumber
,OnYard_User_ID

/*
SELECT TOP 100 * FROM #TMP_PullOutsorLoadOutsCount
SELECT TOP 100 * FROM #TMP_NumberingtheSaleCount
SELECT TOP 100 * FROM #TMP_EnhancementsCount
SELECT TOP 100 * FROM #TMP_LoaderMoveLocationsCount
SELECT TOP 100 * FROM #TMP_HoldWrapsCount
SELECT TOP 100 * FROM #TMP_ODOProbingcheckinsCount
SELECT TOP 100 * FROM #TMP_ReImagesCount
SELECT TOP 100 * FROM #TMP_CheckinsCount
*/

--SELECT * FROM #TMP_LaborManagement

UPDATE LM
SET LM.BidFastAssignmentsCount = ISNULL(AC.BidFastAssignmentsCount, 0)
   ,LM.CancellationsCount = ISNULL(AC.CancellationsCount, 0)
   ,LM.BuyerCheckinsCount = ISNULL(AD.BuyerCheckinsCount, 0)
   ,LM.[CleanTitlesCount] = ISNULL(SD.CleanTitlesCount, 0)
   ,LM.[SaleDocEntryCount] = ISNULL(SD.SaleDocEntryCount,0)
   ,LM.PrintReportHandoutsCount = ISNULL(AD.PrintReportHandoutsCount, 0)
   ,LM.[NYMV907ACount] = ISNULL(SD.NYMV907ACount, 0)
   ,LM.[SalesPrepReviewsCount] = ISNULL(SP.SalesPrepReviewsCount,0)
   ,LM.[VehiclePulloutsCount] = ISNULL(PU.VehiclePulloutsCount, 0)
   ,LM.InputBillingforEnhancementsCount = ISNULL(EN.InputBillingforEnhancementsCount,0)
   ,LM.LoginScanTowerBillsCount = ISNULL(LN.LoginScanTowerBillsCount,0)
   ,LM.ProviderPaymentReconcilationsCount = ISNULL(PP.ProviderPaymentReconcilationsCount,0)
   ,LM.DispatchingRunsCount=ISNULL(DR.DispatchingRunsCount,0)
   ,LM.DSCPaymentsCount=ISNULL(DPC.DSCPaymentsCount,0)
   ,LM.OwnerRetainsCount = ISNULL(ORC.OwnerRetainsCount,0)
   ,LM.ProblemReleaseCallsCount = ISNULL(PRC.ProblemReleaseCallsCount,0)
   ,LM.InterBranchTransfersCount = ISNULL(STC.SendingCounts,0) + ISNULL(RTC.ReceivingCounts,0)
   ,LM.VINVerificationsCount = ISNULL(VVC.VINVerificationsCount,0)
   ,LM.NewAssignmentsCount=ISNULL(NAC.NewAssignmentsCount,0)
   ,LM.[NewBuyerOrBidderRegistrationCount]=ISNULL(NBRC.[NewBuyerOrBidderRegistrationCount],0)
   ,LM.[WaitReleaseCallCount]=ISNULL(WRCC.WaitReleaseCallCount,0)
   ,LM.[FollowUponTitleProblemCount]=ISNULL(FTPC.[FollowUponTitleProblemCount],0)
   ,LM.[TitleApplicationsProcessingCount]=ISNULL(TAP.TitleApplicationsProcessingCount,0)
   ,LM.[PaymentReceipt1PayPulloutsCount] =ISNULL(PPC.PaymentReceipt1PayPulloutsCount,0)
   ,LM.[PaymentReceipt2BuyerPayCount] =ISNULL(PPC.PaymentReceipt2BuyerPayCount,0)
   ,LM.IfCallsMinbidsCount = ISNULL(IFM.IfCallsMinbidsCount,0)
   ,LM.[VisitorPaymentCount] =ISNULL(VPC.[VisitorPaymentCount],0)
   ,LM.[RegistrationRenewalOrUpdateCount] =ISNULL(RR.[RegistrationRenewalOrUpdateCount],0)
   --,LM.PullOutsorLoadOutsCount = ISNULL(PLC.PullOutsorLoadOutsCount,0) 
   --,LM.NumberingtheSaleCount=ISNULL(NSC.NumberingtheSaleCount,0)
   --,LM.BandageWrapsCount=ISNULL(EC.BandageWrapsCount,0)
   --,LM.DeIdLongCount=ISNULL(EC.DeIdLongCount,0)
   --,LM.DeIdShortCount=ISNULL(EC.DeIdShortCount,0)
   --,LM.FullShrinkWrapsCount=ISNULL(EC.FullShrinkWrapsCount,0)
   --,LM.PatchWrapsCount=ISNULL(EC.PatchWrapsCount,0)
   ,LM.InspectionServicesCheckinsCount = ISNULL(ISC.InspectionServicesCheckinsCount,0)
   --,LM.LoaderMoveLocationsCount=ISNULL(LMC.LoaderMoveLocationsCount,0)
   --,LM.HoldWrapsCount=ISNULL(HWC.HoldWrapsCount,0)
   ,LM.PrepCheckinsStateFarmCount=ISNULL(PSC.PrepCheckinsStateFarmCount,0)
   --,LM.ODOProbingcheckinsCount=ISNULL(OPC.ODOProbingcheckinsCount,0)
   --,LM.ReImagesCount=ISNULL(RIC.ReImagesCount,0)
   --,LM.CheckinsCount=ISNULL(CC.CheckinsCount,0)

FROM #TMP_LaborManagement LM WITH(NOLOCK)
LEFT OUTER JOIN #TMP_AssignedCancelled AC WITH(NOLOCK)
 ON LM.DayId = AC.DayId
 AND LM.BranchNumber = AC.BranchNumber  
LEFT OUTER JOIN #TMP_Attendance AD WITH(NOLOCK)
 ON LM.DayId = AD.DayId
 AND LM.BranchNumber = AD.BranchNumber 
LEFT OUTER JOIN #TMP_SaleDoc SD WITH(NOLOCK)
  ON LM.DayId = SD.DayId
 AND LM.BranchNumber = SD.BranchNumber 
LEFT OUTER JOIN #TMP_SalesPrepReviews SP WITH(NOLOCK)
  ON LM.DayId = SP.DayId
  AND LM.BranchNumber = SP.BranchNumber
LEFT OUTER JOIN #TMP_Pullouts PU WITH(NOLOCK)
 ON LM.dayID = PU.DayId
 AND LM.BranchNumber = PU.BranchNumber
LEFT OUTER JOIN #TMP_Enhancements EN WITH(NOLOCK)
  ON LM.dayID = EN.DayId
 AND LM.BranchNumber = EN.BranchNumber
LEFT OUTER JOIN  #TMP_Logins LN WITH(NOLOCK)
  ON LM.dayID = LN.DayId
AND LM.BranchNumber = LN.BranchNumber
LEFT OUTER JOIN #TMP_ProviderPaymentReconcilations PP WITH(NOLOCK)
 ON LM.dayID = PP.DayId
AND LM.BranchNumber = PP.BranchNumber
LEFT OUTER JOIN  #TMP_DispatchingRuns  DR WITH(NOLOCK)
ON LM.DayId =DR.dayID
 AND LM.BranchNumber = DR.BranchNumber
LEFT OUTER JOIN #TMP_DSCPaymentsCount DPC WITH(NOLOCK)
on LM.DayId =DPC.dayID
AND LM.BranchNumber = DPC.BranchNumber
LEFT OUTER JOIN #TMP_OwnerRetainsCount ORC WITH(NOLOCK)
ON LM.DayId = ORC.DayID
AND LM.BranchNumber = ORC.BranchNumber
LEFT OUTER JOIN #TMP_ProblemReleaseCallsCount PRC WITH(NOLOCK)
ON LM.DayId = PRC.DayID
AND LM.BranchNumber = PRC.BranchNumber
LEFT OUTER JOIN #TMP_SendingCounts STC WITH(NOLOCK)
ON LM.DayId = STC.DayID
AND LM.BranchNumber = STC.BranchNumber
LEFT OUTER JOIN #TMP_ReceivingCounts RTC WITH(NOLOCK)
ON LM.DayId = RTC.DayID
AND LM.BranchNumber = RTC.BranchNumber
LEFT OUTER JOIN #TMP_VINVerificationsCount VVC WITH(NOLOCK)
ON LM.DayId = VVC.DayID
AND LM.BranchNumber = VVC.BranchNumber 
LEFT OUTER JOIN #TMP_NewAssignmentsCount NAC  WITH(NOLOCK)
ON LM.DayId = NAC.DayID
AND LM.BranchNumber = NAC.BranchNumber 
LEFT OUTER JOIN #TMP_NewBidderRegistrationCount NBRC WITH(NOLOCK)
ON LM.DayId = NBRC.DayID
AND LM.BranchNumber = NBRC.BranchNumber 
LEFT OUTER JOIN  #TMP_WaitReleaseCallCount WRCC WITH(NOLOCK)
ON LM.DayId = WRCC.DayID
AND LM.BranchNumber = WRCC.BranchNumber 
LEFT OUTER JOIN #TMP_FollowUponTitleProblemCount FTPC WITH(NOLOCK)
ON LM.DayId = FTPC.DayID
AND LM.BranchNumber = FTPC.BranchNumber 
LEFT OUTER JOIN #TMP_TitleApplicationsProcessingCount TAP WITH(NOLOCK)
ON LM.DayId = TAP.DayId
AND LM.BranchNumber = TAP.BranchNumber 
LEFT OUTER JOIN #TMP_PaymentReceiptsCount PPC WITH(NOLOCK)
ON LM.DayId = PPC.DayId
AND LM.BranchNumber = PPC.BranchNumber 
LEFT OUTER JOIN #TMP_IfCallsMinbidsCount IFM WITH(NOLOCK)
ON LM.DayId = IFM.DayID
AND LM.BranchNumber = IFM.BranchNumber
LEFT OUTER JOIN #TMP_VisitorPaymentCount VPC  WITH(NOLOCK)
ON LM.DayId = VPC.DayId
AND LM.BranchNumber = VPC.BranchNumber 
LEFT OUTER JOIN #TMP_RegistrationRenewal RR WITH(NOLOCK)
ON LM.DayId = RR.DayId
AND LM.BranchNumber = RR.BranchNumber 
--LEFT OUTER JOIN #TMP_PullOutsorLoadOutsCount PLC WITH(NOLOCK)
--ON LM.DayId = PLC.DayId
--AND LM.BranchNumber = PLC.BranchNumber 
--LEFT OUTER JOIN #TMP_NumberingtheSaleCount NSC WITH(NOLOCK)
--ON LM.DayId = NSC.DayId
--AND LM.BranchNumber = NSC.BranchNumber
--LEFT OUTER JOIN #TMP_EnhancementsCount EC WITH(NOLOCK)
--ON LM.DayId = EC.DayId
--AND LM.BranchNumber = EC.BranchNumber
LEFT OUTER JOIN #TMP_InspectionServicesCheckinsCount ISC WITH(NOLOCK)
ON LM.DayId = ISC.DayId
AND LM.BranchNumber = ISC.BranchNumber
--LEFT OUTER JOIN #TMP_LoaderMoveLocationsCount LMC WITH(NOLOCK)
--ON LM.DayId = LMC.DayId
--AND LM.BranchNumber = LMC.BranchNumber
--LEFT OUTER JOIN #TMP_HoldWrapsCount HWC WITH(NOLOCK)
--ON LM.DayId = HWC.DayId
--AND LM.BranchNumber = HWC.BranchNumber
LEFT OUTER JOIN #TMP_PrepCheckinsStateFarmCount PSC WITH(NOLOCK)
ON LM.DayId = PSC.DayId
AND LM.BranchNumber = PSC.BranchNumber
--LEFT OUTER JOIN #TMP_ODOProbingcheckinsCount OPC WITH(NOLOCK)
--ON LM.DayId = OPC.DayId
--AND LM.BranchNumber = OPC.BranchNumber
--LEFT OUTER JOIN #TMP_ReImagesCount RIC WITH(NOLOCK)
--ON LM.DayId = RIC.DayId
--AND LM.BranchNumber = RIC.BranchNumber
--LEFT OUTER JOIN #TMP_CheckinsCount CC WITH(NOLOCK)
--ON LM.DayId = CC.DayId
--AND LM.BranchNumber = CC.BranchNumber
WHERE OnYardUserID =99999

INSERT INTO #TMP_LaborManagement(DayID,BranchNumber,OnYardUserID )
SELECT DISTINCT [DayId] , [BranchNumber], ISNULL(OnYard_User_ID, 99999) FROM #TMP_PullOutsorLoadOutsCount WHERE PullOutsorLoadOutsCount > 0
UNION
SELECT DISTINCT [DayId] , [BranchNumber],ISNULL(OnYard_User_ID, 99999) FROM #TMP_NumberingtheSaleCount WHERE NumberingtheSaleCount > 0
UNION
SELECT DISTINCT [DayId] , [BranchNumber],ISNULL(OnYard_User_ID,99999) FROM #TMP_EnhancementsCount WHERE  [BandageWrapsCount] > 0 OR [DeIdLongCount] > 0 OR [DeIdShortCount]  > 0 OR [FullShrinkWrapsCount]  > 0 OR [PatchWrapsCount] > 0
UNION
SELECT DISTINCT [DayId] , [BranchNumber],ISNULL(OnYard_User_ID,99999) FROM #TMP_LoaderMoveLocationsCount WHERE LoaderMoveLocationsCount > 0
UNION
SELECT DISTINCT [DayId] , [BranchNumber],ISNULL(OnYard_User_ID,99999) FROM #TMP_HoldWrapsCount WHERE HoldWrapsCount > 0
UNION
SELECT DISTINCT [DayId] , [BranchNumber],ISNULL(OnYard_User_ID,99999) FROM #TMP_ODOProbingcheckinsCount WHERE ODOProbingcheckinsCount > 0
UNION
SELECT DISTINCT [DayId] , [BranchNumber],ISNULL(OnYard_User_ID,99999) FROM #TMP_ReImagesCount WHERE ReImagesCount > 0
UNION
SELECT DISTINCT [DayId] , [BranchNumber],ISNULL(OnYard_User_ID,99999) FROM #TMP_CheckinsCount WHERE CheckinsCount > 0

UPDATE LM
SET LM.PullOutsorLoadOutsCount = ISNULL(PLC.PullOutsorLoadOutsCount,0) 
   ,LM.NumberingtheSaleCount=ISNULL(NSC.NumberingtheSaleCount,0)
   ,LM.BandageWrapsCount=ISNULL(EC.BandageWrapsCount,0)
   ,LM.DeIdLongCount=ISNULL(EC.DeIdLongCount,0)
   ,LM.DeIdShortCount=ISNULL(EC.DeIdShortCount,0)
   ,LM.FullShrinkWrapsCount=ISNULL(EC.FullShrinkWrapsCount,0)
   ,LM.PatchWrapsCount=ISNULL(EC.PatchWrapsCount,0)
   ,LM.LoaderMoveLocationsCount=ISNULL(LMC.LoaderMoveLocationsCount,0)
   ,LM.HoldWrapsCount=ISNULL(HWC.HoldWrapsCount,0)
   ,LM.ODOProbingcheckinsCount=ISNULL(OPC.ODOProbingcheckinsCount,0)
   ,LM.ReImagesCount=ISNULL(RIC.ReImagesCount,0)
   ,LM.CheckinsCount=ISNULL(CC.CheckinsCount,0)
--SELECT *
FROM #TMP_LaborManagement LM WITH(NOLOCK)
LEFT OUTER JOIN #TMP_PullOutsorLoadOutsCount PLC WITH(NOLOCK)
ON LM.DayId = PLC.DayId
AND LM.BranchNumber = PLC.BranchNumber 
AND LM.OnYardUserID = ISNULL(PLC.OnYard_User_ID,99999)
LEFT OUTER JOIN #TMP_NumberingtheSaleCount NSC WITH(NOLOCK)
ON LM.DayId = NSC.DayId
AND LM.BranchNumber = NSC.BranchNumber 
AND LM.OnYardUserID = ISNULL(NSC.OnYard_User_ID,99999)
LEFT OUTER JOIN #TMP_EnhancementsCount EC WITH(NOLOCK)
ON LM.DayId = EC.DayId
AND LM.BranchNumber = EC.BranchNumber 
AND LM.OnYardUserID = ISNULL(EC.OnYard_User_ID,99999)
LEFT OUTER JOIN #TMP_LoaderMoveLocationsCount LMC WITH(NOLOCK)
ON LM.DayId = LMC.DayId
AND LM.BranchNumber = LMC.BranchNumber 
AND LM.OnYardUserID = ISNULL(LMC.OnYard_User_ID,99999)
LEFT OUTER JOIN #TMP_HoldWrapsCount HWC WITH(NOLOCK)
ON LM.DayId = HWC.DayId
AND LM.BranchNumber = HWC.BranchNumber 
AND LM.OnYardUserID = ISNULL(HWC.OnYard_User_ID,99999)
LEFT OUTER JOIN #TMP_ODOProbingcheckinsCount OPC WITH(NOLOCK)
ON LM.DayId = OPC.DayId
AND LM.BranchNumber = OPC.BranchNumber 
AND LM.OnYardUserID = ISNULL(OPC.OnYard_User_ID,99999)
LEFT OUTER JOIN #TMP_ReImagesCount RIC WITH(NOLOCK)
ON LM.DayId = RIC.DayId
AND LM.BranchNumber = RIC.BranchNumber 
AND LM.OnYardUserID = ISNULL(RIC.OnYard_User_ID,99999)
LEFT OUTER JOIN #TMP_CheckinsCount CC WITH(NOLOCK)
ON LM.DayId = CC.DayId
AND LM.BranchNumber = CC.BranchNumber 
AND LM.OnYardUserID = ISNULL(CC.OnYard_User_ID,99999)
--WHERE LM.OnYardUserID IS NOT NULL


 --SELECT * FROM #TMP_LaborManagement
 --BEGIN TRANSACTION 
 --DELETE FROM EDW.dbo.LaborManagement where DayID > 20161213

 
UPDATE TLM SET KAREmployeeID = ISNULL(EmplID,99999)
FROM #TMP_LaborManagement TLM
  LEFT OUTER JOIN QASAPDB.[OnYard].[dbo].[OnYard_User] OYU WITH (NOLOCK)
  ON TLM.OnYardUserID = OYU.OnYard_User_ID
  LEFT OUTER JOIN EDW.DBO.KAR_Employee_IDs KEIDS WITH (NOLOCK)
  ON OYU.[Login_Name] = KEIDS.User4Character
WHERE TLM.OnYardUserID IS NOT NULL

	--SELECT TLM.*
	--FROM EDW.dbo.LaborManagement AS LM
 --   INNER JOIN #TMP_LaborManagement AS TLM
 --         ON LM.DayID = TLM.DayID
 --         AND LM.BranchNumber = TLM.BranchNumber
	--	AND ISNULL(LM.KAREmployeeID,0) = ISNULL(TLM.KAREmployeeID,0)

--SELECT DayID,
--          BranchNumber,
--		KAREmployeeID,COUNT(*) FROM #TMP_LaborManagement
--group by DayID,
--          BranchNumber,
--		KAREmployeeID
--HAVING COUNT(*) > 1


--SELECT * FROM #TMP_LaborManagement
--WHERE  DayID= 20161213 
--AND BranchNumber =332 
--AND ISNULL(KAREmployeeID,99999) = 99999


	MERGE EDW.dbo.LaborManagement AS LM
    USING #TMP_LaborManagement AS TLM
          ON LM.DayID = TLM.DayID
          AND LM.BranchNumber = TLM.BranchNumber
		AND ISNULL(LM.OnYardUserID,99999) = ISNULL(TLM.OnYardUserID,99999)
    WHEN MATCHED THEN 
        UPDATE SET 
         DayId = TLM.DayId
		,BidFastAssignmentsCount = TLM.BidFastAssignmentsCount
		,CancellationsCount = TLM.CancellationsCount
		,BuyerCheckinsCount= TLM.BuyerCheckinsCount
		,CleanTitlesCount = TLM.CleanTitlesCount
		,PrintReportHandoutsCount = TLM.PrintReportHandoutsCount
		,SaleDocEntryCount = TLM.SaleDocEntryCount
		,NYMV907ACount = TLM.NYMV907ACount
		,SalesPrepReviewsCount = TLM.SalesPrepReviewsCount
		,VehiclePulloutsCount = TLM.VehiclePulloutsCount
		,InputBillingforEnhancementsCount = TLM.InputBillingforEnhancementsCount
		,LoginScanTowerBillsCount = TLM.LoginScanTowerBillsCount
		,ProviderPaymentReconcilationsCount = TLM.ProviderPaymentReconcilationsCount
		,DispatchingRunsCount=TLM.DispatchingRunsCount
        ,DSCPaymentsCount=TLM.DSCPaymentsCount
		,OwnerRetainsCount = TLM.OwnerRetainsCount
		,ProblemReleaseCallsCount = TLM.ProblemReleaseCallsCount
		,InterBranchTransfersCount = TLM.InterBranchTransfersCount
		,VINVerificationsCount = TLM.VINVerificationsCount
		,NewAssignmentsCount=TLM.NewAssignmentsCount
		,[NewBuyerOrBidderRegistrationCount]=TLM.[NewBuyerOrBidderRegistrationCount]
		,[WaitReleaseCallCount]=TLM.[WaitReleaseCallCount]
		,[FollowUponTitleProblemCount]=TLM.[FollowUponTitleProblemCount]
		,TitleApplicationsProcessingCount=TLM.TitleApplicationsProcessingCount
        ,PaymentReceipt1PayPulloutsCount=TLM.PaymentReceipt1PayPulloutsCount
		,PaymentReceipt2BuyerPayCount=TLM.PaymentReceipt2BuyerPayCount
		,IfCallsMinbidsCount = TLM.IfCallsMinbidsCount
		,[VisitorPaymentCount] =TLM.[VisitorPaymentCount]
        ,[RegistrationRenewalOrUpdateCount] =TLM.[RegistrationRenewalOrUpdateCount]
		,PullOutsorLoadOutsCount = TLM.PullOutsorLoadOutsCount
		,NumberingtheSaleCount = TLM.NumberingtheSaleCount
		,BandageWrapsCount = TLM.BandageWrapsCount
        ,DeIdLongCount = TLM.DeIdLongCount
        ,DeIdShortCount = TLM.DeIdShortCount
        ,FullShrinkWrapsCount = TLM.FullShrinkWrapsCount
        ,PatchWrapsCount = TLM.PatchWrapsCount
		,InspectionServicesCheckinsCount = TLM.InspectionServicesCheckinsCount
		,LoaderMoveLocationsCount = TLM.LoaderMoveLocationsCount
		,HoldWrapsCount = TLM.HoldWrapsCount
		,PrepCheckinsStateFarmCount = TLM.PrepCheckinsStateFarmCount
		,ODOProbingcheckinsCount = TLM.ODOProbingcheckinsCount
        ,ReImagesCount = TLM.ReImagesCount
		,CheckinsCount = TLM.CheckinsCount
		,KAREmployeeID = ISNULL(TLM.KAREmployeeID,99999)
		,OnYardUserID = TLM.OnYardUserID
	WHEN NOT MATCHED THEN
    INSERT( 
		BranchNumber
		,DayId
		,BidFastAssignmentsCount
		,CancellationsCount
		,BuyerCheckinsCount
		,CleanTitlesCount
		,PrintReportHandoutsCount
		,SaleDocEntryCount
		,NYMV907ACount
		,SalesPrepReviewsCount
		,VehiclePulloutsCount
		,InputBillingforEnhancementsCount
		,LoginScanTowerBillsCount
		,ProviderPaymentReconcilationsCount 
		,DispatchingRunsCount
        ,DSCPaymentsCount
		,OwnerRetainsCount
		,ProblemReleaseCallsCount
		,InterBranchTransfersCount
		,VINVerificationsCount
		,NewAssignmentsCount
		,[NewBuyerOrBidderRegistrationCount]
		,[WaitReleaseCallCount]
		,[FollowUponTitleProblemCount]
		,TitleApplicationsProcessingCount
		,PaymentReceipt1PayPulloutsCount
		,PaymentReceipt2BuyerPayCount
		,IfCallsMinbidsCount
		,[VisitorPaymentCount]
        ,[RegistrationRenewalOrUpdateCount]
	    ,PullOutsorLoadOutsCount
		,NumberingtheSaleCount
		,BandageWrapsCount
		,DeIdLongCount
		,DeIdShortCount
		,FullShrinkWrapsCount
		,PatchWrapsCount
		,InspectionServicesCheckinsCount
		,LoaderMoveLocationsCount
		,HoldWrapsCount
		,PrepCheckinsStateFarmCount
		,ODOProbingcheckinsCount
		,ReImagesCount
		,CheckinsCount
		,KAREmployeeID
		,OnYardUserID
		)
    VALUES (TLM.BranchNumber
         ,TLM.DayId
    	 ,TLM.BidFastAssignmentsCount
		 ,TLM.CancellationsCount
		 ,TLM.BuyerCheckinsCount
		 ,TLM.CleanTitlesCount
		 ,TLM.PrintReportHandoutsCount
		 ,TLM.SaleDocEntryCount
		 ,TLM.NYMV907ACount
		 ,TLM.SalesPrepReviewsCount
		 ,TLM.VehiclePulloutsCount
		 ,TLM.InputBillingforEnhancementsCount
		 ,TLM.LoginScanTowerBillsCount
		 ,TLM.ProviderPaymentReconcilationsCount
		 ,TLM.DispatchingRunsCount
        , TLM.DSCPaymentsCount
		 ,TLM.OwnerRetainsCount
		 ,TLM.ProblemReleaseCallsCount
		 ,TLM.InterBranchTransfersCount
		 ,TLM.VINVerificationsCount
		 ,TLM.NewAssignmentsCount
		 ,TLM.[NewBuyerOrBidderRegistrationCount]
		 ,TLM.[WaitReleaseCallCount]
		 ,TLM.[FollowUponTitleProblemCount]
		 ,TLM.TitleApplicationsProcessingCount
         ,TLM.PaymentReceipt1PayPulloutsCount
         ,TLM.PaymentReceipt2BuyerPayCount
		 ,TLM.IfCallsMinbidsCount
		 ,TLM.[VisitorPaymentCount]
         ,TLM.[RegistrationRenewalOrUpdateCount]
		 ,TLM.PullOutsorLoadOutsCount
		 ,TLM.NumberingtheSaleCount
		 ,TLM.BandageWrapsCount
		 ,TLM.DeIdLongCount
		 ,TLM.DeIdShortCount
		 ,TLM.FullShrinkWrapsCount
		 ,TLM.PatchWrapsCount
		 ,TLM.InspectionServicesCheckinsCount
		 ,TLM.LoaderMoveLocationsCount
		 ,TLM.HoldWrapsCount
		 ,TLM.PrepCheckinsStateFarmCount
		 ,TLM.ODOProbingcheckinsCount
		 ,TLM.ReImagesCount
		 ,TLM.CheckinsCount
		 ,ISNULL(KAREmployeeID,99999)
		 ,TLM.OnYardUserID
		 )
		 
		 OUTPUT $action INTO #TMP_RecordCount;
		 		 
/*********************************************************************************************
Update Objects for Reporting
*********************************************************************************************/

IF OBJECT_ID('tempdb..#TMP_Days') IS NOT NULL DROP TABLE #TMP_Days
CREATE TABLE #TMP_Days
(	[DayID] [int] NOT NULL,
	[DayDate] [datetime] NOT NULL,
	[FiscalWeekID] [int] NOT NULL,
	[LastDayOfWeekDayID] [int] NULL
)
INSERT INTO #TMP_Days
(	DayID,
	DayDate,
	FiscalWeekID,
	LastDayOfWeekDayID
)
SELECT DayId,
       DayDate,
       FiscalWeekID,
       @CurrentWeekLastDayofWeekDayID
FROM EDW.dbo.DimDay WITH (NOLOCK)
WHERE [DayID] IN (SELECT DayID FROM dbo.DimDay Where FiscalWeekID = @CurrentRunWeekID)

----Update FiscalWeekId

UPDATE TL
SET  TL.FiscalWeekID = TD. FiscalWeekID
FROM EDW.dbo.LaborManagement TL  WITH(NOLOCK)
INNER JOIN #TMP_Days TD
ON TL.DayId = TD.DayID 

--SELECT * FROM #TMP_Days

----Update IsCurrentWeek

UPDATE TL
SET TL.IsCurrentWeek = 0
FROM EDW.dbo.LaborManagement TL WITH(NOLOCK)
WHERE IsCurrentWeek = 1


IF @CurrentRunDayID <> @CurrentWeekLastDayOfWeekDayID
BEGIN
	UPDATE TL
	SET IsCurrentWeek = 1
	FROM EDW.dbo.LaborManagement TL WITH(NOLOCK)
	INNER JOIN #TMP_Days TD WITH(NOLOCK)
	ON TL.FiscalWeekID = TD.FiscalWeekID
END

----Update IsRolling13Week

IF OBJECT_ID('tempdb..#Rolling3Weeks') IS NOT NULL DROP TABLE #Rolling3Weeks

SELECT DISTINCT TOP 3 FiscalWeekID, 
	   IDENTITY(smallint, 1, 1) AS IsRolling3Week
INTO #Rolling3Weeks	   
FROM EDW.dbo.LaborManagement  WITH(NOLOCK)
WHERE IsCurrentWeek <> 1
ORDER BY FiscalWeekID desc

--SELECT * FROM #Rolling3Weeks	

UPDATE TL
SET TL.IsRolling3Week = 0
FROM EDW.dbo.LaborManagement TL WITH(NOLOCK)


UPDATE TL
SET TL.IsRolling3Week = RL.IsRolling3Week
FROM EDW.dbo.LaborManagement TL  WITH(NOLOCK)
INNER JOIN #Rolling3Weeks RL
ON TL.FiscalWeekID = RL.FiscalWeekID

END
