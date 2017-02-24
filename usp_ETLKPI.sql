/* 
USE EDW
GO 
==============================================================================
Author		: Juan X. Jacome
--Create date	: 08/17/2012
Description	: Populate the EDW KPRCorporate and KPIBranch table 	
==============================================================================
REVISION History
ChangeDate	Developer			Release/Problem Number	

9/17/2012	Juan X. Jacome		--Update logic for @CurrentWeekLastDayofWeekDayID and IsRolling13Week 
10/15/2012	Juan X. Jacome		-- Add objects for Operational KPI 
10/30/2012	Juan X. Jacome		-- Update logic for KPI Corporate to include adjusting company for Insurance objects    
10/31/2012	Juan X. Jacome		-- Update logic for  #TMP_VolumeProceeds    
11/9/2012	Juan X. Jacome		--Update logic for #TMP_ReleaseToPickup
11/19/2012	Juan X. Jacome		-- Updated logic for #TMP_ReleaseProblem
12/10/2012	Juan X. Jacome		-- Added loigc for CurrentYearFiscalWeekMinAverage, PriorYearFiscalWeekMinAverage
1/3/2013	Juan X. Jacome		-- Added Operational kpi fields
1/7/2013	Juan X. Jacome		-- Update logic for Volume/Proceeds by Customer Segment
1/25/2013	Juan X. Jacome		-- Update logic for #TMP_MinimumSalesPrice	
2/4/2013	Juan X. Jacome		-- Add logic for NonCAT objects 
2/18/2013	Juan X. Jacome		-- Update logic for ElectronicAssignmentsNonCAT and UnitsSoldRunAndDriveAutoOnlyNonCAT
4/2/2013	Juan X. Jacome		-- Fixed issue for NonCAT_VRD and NonCAT_Rental objects
4/3/2013	Juan X. Jacome		-- Update logic for #TMP_BidTypeSold
7/16/2013   Sidd Murao         -- Update logic for Buyer Revenue
8/27/2013   Sidd Murao         -- Update logic for some Non CAT objects
9/5/2013	Juan X. Jacome	   -- Fixed issue with Reruns
9/11/2013   Juan X. Jacome	   -- Fixed issue with PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApprovalNonCAT, PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApproval
9/17/2013	Juan X. Jacome	   -- Fixed issue with UnitsSoldsWithKeyOnly, VRDUnitsSoldsWithKeyOnlyNonCAT
9/25/2013	Juan X. Jacome	   -- Added logice to recalcualte WeekEndPickups for the last 3 weeks 
1/27/2014	Juan X. Jacome	   -- Update logic for InsuranceACVNonCAT, InsuranceACV for #TMP_VolumeProceeds
1/29/2014	Juan X. Jacome	   -- Add logic for UnitsSoldLast13WeeksNonCAT, UnitsSoldLast13Weeks, PipelineInventory14WeeksAgoNonCAT and PipelineInventory14WeeksAgo
3/30/2014	Juan X. Jacome	   -- Update logice Provider Segment
4/25/2016  Yogitha Alwarsetty  -- Added PipelineInventory13WeeksAgoNonCAT,PipelineInventory13WeeksAgo
08/03//2016 Mohammad Waheed    -- Changed the scope of RunandDrive columns to look back past 4 weeks and update the KPIBranch Separately  
=============================================================================
Usage Example:
Exec usp_ETLKPI
==============================================================================
*/
Create  PROCEDURE [dbo].[usp_ETLKPI] WITH RECOMPILE
AS     
BEGIN

DECLARE @ETLLoadID    INT,
		@ExtractRowCount_sp INT,
		@InsertRowCount INT,
		@UpdateRowCount INT,
		@DeleteRowCount INT,
		@CurrentRunDateTime DateTime,
		@CurrentRunDayID INT,
		@CurrentRunWeekID INT,
		@CurrentWeekLastDayOfWeekDayID INT,
		@CurrentYearFiscalWeekMinAverage money,
	    @PriorYearFiscalWeekMinAverage money,
	    @CurrentYearFiscalWeekMinAverageNonCAT money,
	    @PriorYearFiscalWeekMinAverageNonCAT money,
		@3weeksbackDatetime datetime,
		@3weeksbackDayID int,
		@3weeksbackWeekID int,
		@Last4thRunWeekID int			
SET NOCOUNT ON;

BEGIN TRY

--Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S', @PackageName = 'usp_ETLKPI'


--Populate @CurrentRunDateTime
SELECT @CurrentRunDateTime = 
	(SELECT 
		CASE WHEN datepart(hh,MaxDatetime) between 0 and 14 then DateAdd(HOUR,23,CONVERT(DATETIME,CONVERT(varchar(8), MaxDatetime - 1,112),114))
		ELSE MaxDatetime
		END 
	 FROM dbo.ETLRUN with (Nolock)
	 WHERE ETLJobName = 'DailyETLJob'
	 )
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
select @Last4thRunWeekID=year(dateadd(ww,-4, @CurrentRunDateTime))*100+datepart(ww,dateadd(ww,-4, @CurrentRunDateTime))

/*********************************************************************************************
Create the temp table KPI
*********************************************************************************************/
IF OBJECT_ID('tempdb..#TMP_KPI_Branch') IS NOT NULL DROP TABLE #TMP_KPI_Branch
CREATE TABLE #TMP_KPI_Branch
(	[BranchNumber] [int] NOT NULL,
	[Lastdayofweekid] [int] NOT NULL,
	[FiscalWeekID] [int] NOT NULL,
	[UnitsAssigned] [int] NULL,
	[UnitsNetAssigned] [int] NULL,
	[UnitsAssignedPlan] [int] NULL,
	[ElectronicAssignments] [int] NULL,
	[CancelledAssignments] [int] NULL,
	[NumberofPickups] [int] NULL,
	[CalendarDaysReleasetoPickup] [int] NULL,
	[NumberofPickups100MileswithinoneBusinessDays] [int] NULL,
	[NumberofPickupsSameDayReleasedByNoonWithin35Miles] [int] NULL,
	[NumberofReleaseProblems] [int] NULL,
	[PipelineInventory] [int] NULL,
	[UnitSold] [int] NULL,
	[UnitSoldPlan] [int] NULL,
	[UnitsSetForSale] [int] NULL,
	[ProviderRevenue] [money] NULL,
	[ProviderNetRevenue] [money] NULL,
	[PARevenue] [money] NULL,
	[TowCost] [money] NULL,
	[BuyerRevenue] [money] NULL,
	[MinimumSalesPrice] [money] NULL,
	[TotalSalePrice] [money] NULL,
	[InsuranceUnitsSold] [int] NULL,
	[InsuranceSalePrice] [money] NULL,
	[InsuranceACV] [money] NULL,
	[CharityUnitsSold] [int] NULL,
	[CharityTotalSalePrice] [money] NULL,
	[VRDUnitsSold] [int] NULL,
	[VRDTotalSalePrice] [int] NULL,
	[OtherUnitsSold] [int] NULL,
	[OtherTotalSalePrice] [money] NULL,
	[IBIDSoldUnits] [int] NULL,
	[ProxySoldUnits] [int] NULL,
	[IBFSoldUnits] [int] NULL,
	[BuyerAttendance] [int] NULL,
	[SuccessfulBuyer] [int] NULL,
	[OutofCountryBuyerAttendance] [int] NULL,
	[OutofCountrySoldUnits] [int] NULL,
	[OutofStateSoldUnits] [int] NULL,
	[GuestAttendance] [int] NULL,
	[InsuranceGrossReturn] [money] NULL,
	[RentalUnitsSold] [int] NULL,
	[RentalTotalSalePrice] [money] NULL,
	[PAUnitSold] [money] NULL,
	[CountofPUwithIn100MilesOneBusinessDays] [int] NULL,
	[ACV] [money] NULL,
	[UnitsSoldsWithKey] [int] NULL,
	[UnitsSoldRunAndDriveAutoOnly] [int] NULL,
	[UnitsSoldCarStart] [int] NULL,
	[UnitsSoldMileage] [int] NULL,
	[UnitsSoldEnhancement] [int] NULL,
	[UnitsSoldReRun] [int] NULL,
	[VRDUnitsSoldKey] [int] NULL,
	[VRDUnitsSoldRunAndDriveAutoOnly] [int] NULL,
	[VRDUnitsSoldCarStart] [int] NULL,
	[VRDUnitsSoldMileage] [int] NULL,
	[VRDUnitsSoldEnhancement] [int] NULL,
	[VRDReRun] [int] NULL,
	[IBFSalePrice] [money] NULL,
	[IBFNextHighestHistoricalBid] [money] NULL,
	[UnitsSoldPublic] [int] NULL,
	[UnitsSoldPushedByPublic] [int] NULL,
	[NumberOfWeekendPickups] [int]  NULL,
	[CalendarDaysAssignmentToRelease] [int]  NULL,
	[UnitsWrappedWithin7DaysOfCheckIn] [int]  NULL,
	[UsableAcreage] [int]  NULL,
	[PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApproval] [int]  NULL,
	[ElectronicAssignmentsNonCAT] [int] NULL,
	[CancelledAssignmentsNonCAT] [int] NULL,
	[NumberofPickupsNonCAT] [int] NULL,
	[CalendarDaysReleasetoPickupNonCAT] [int] NULL,
	[NumberofPickups100MileswithinoneBusinessDaysNonCAT] [int] NULL,
	[NumberofPickupsSameDayReleasedByNoonWithin35MilesNonCAT] [int] NULL,
	[NumberofReleaseProblemsNonCAT] [int] NULL,
	[PipelineInventoryNonCAT] [int] NULL,
	[UnitSoldNonCAT] [int] NULL,
	[UnitsSetForSaleNonCAT] [int] NULL,
	[ProviderRevenueNonCAT] [money] NULL,
	[ProviderNetRevenueNonCAT] [money] NULL,
	[PARevenueNonCAT] [money] NULL,
	[TowCostNonCAT] [money] NULL,
	[BuyerRevenueNonCAT] [money] NULL,
	[MinimumSalesPriceNonCAT] [money] NULL,
	[TotalSalePriceNonCAT] [money] NULL,
	[InsuranceUnitsSoldNonCAT] [int] NULL,
	[InsuranceSalePriceNonCAT] [money] NULL,
	[InsuranceACVNonCAT] [money] NULL,
	[CharityUnitsSoldNonCAT] [int] NULL,
	[CharityTotalSalePriceNonCAT] [money] NULL,
	[VRDUnitsSoldNonCAT] [int] NULL,
	[VRDTotalSalePriceNonCAT] [money] NULL,
	[OtherUnitsSoldNonCAT] [int] NULL,
	[OtherTotalSalePriceNonCAT] [money] NULL,
	[IBIDSoldUnitsNonCAT] [int] NULL,
	[ProxySoldUnitsNonCAT] [int] NULL,
	[IBFSoldUnitsNonCAT] [int] NULL,
	[BuyerAttendanceNonCAT] [int] NULL,
	[SuccessfulBuyerNonCAT] [int] NULL,
	[OutofCountryBuyerAttendanceNonCAT] [int] NULL,
	[OutofCountrySoldUnitsNonCAT] [int] NULL,
	[OutofStateSoldUnitsNonCAT] [int] NULL,
	[GuestAttendanceNonCAT] [int] NULL,
	[InsuranceGrossReturnNonCAT] [money] NULL,
	[RentalUnitsSoldNonCAT] [int] NULL,
	[RentalTotalSalePriceNonCAT] [money] NULL,
	[PAUnitSoldNonCAT] [money] NULL,
	[ACVNonCAT] [money] NULL,
	[UnitsSoldsWithKeyNonCAT] [int] NULL,
	[UnitsSoldRunAndDriveAutoOnlyNonCAT] [int] NULL,
	[UnitsSoldCarStartNonCAT] [int] NULL,
	[UnitsSoldMileageNonCAT] [int] NULL,
	[UnitsSoldEnhancementNonCAT] [int] NULL,
	[UnitsSoldReRunNonCAT] [int] NULL,
	[VRDUnitsSoldKeyNonCAT] [int] NULL,
	[VRDUnitsSoldRunAndDriveAutoOnlyNonCAT] [int] NULL,
	[VRDUnitsSoldCarStartNonCAT] [int] NULL,
	[VRDUnitsSoldMileageNonCAT] [int] NULL,
	[VRDUnitsSoldEnhancementNonCAT] [int] NULL,
	[VRDReRunNonCAT] [int] NULL,
	[IBFSalePriceNonCAT] [money] NULL,
	[IBFNextHighestHistoricalBidNonCAT] [money] NULL,
	[UnitsSoldPublicNonCAT] [int] NULL,
	[UnitsSoldPushedByPublicNonCAT] [int] NULL,
	[CurrentYearFiscalWeekMinAverageNonCAT] [money] NULL,
	[PriorYearFiscalWeekMinAverageNonCAT] [money] NULL,
	[BranchMinimumSalesPriceIndNonCAT] [int] NULL,
	[CountofPUwithIn100MilesOneBusinessDaysNonCAT] [int] NULL,
	[PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApprovalNonCAT] [int] NULL,
	[TotalPipelineInventoryNonCAT] [int] NULL,
	[UnitsWrappedWithin7DaysOfCheckInNonCAT] [int] NULL,
	[CurrentYearFiscalWeekMinAverageRegionNonCAT] [money] NULL,
	[PriorYearFiscalWeekMinAverageRegionNonCAT] [money] NULL,
	[NumberOfWeekendPickupsNonCAT] [int] NULL,
	[CalendarDaysAssignmenttoReleaseNonCAT] [int] NULL,
	[UnitsAssignedNonCAT] [int] NULL,
	[UnitsNetAssignedNonCAT] [int] NULL,
	[UnitsAssignedPlanNonCAT] [int] NULL,
	[UnitSoldPlanNonCAT] [int] NULL,
	[UnitsSoldsWithKeyOnly] [int] NULL,	
	[VRDUnitsSoldKeyOnlyNonCAT] [int] NULL
	)

IF OBJECT_ID('tempdb..#TMP_KPI_BranchRecordCount') IS NOT NULL DROP TABLE #TMP_KPI_BranchRecordCount
CREATE TABLE #TMP_KPI_BranchRecordCount(ChangeType VARCHAR(30)) 

IF OBJECT_ID('tempdb..#TMP_KPIDay') IS NOT NULL DROP TABLE #TMP_KPIDay
CREATE TABLE #TMP_KPIDay
(	[DayID] [int] NOT NULL,
	[DayDate] [datetime] NOT NULL,
	[DayOfMonth] [varchar](2) NOT NULL,
	[DayOfQtr] [int] NOT NULL,
	[DayOfWeek] [int] NOT NULL,
	[DayOfYear] [int] NOT NULL,
	[DaylightSavingTimeDay] [bit] NOT NULL,
	[DaysInMonth] [int] NOT NULL,
	[DaysInQtr] [int] NOT NULL,
	[DaysInWeek] [int] NOT NULL,
	[DaysInYear] [int] NOT NULL,
	[DowDescShort] [varchar](3) NOT NULL,
	[DowDescLong] [varchar](9) NOT NULL,
	[FiscalMonthID] [int] NOT NULL,
	[FiscalMonthIDDescLong] [varchar](14) NOT NULL,
	[FiscalMonthIDDescShort] [varchar](8) NOT NULL,
	[FiscalQuarterDesc] [varchar](7) NOT NULL,
	[FiscalQuarterID] [int] NOT NULL,
	[FiscalWeekID] [int] NOT NULL,
	[FiscalWeekIDDesc] [varchar](11) NOT NULL,
	[FiscalWeekOfMonthNum] [int] NOT NULL,
	[FiscalYearID] [int] NOT NULL,
	[HolidayFlag] [bit] NOT NULL,
	[MonthDaysLeft] [int] NOT NULL,
	[MonthId] [int] NOT NULL,
	[MonthIDDescLong] [varchar](14) NOT NULL,
	[MonthIDDescShort] [varchar](8) NOT NULL,
	[MonthNum] [int] NOT NULL,
	[MonthNumDescLong] [varchar](9) NOT NULL,
	[MonthNumDescShort] [varchar](3) NOT NULL,
	[QtrDaysLeft] [int] NOT NULL,
	[QtrDesc] [varchar](7) NOT NULL,
	[QtrId] [int] NOT NULL,
	[QtrNum] [int] NOT NULL,
	[QtrNumDescLong] [varchar](11) NOT NULL,
	[QtrNumDescShort] [varchar](7) NOT NULL,
	[WeekEndDate] [datetime] NOT NULL,
	[WeekId] [int] NOT NULL,
	[WeekIDDesc] [varchar](11) NOT NULL,
	[WeekNum] [int] NOT NULL,
	[WeekNumDesc] [varchar](7) NOT NULL,
	[WeekDayFlag] [bit] NOT NULL,
	[WeekDaysLeft] [int] NOT NULL,
	[WeekEndFlag] [bit] NOT NULL,
	[WorkDaysPreviousFiscalWeek] [int] NOT NULL,
	[YearDaysLeft] [int] NOT NULL,
	[YearID] [int] NOT NULL,
	[CalendarQTR] [int] NULL,
	[CalendarQTRNum] [int] NULL,
	[LastDayOfWeekDayID] [int] NULL
	)

IF OBJECT_ID('tempdb..#TMP_Budget') IS NOT NULL DROP TABLE #TMP_Budget
CREATE TABLE #TMP_Budget
(	[FiscalWeekID] [int] NOT NULL,
	[BranchNumber] [int] NOT NULL,
	[PlannedUnitsAssigned] [int] NULL,
	[PlannedUnitsSold] [int] NULL,
	[UnitsAssignedPlanNonCAT] [int] NULL,
	[UnitSoldPlanNonCAT] [int] NULL
	)

IF OBJECT_ID('tempdb..#TMP_Assignments') IS NOT NULL DROP TABLE #TMP_Assignments
CREATE TABLE #TMP_Assignments
(	[FiscalWeekID] [int] NOT NULL,
	[AssignmentBranchNumber] [int] NOT NULL,
	[CancelledAssignments] [int] NULL,
	[UnitsAssigned] [int]  NULL,
	[UnitsNetAssigned] [int] NULL,
	[ElectronicAssignments] [int] NULL,
	[CancelledAssignmentsNonCAT] [int] NULL,
	[UnitsAssignedNonCAT] [int] NULL,
	[UnitsNetAssignedNonCAT] [int] NULL,	
	[ElectronicAssignmentsNonCAT] [int] NULL
	)

IF OBJECT_ID('tempdb..#TMP_ReleasePickup') IS NOT NULL DROP TABLE #TMP_ReleasePickup
CREATE TABLE #TMP_ReleasePickup
(	[FiscalWeekID] [int] NOT NULL,
	[BranchNumber] [int] NOT NULL,
	[NumberofPickups] [int] NOT NULL,
	[CalendarDaysReleasetoPickup] [int] NOT NULL,
	[NumberofPickupsNonCAT] [int] NULL,
	[CalendarDaysReleasetoPickupNonCAT] [int] NULL	
	)
	
IF OBJECT_ID('tempdb..#TMP_ReleaseToPickup') IS NOT NULL DROP TABLE #TMP_ReleaseToPickup
CREATE TABLE #TMP_ReleaseToPickup
(	[FiscalWeekID] [int] NOT NULL,
	[BranchNumber] [int] NOT NULL,
	[NumberofPickups100MilesWithInOneBusinessDays] [int] NOT NULL,
	[NumberofPickupsSameDayReleasedByNoonWithin35Miles] [int] NOT NULL,
	[NumberOfWeekendPickups] [int]  NULL,
	[NumberofPickups100MileswithinoneBusinessDaysNonCAT] [int] NULL,
	[NumberofPickupsSameDayReleasedByNoonWithin35MilesNonCAT] [int] NULL,
	[NumberOfWeekendPickupsNonCAT] [int] NULL	
	)
	
IF OBJECT_ID('tempdb..#TMP_ReleaseProblem') IS NOT NULL DROP TABLE #TMP_ReleaseProblem
CREATE TABLE #TMP_ReleaseProblem
(	[FiscalWeekID] [int] NOT NULL,
	[BranchNumber] [int] NOT NULL,
	[NumberOfReleaseProblems] [int] NOT NULL
	)

IF OBJECT_ID('tempdb..#TMP_ReleaseProblemNonCAT') IS NOT NULL DROP TABLE #TMP_ReleaseProblemNonCAT
CREATE TABLE #TMP_ReleaseProblemNonCAT
(	[FiscalWeekID] [int] NOT NULL,
	[BranchNumber] [int] NOT NULL,
	[NumberofReleaseProblemsNonCAT] [int] NULL
	)	

IF OBJECT_ID('tempdb..#TMP_Inventory') IS NOT NULL DROP TABLE #TMP_Inventory
CREATE TABLE #TMP_Inventory
(	[FiscalWeekID] [int] NOT NULL,
	[AdministrativeBranchNumber] [int] NOT NULL,
	[PipelineInventory] [int] NULL,
	[PipelineInventoryNonCAT] [int] NULL
	)
	
	
IF OBJECT_ID('tempdb..#TMP_Sale') IS NOT NULL DROP TABLE #TMP_Sale 
CREATE TABLE #TMP_Sale
(	[FiscalWeekID] [int] NOT NULL,
	[AdministrativeBranchNumber] [int] NOT NULL,
	[UnitSoldCount] [int] NULL,
	[PAUnitSold] [int] NULL,
	[TotalSalePrice] [money] NULL,
	[ProviderRevenue] [money] NULL,
	[PARevenue] [money] NULL,
	[ProviderNetReturn] [money] NULL,
	[BuyerRevenue] [money] NULL,
	[TowCost] [money] NULL,
	[UnitSoldNonCAT] [int] NULL,
	[PAUnitSoldNonCAT] [money] NULL,
	[TotalSalePriceNonCAT] [money] NULL,
	[ProviderRevenueNonCAT] [money] NULL,
	[PARevenueNonCAT] [money] NULL,
	[ProviderNetRevenueNonCAT] [money] NULL,
	[BuyerRevenueNonCAT] [money] NULL,
	[TowCostNonCAT] [money] NULL	
	)	

IF OBJECT_ID('tempdb..#TMP_MinimumSalesPrice') IS NOT NULL DROP TABLE #TMP_MinimumSalesPrice
CREATE TABLE #TMP_MinimumSalesPrice
(	[FiscalWeekID] [int] NOT NULL,
	[AdministrativeBranchNumber] [int] NOT NULL,
	[MinimumSalesPrice] [money] NULL,
	[MinimumSalesPriceNonCAT] [money] NULL
	)
	
IF OBJECT_ID('tempdb..#TMP_SetForSale') IS NOT NULL DROP TABLE #TMP_SetForSale 
CREATE TABLE #TMP_SetForSale
(	[FiscalWeekID] [int] NOT NULL,
	[BranchNumber] [int] NOT NULL,
	[UnitsSetForSale] [int] NULL,
	[UnitsSetForSaleNonCAT] [int] NULL
	)	
	
IF OBJECT_ID('tempdb..#TMP_BidTypeSold') IS NOT NULL DROP TABLE #TMP_BidTypeSold 
CREATE TABLE #TMP_BidTypeSold
(	[FiscalWeekID] [int] NOT NULL,
	[AdministrativeBranchNumber] [int] NOT NULL,
	[IBIDSoldUnits] [int] NULL,
	[ProxySoldUnits] [int] NULL,
	[IBFSoldUnits] [int] NULL,
	[OutOfCountrySoldUnits] [int] NULL,
	[OutOfStateSoldUnits] [int] NULL,
	[SuccessfulBuyer] [int] NULL,
	[IBIDSoldUnitsNonCAT] [int] NULL,
	[ProxySoldUnitsNonCAT] [int] NULL,
	[IBFSoldUnitsNonCAT] [int] NULL,
	[OutofCountrySoldUnitsNonCAT] [int] NULL,
	[OutofStateSoldUnitsNonCAT] [int] NULL,
	[SuccessfulBuyerNonCAT] [int] NULL,
	)	
	
IF OBJECT_ID('tempdb..#TMP_BuyerAttendance') IS NOT NULL DROP TABLE #TMP_BuyerAttendance 
CREATE TABLE #TMP_BuyerAttendance
(	[FiscalWeekID] [int] NOT NULL,
	[BranchNumber] [int] NOT NULL,
	[BuyerAttendance] [int] NULL,
	[OutofCountryBuyerAttendance] [int] NULL,
	[GuestAttendance] [int] NULL,
	[BuyerAttendanceNonCAT] [int] NULL,
	[OutofCountryBuyerAttendanceNonCAT] [int] NULL,
	[GuestAttendanceNonCAT] [int] NULL
	)	

IF OBJECT_ID('tempdb..#TMP_VolumeProceeds') IS NOT NULL DROP TABLE #TMP_VolumeProceeds 
CREATE TABLE #TMP_VolumeProceeds
(	[FiscalWeekID] [int] NOT NULL,
	[AdministrativeBranchNumber] [int] NOT NULL,
	[InsuranceUnitsSold] [int] NULL,
	[InsuranceSalePrice][money] NULL,
	[InsuranceACV][money] NULL,
	[CharityUnitsSold][int] NULL,
	[CharityTotalSalePrice][money] NULL,
	[VRDUnitsSold][int] NULL,
	[VRDTotalSalePrice][money] NULL,
	[RentalUnitsSold][int] NULL,
	[RentalTotalSalePrice][money] NULL,
	[OtherUnitsSold][int] NULL,
	[OtherTotalSalePrice][money] NULL,
	[InsuranceGrossReturn][money] NULL,
	[InsuranceUnitsSoldNonCAT] [int] NULL,
	[InsuranceSalePriceNonCAT] [money] NULL,
	[InsuranceACVNonCAT] [money] NULL,
	[CharityUnitsSoldNonCAT] [int] NULL,
	[CharityTotalSalePriceNonCAT] [money] NULL,
	[VRDUnitsSoldNonCAT] [int] NULL,
	[VRDTotalSalePriceNonCAT] [money] NULL,
	[RentalUnitsSoldNonCAT] [int] NULL,
	[RentalTotalSalePriceNonCAT] [money] NULL,
	[OtherUnitsSoldNonCAT] [int] NULL,
	[OtherTotalSalePriceNonCAT] [money] NULL,
	[InsuranceGrossReturnNonCAT] [money] NULL
	)
	
--KPI Corporate Temp Tables

IF OBJECT_ID('tempdb..#TMP_KPI_Corporate') IS NOT NULL DROP TABLE #TMP_KPI_Corporate
CREATE TABLE #TMP_KPI_Corporate
(	[FiscalWeekID] [int] NOT NULL,
	[LastdayOfweekid] [int] NOT NULL,
	[InsuranceGrossReturn] [money] NULL,
	[InsuranceUnitsSold] [int] NULL,
	[InsuranceSalePrice] [money] NULL,
	[InsuranceACV] [money] NULL,
	[InsuranceBottom20GrossReturn] [money] NULL,
	[InsuranceBottom20Units] [int] NULL,
	[InsuranceMiddle60GrossReturn] [money] NULL,
	[InsuranceMiddle60Units] [int] NULL,
	[InsuranceTop20GrossReturn] [money] NULL,
	[InsuranceTop20Units] [int] NULL,
	[InsuranceGrossReturnNonCAT] [money] NULL,
    [InsuranceUnitsSoldNonCAT] [int] NULL,
    [InsuranceSalePriceNonCAT] [money] NULL,
    [InsuranceACVNonCAT] [money] NULL,
    [InsuranceBottom20GrossReturnNonCAT] [money] NULL,
    [InsuranceBottom20UnitsNonCAT] [int] NULL,
    [InsuranceMiddle60GrossReturnNonCAT] [money] NULL,
    [InsuranceMiddle60UnitsNonCAT] [int] NULL,
    [InsuranceTop20GrossReturnNonCAT] [money] NULL,
    [InsuranceTop20UnitsNonCAT] [int] NULL
)	

IF OBJECT_ID('tempdb..#TMP_KPI_CorporateRecordCount') IS NOT NULL DROP TABLE #TMP_KPI_CorporateRecordCount
CREATE TABLE #TMP_KPI_CorporateRecordCount(ChangeType VARCHAR(30)) 

IF OBJECT_ID('tempdb..#TMP_Sale_Corporate') IS NOT NULL DROP TABLE #TMP_Sale_Corporate 
CREATE TABLE #TMP_Sale_Corporate
(	[FiscalWeekID] [int] NOT NULL,
	[InsuranceGrossReturn] [money] NULL,
	[InsuranceUnitsSold] [int] NULL,
	[InsuranceSalePrice] [money] NULL,
	[InsuranceACV] [money] NULL,
	[InsuranceGrossReturnNonCAT] [money] NULL,
	[InsuranceUnitsSoldNonCAT] [int] NULL,
	[InsuranceSalePriceNonCAT] [money] NULL,
	[InsuranceACVNonCAT] [money] NULL	
	)

IF OBJECT_ID('tempdb..#TMP_Top20Percent_StockID') IS NOT NULL DROP TABLE #TMP_Top20Percent_StockID
CREATE TABLE #TMP_Top20Percent_StockID
(	[StockID] [int] NOT NULL,
	
	)		
IF OBJECT_ID('tempdb..#TMP_Top20Percent') IS NOT NULL DROP TABLE #TMP_Top20Percent
CREATE TABLE #TMP_Top20Percent
(	[FiscalWeekID] [int] NOT NULL,
	[InsuranceTop20Units] [int] NULL,
	[InsuranceTop20GrossReturn] [money] NULL
	)	

IF OBJECT_ID('tempdb..#TMP_Bottom20Percent_StockID') IS NOT NULL DROP TABLE #TMP_Bottom20Percent_StockID
CREATE TABLE #TMP_Bottom20Percent_StockID
(	[StockID] [int] NOT NULL,
	)	
		
IF OBJECT_ID('tempdb..#TMP_Bottom20Percent') IS NOT NULL DROP TABLE #TMP_Bottom20Percent
CREATE TABLE #TMP_Bottom20Percent
(	[FiscalWeekID] [int] NOT NULL,
	[InsuranceBottom20Units] [int] NULL,
	[InsuranceBottom20GrossReturn] [money] NULL
	)	

IF OBJECT_ID('tempdb..#TMP_Middle60Percent_StockID') IS NOT NULL DROP TABLE #TMP_Middle60Percent_StockID
CREATE TABLE #TMP_Middle60Percent_StockID
(	[FiscalWeekID] [int] NOT NULL,
	[StockID] [int] NOT NULL,
	[InsuranceMiddle60GrossReturn] [money] NULL
	)	
IF OBJECT_ID('tempdb..#TMP_Middle60Percent') IS NOT NULL DROP TABLE #TMP_Middle60Percent
CREATE TABLE #TMP_Middle60Percent
(	[FiscalWeekID] [int] NOT NULL,
	[InsuranceMiddle60units] [int] NOT NULL,
	[InsuranceMiddle60GrossReturn] [money] NULL
	)

IF OBJECT_ID('tempdb..#TMP_Insurance_GrossReturn') IS NOT NULL DROP TABLE #TMP_Insurance_GrossReturn
CREATE TABLE #TMP_Insurance_GrossReturn
(	[FiscalWeekID] [int] NOT NULL,
	[StockID] [int] NULL,
	[GrossReturn] [money] NULL	
	)
	
IF OBJECT_ID('tempdb..#TMP_CountofPUwithIn100MilesOneBusinessDays') IS NOT NULL DROP TABLE #TMP_CountofPUwithIn100MilesOneBusinessDays
CREATE TABLE #TMP_CountofPUwithIn100MilesOneBusinessDays
(	[FiscalWeekID] [int] NOT NULL,
	[BranchNumber] [int] NOT NULL,
	[CountofPUwithIn100MilesOneBusinessDays] [int] NOT NULL,
	[CountofPUwithIn100MilesOneBusinessDaysNonCAT] [int] NULL
	)	

-- KPI Operational Objects	
IF OBJECT_ID('tempdb..#TMP_Sold_ACV') IS NOT NULL DROP TABLE #TMP_Sold_ACV 
CREATE TABLE #TMP_Sold_ACV
(	[FiscalWeekID] [int] NOT NULL,
	[BranchNumber] [int] NOT NULL,
	[BlackbookACV] [money] NULL,
	[ACVNonCAT] [money] NULL	
	)

IF OBJECT_ID('tempdb..#TMP_SoldVehicleAtrributes') IS NOT NULL DROP TABLE #TMP_SoldVehicleAtrributes 
CREATE TABLE #TMP_SoldVehicleAtrributes
(	[FiscalWeekID] [int] NOT NULL,
	[BranchNumber] [int] NOT NULL,
	[Keys] [int]  NULL,
	[RunAndDriveAutoOnly] [int]  NULL,
	[CarStart] [int]  NULL,
	[Mileage] [int]  NULL,
	[Enhancement] [int]  NULL,
	[ReRun] [int]  NULL,
	[VRDKeys] [int]  NULL,
	[VRDRunAndDriveAutoOnly] [int]  NULL,
	[VRDCarStart] [int]  NULL,
	[VRDMileage] [int]  NULL,
	[VRDEnhancement] [int]  NULL,
	[VRDReRun] [int]  NULL,
	[UnitsSoldsWithKeyNonCAT] [int] NULL,
	[UnitsSoldsWithKeyOnly] [int] NULL,
	[UnitsSoldRunAndDriveAutoOnlyNonCAT] [int] NULL,
	[UnitsSoldCarStartNonCAT] [int] NULL,
	[UnitsSoldMileageNonCAT] [int] NULL,
	[UnitsSoldEnhancementNonCAT] [int] NULL,
	[UnitsSoldReRunNonCAT] [int] NULL,
	[VRDUnitsSoldKeyNonCAT] [int] NULL,
	[VRDUnitsSoldRunAndDriveAutoOnlyNonCAT] [int] NULL,
	[VRDUnitsSoldCarStartNonCAT] [int] NULL,
	[VRDUnitsSoldMileageNonCAT] [int] NULL,
	[VRDUnitsSoldEnhancementNonCAT] [int] NULL,
	[VRDReRunNonCAT] [int] NULL,
	[VRDUnitsSoldKeyOnlyNonCAT] [int] NULL
	)

IF OBJECT_ID('tempdb..#TMP_IBF') IS NOT NULL DROP TABLE #TMP_IBF 
CREATE TABLE #TMP_IBF
(	[FiscalWeekID] [int] NOT NULL,
	[BranchNumber] [int] NOT NULL,
	[IBFUnitsSold] [int] NULL,
	[IBFSalePrice] [money] NULL,
	[IBFNextHighestHistoricalBid] [money] NULL,
	[IBFSoldUnitsNonCAT] [int] NULL,
	[IBFSalePriceNonCAT] [money] NULL,
	[IBFNextHighestHistoricalBidNonCAT] [money] NULL,
	)	
	
IF OBJECT_ID('tempdb..#TMP_BuyerMetrics') IS NOT NULL DROP TABLE #TMP_BuyerMetrics 
CREATE TABLE #TMP_BuyerMetrics
(	[FiscalWeekID] [int] NOT NULL,
	[BranchNumber] [int] NOT NULL,
	[PublicUnitsSold] [int]  NULL,
	[UnitsSoldPublicNonCAT] [int] NULL
	)	
	
IF OBJECT_ID('tempdb..#TMP_CalendarDaysAssignmentToRelease') IS NOT NULL DROP TABLE #TMP_CalendarDaysAssignmentToRelease 
CREATE TABLE #TMP_CalendarDaysAssignmentToRelease
(	[FiscalWeekID] [int] NOT NULL,
	[BranchNumber] [int] NOT NULL,
	[CalendarDaysAssignmentToRelease] [int]  NULL,
	[CalendarDaysAssignmenttoReleaseNonCAT] [int] NULL
	)	
	
IF OBJECT_ID('tempdb..#TMP_PushedByPublic') IS NOT NULL DROP TABLE #TMP_PushedByPublic 
CREATE TABLE #TMP_PushedByPublic
(	[FiscalWeekID] [int] NOT NULL,
	[BranchNumber] [int] NOT NULL,
	[PushedByPublic] [int]  NULL,
	[UnitsSoldPushedByPublicNonCAT] [int] NULL	
	)		

IF OBJECT_ID('tempdb..#TMP_UnitsWrappedWithin7DaysOfCheckIn') IS NOT NULL DROP TABLE #TMP_UnitsWrappedWithin7DaysOfCheckIn 
CREATE TABLE #TMP_UnitsWrappedWithin7DaysOfCheckIn
(	[FiscalWeekID] [int] NOT NULL,
	[BranchNumber] [int] NOT NULL,
	[UnitsWrappedWithin7DaysOfCheckIn] [int]  NULL,
	[UnitsWrappedWithin7DaysOfCheckInNonCAT] [int] NULL	
	)		

IF OBJECT_ID('tempdb..#TMP_PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApproval') IS NOT NULL DROP TABLE #TMP_PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApproval
CREATE TABLE #TMP_PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApproval 
(	[FiscalWeekID] [int] NOT NULL,
	[BranchNumber] [int] NOT NULL,
	[PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApproval] [int]  NULL,
	[PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApprovalNonCAT] [int] NULL	
	)

IF OBJECT_ID('tempdb..#TMP_Insurance_GrossReturn_NonCAT') IS NOT NULL DROP TABLE #TMP_Insurance_GrossReturn_NonCAT
CREATE TABLE #TMP_Insurance_GrossReturn_NonCAT
(	[FiscalWeekID] [int] NOT NULL,
	[StockID] [int] NULL,
	[GrossReturn] [money] NULL	
	)		

IF OBJECT_ID('tempdb..#TMP_Top20Percent_StockID_NonCAT') IS NOT NULL DROP TABLE #TMP_Top20Percent_StockID_NonCAT
CREATE TABLE #TMP_Top20Percent_StockID_NonCAT
(	[StockID] [int] NOT NULL,
	
	)		

IF OBJECT_ID('tempdb..#TMP_Top20Percent_NonCAT') IS NOT NULL DROP TABLE #TMP_Top20Percent_NonCAT
CREATE TABLE #TMP_Top20Percent_NonCAT
(	[FiscalWeekID] [int] NOT NULL,
	[InsuranceTop20Units] [int] NULL,
	[InsuranceTop20GrossReturn] [money] NULL
	)	

IF OBJECT_ID('tempdb..#TMP_Bottom20Percent_StockID_NonCAT') IS NOT NULL DROP TABLE #TMP_Bottom20Percent_StockID_NonCAT
CREATE TABLE #TMP_Bottom20Percent_StockID_NonCAT
(	[StockID] [int] NOT NULL,
	)	
		
IF OBJECT_ID('tempdb..#TMP_Bottom20Percent_NonCAT') IS NOT NULL DROP TABLE #TMP_Bottom20Percent_NonCAT
CREATE TABLE #TMP_Bottom20Percent_NonCAT
(	[FiscalWeekID] [int] NOT NULL,
	[InsuranceBottom20Units] [int] NULL,
	[InsuranceBottom20GrossReturn] [money] NULL
	)	

IF OBJECT_ID('tempdb..#TMP_Middle60Percent_StockID_NonCAT') IS NOT NULL DROP TABLE #TMP_Middle60Percent_StockID_NonCAT
CREATE TABLE #TMP_Middle60Percent_StockID_NonCAT
(	[FiscalWeekID] [int] NOT NULL,
	[StockID] [int] NOT NULL,
	[InsuranceMiddle60GrossReturn] [money] NULL
	)	
IF OBJECT_ID('tempdb..#TMP_Middle60Percent_NonCAT') IS NOT NULL DROP TABLE #TMP_Middle60Percent_NonCAT
CREATE TABLE #TMP_Middle60Percent_NonCAT
(	[FiscalWeekID] [int] NOT NULL,
	[InsuranceMiddle60units] [int] NOT NULL,
	[InsuranceMiddle60GrossReturn] [money] NULL
	)
	
IF OBJECT_ID('tempdb..#TMP_BuyerRevnue') IS NOT NULL DROP TABLE #TMP_BuyerRevnue
CREATE TABLE #TMP_BuyerRevnue
        ( FiscalWeekID [int] NOT NULL,
        BranchNumber [int] NULL,		
		BuyerRevenueNonCAT [money] NULL ,
		BuyerRevenue [money] NULL )		

IF OBJECT_ID('tempdb..#TMP_WeekendPickups') IS NOT NULL DROP TABLE #TMP_WeekendPickups
CREATE TABLE #TMP_WeekendPickups
(	[FiscalWeekID] [int] NOT NULL,
	[BranchNumber] [int] NOT NULL,
	[NumberOfWeekendPickups] [int] NOT NULL, 
	[NumberOfWeekendPickupsNonCAT] [int] NULL
	)


IF OBJECT_ID('tempdb..#TMP_KPIDayForLast4Weeks') IS NOT NULL DROP TABLE #TMP_KPIDayForLast4Weeks
CREATE TABLE #TMP_KPIDayForLast4Weeks
(	
[DayID] [int] NOT NULL,
[FiscalWeekID] [int] NOT NULL
)

IF OBJECT_ID('tempdb..#TMP_SoldVehicleAtrributes_RunAndDrive') IS NOT NULL DROP TABLE #TMP_SoldVehicleAtrributes_RunAndDrive
CREATE TABLE #TMP_SoldVehicleAtrributes_RunAndDrive
(	[FiscalWeekID] [int] NOT NULL,
	[BranchNumber] [int] NOT NULL,
	[RunAndDriveAutoOnly] [int]  NULL,
	[VRDRunAndDriveAutoOnly] [int]  NULL,
	[UnitsSoldRunAndDriveAutoOnlyNonCAT] [int] NULL,
	[VRDUnitsSoldRunAndDriveAutoOnlyNonCAT] [int] NULL
)


		
/*********************************************************************************************
Populate KPI Temp Tables
*********************************************************************************************/
INSERT INTO #TMP_KPIDay
(	[DayID],
	[DayDate],
	[DayOfMonth],
	[DayOfQtr],
	[DayOfWeek],
	[DayOfYear],
	[DaylightSavingTimeDay],
	[DaysInMonth],
	[DaysInQtr],
	[DaysInWeek],
	[DaysInYear],
	[DowDescShort],
	[DowDescLong],
	[FiscalMonthID],
	[FiscalMonthIDDescLong],
	[FiscalMonthIDDescShort] ,
	[FiscalQuarterDesc],
	[FiscalQuarterID],
	[FiscalWeekID],
	[FiscalWeekIDDesc] ,
	[FiscalWeekOfMonthNum],
	[FiscalYearID],
	[HolidayFlag],
	[MonthDaysLeft],
	[MonthId],
	[MonthIDDescLong],
	[MonthIDDescShort],
	[MonthNum],
	[MonthNumDescLong],
	[MonthNumDescShort],
	[QtrDaysLeft],
	[QtrDesc],
	[QtrId],
	[QtrNum],
	[QtrNumDescLong],
	[QtrNumDescShort],
	[WeekEndDate],
	[WeekId],
	[WeekIDDesc],
	[WeekNum],
	[WeekNumDesc],
	[WeekDayFlag] ,
	[WeekDaysLeft],
	[WeekEndFlag],
	[WorkDaysPreviousFiscalWeek],
	[YearDaysLeft],
	[YearID],
	[CalendarQTR],
	[CalendarQTRNum],
	[LastDayOfWeekDayID]
	)
SELECT	[DayID],
	[DayDate],
	[DayOfMonth],
	[DayOfQtr],
	[DayOfWeek],
	[DayOfYear],
	[DaylightSavingTimeDay],
	[DaysInMonth],
	[DaysInQtr],
	[DaysInWeek],
	[DaysInYear],
	[DowDescShort],
	[DowDescLong],
	[FiscalMonthID],
	[FiscalMonthIDDescLong],
	[FiscalMonthIDDescShort] ,
	[FiscalQuarterDesc],
	[FiscalQuarterID],
	[FiscalWeekID],
	[FiscalWeekIDDesc] ,
	[FiscalWeekOfMonthNum],
	[FiscalYearID],
	[HolidayFlag],
	[MonthDaysLeft],
	[MonthId],
	[MonthIDDescLong],
	[MonthIDDescShort],
	[MonthNum],
	[MonthNumDescLong],
	[MonthNumDescShort],
	[QtrDaysLeft],
	[QtrDesc],
	[QtrId],
	[QtrNum],
	[QtrNumDescLong],
	[QtrNumDescShort],
	[WeekEndDate],
	[WeekId],
	[WeekIDDesc],
	[WeekNum],
	[WeekNumDesc],
	[WeekDayFlag] ,
	[WeekDaysLeft],
	[WeekEndFlag],
	[WorkDaysPreviousFiscalWeek],
	[YearDaysLeft],
	[YearID],
	[CalendarQTR],
	[CalendarQTRNum],
	@CurrentWeekLastDayofWeekDayID
FROM dbo.DimDay WITH (NOLOCK)
WHERE [DayID] IN (SELECT DayID FROM dbo.DimDay Where FiscalWeekID = @CurrentRunWeekID)	


INSERT INTO #TMP_KPI_Branch
(	[BranchNumber],
	[Lastdayofweekid],
	[FiscalWeekID]
)
SELECT DISTINCT DB.BranchNumber,
		D.LastDayOfWeekDayID,
		D.FiscalWeekID
FROM dbo.DimBranch DB WITH(NOLOCK)
CROSS JOIN #TMP_KPIDay D with (nolock)
WHERE D.DayID = @CurrentRunDayID

INSERT INTO #TMP_Budget       
(	FiscalWeekID,
	BranchNumber,
	PlannedUnitsAssigned,
	PlannedUnitsSold,
	UnitsAssignedPlanNonCAT,
	UnitSoldPlanNonCAT
	)
SELECT  D.FiscalWeekID,
		FB.BranchNumber,
		sum(FB.PlannedUnitsAssigned) AS PlannedUnitsAssigned,
		sum(FB.PlannedUnitsSold) AS PlannedUnitsSold,
		sum(FB.PlannedUnitsAssigned) AS UnitsAssignedPlanNonCAT,
		sum(FB.PlannedUnitsSold) AS UnitSoldPlanNonCAT		
FROM	dbo.FactBudget FB WITH(NOLOCK)  
INNER JOIN #TMP_KPIDay D WITH(NOLOCK)  
ON D.DayID  = FB.LastDayofWeekID
GROUP BY D.Fiscalweekid, FB.BranchNumber	


INSERT INTO #TMP_Assignments
(	FiscalWeekID,
	AssignmentBranchNumber,
	CancelledAssignments,
	UnitsAssigned,
	UnitsNetAssigned,
	ElectronicAssignments,
	CancelledAssignmentsNonCAT,
	UnitsAssignedNonCAT,
	UnitsNetAssignedNonCAT,	
	ElectronicAssignmentsNonCAT
	)
SELECT	D.FiscalWeekID,
		FA.AssignmentBranchNumber,
		ABS(SUM(FA.CancelledAssignments)) AS CancelledAssignments,	
		SUM(FA.unitsassignedGross) AS UnitsAssigned,
		SUM (FA.UnitsAssigned) AS UnitsNetAssigned,
		SUM(FA.ElectronicAssignments) AS ElectronicAssignments,
		--NonCAT
		ABS(SUM(CASE WHEN DS.IsCAT = 0 THEN FA.CancelledAssignments ELSE 0 END)) AS CancelledAssignmentsNonCAT,	
		SUM(CASE WHEN DS.IsCAT = 0 THEN FA.unitsassignedGross ELSE 0 END) AS UnitsAssignedNonCAT, 
		SUM(CASE WHEN DS.IsCAT = 0 THEN FA.UnitsAssigned ELSE 0 END) AS UnitsNetAssignedNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 THEN FA.ElectronicAssignments ELSE 0 END) AS ElectronicAssignmentsNonCAT
FROM	dbo.FactAssignment AS FA WITH(NOLOCK)  
INNER JOIN dbo.DimStock AS DS WITH(NOLOCK)
 ON FA.StockID = DS.StockID
INNER JOIN #TMP_KPIDay AS D WITH(NOLOCK)  
 ON FA.assigneddayid = d.dayid
WHERE DS.AssignmentTypeCode NOT IN ('TOW', 'TTL')
GROUP BY D.fiscalweekid, FA.AssignmentBranchNumber


INSERT INTO #TMP_ReleasePickup
(	FiscalWeekID,
	BranchNumber,
	NumberofPickups,
	CalendarDaysReleasetoPickup,
	NumberofPickupsNonCAT,
	CalendarDaysReleasetoPickupNonCAT	
	)
SELECT	D.FiscalWeekID,
		FST.BranchNumber,
		COUNT(FST.TowBillNumber) AS NumberofPickups,
		SUM(FSC.ReleaseToPickupCalendar) AS CalendarDaysReleasetoPickup,
		--NonCAT
		COUNT(CASE WHEN DS.IsCAT = 0 THEN FST.TowBillNumber ELSE 0 END) AS NumberofPickupsNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 THEN FSC.ReleaseToPickupCalendar ELSE 0 END) AS CalendarDaysReleasetoPickupNonCAT
FROM  dbo.FactStockTow FST WITH(NOLOCK)   
INNER JOIN dbo.FactStockTowCycleTime FSC WITH(NOLOCK)  
ON FSC.TowBillNumber  = FST.TowBillNumber
INNER JOIN #TMP_KPIDay D WITH(NOLOCK)  
ON FSC.TowCompletedDayID = D.DayID
INNER JOIN dbo.DimStock DS WITH (NOLOCK)
 ON FST.StockID = DS.StockID
WHERE FST.TowTypeCode = 'PK'
AND FST.IsTowSuccessful = 1
AND DS.IsDeliveredToBranch = 0
GROUP BY D.FiscalWeekID, FST.BranchNumber


INSERT INTO #TMP_ReleaseToPickup
(	FiscalWeekID,
	BranchNumber,
	NumberOfPickups100MilesWithInOneBusinessDays,
	NumberOfPickupsSameDayReleasedByNoonWithin35Miles,
	NumberOfWeekendPickups,
	NumberofPickups100MileswithinoneBusinessDaysNonCAT,
	NumberofPickupsSameDayReleasedByNoonWithin35MilesNonCAT,
	NumberOfWeekendPickupsNonCAT
	)
SELECT	D.FiscalWeekID,
		FST.BranchNumber,
		SUM(CASE WHEN (FST.TowMiles <= 100 AND FST.TowerPickupPerformance IN ('Early','On Time')) THEN 1 ELSE 0 END)
			AS NumberOfPickups100MilesWithInOneBusinessDays,
		SUM(CASE WHEN (FST.TowMiles <= 35 AND (FSTCT.SalvageDroppedOffDayID = FSTCT.ActualReleaseDayID) 
			AND Datepart(HOUR, ActualReleaseTimezonedDateTime)<=12) THEN 1 ELSE 0 END)
			AS NumberOfPickupsSameDayReleasedByNoonWithin35Miles,
		SUM(CASE WHEN (D.WeekEndFlag = 1) THEN 1 ELSE 0 END) AS NumberOfWeekendPickups,	
		--NonCAT
		SUM(CASE WHEN (DS.IsCAT = 0 AND FST.TowMiles <= 100 AND FST.TowerPickupPerformance IN ('Early','On Time')) THEN 1 ELSE 0 END)
			AS NumberofPickups100MileswithinoneBusinessDaysNonCAT,
		SUM(CASE WHEN (DS.IsCAT = 0 AND FST.TowMiles <= 35 AND (FSTCT.SalvageDroppedOffDayID = FSTCT.ActualReleaseDayID) 
			AND Datepart(HOUR, ActualReleaseTimezonedDateTime)<=12) THEN 1 ELSE 0 END)
			AS NumberofPickupsSameDayReleasedByNoonWithin35MilesNonCAT,
		SUM(CASE WHEN (DS.IsCAT = 0 AND D.WeekEndFlag = 1) THEN 1 ELSE 0 END) AS NumberOfWeekendPickupsNonCAT				
FROM dbo.FactStockTow FST WITH (NOLOCK)
INNER JOIN dbo.FactStockTowCycleTime FSTCT WITH (NOLOCK)
 ON FST.StockID = FSTCT.StockID
 AND FST.TowBillNumber = FSTCT.TowBillNumber
INNER JOIN #TMP_KPIDay D WITH(NOLOCK)
 ON FSTCT.TowCompletedDayID = D.DayID
INNER JOIN dbo.DimStock DS WITH (NOLOCK)
 ON FST.StockID = DS.StockID
WHERE FST.TowTypeCode ='PK'
AND FST.IsTowSuccessful = 1
AND DS.IsDeliveredToBranch = 0
GROUP BY D.FiscalWeekID, FST.BranchNumber


INSERT INTO #TMP_ReleaseProblem
(	FiscalWeekID,
	BranchNumber,
	NumberOfReleaseProblems
	)
SELECT	D.FiscalWeekID,
		DS.BranchNumber,
		COUNT(DISTINCT DS.StockID)AS NumberOfReleaseProblems
FROM  dbo.FactReleaseProblem FRP WITH(NOLOCK) 
INNER JOIN dbo.Dimstock DS WITH(NOLOCK) 
 ON DS.StockID = FRP.StockID
INNER JOIN #TMP_KPIDay D WITH(NOLOCK)
ON FRP.ReleaseProblemStartDayID = D.DayID
WHERE DS.IsDeliveredToBranch = 0
and DS.AssignmentTypeCode not in ('TOW', 'TTl')		
GROUP BY D.FiscalWeekID, DS.BranchNumber

INSERT INTO #TMP_ReleaseProblemNonCAT
(	FiscalWeekID,
	BranchNumber,
	NumberofReleaseProblemsNonCAT
	)
SELECT	D.FiscalWeekID,
		DS.BranchNumber,
		COUNT(DISTINCT DS.StockID)AS NumberofReleaseProblemsNonCAT
FROM  dbo.FactReleaseProblem FRP WITH(NOLOCK) 
INNER JOIN dbo.Dimstock DS WITH(NOLOCK) 
 ON DS.StockID = FRP.StockID
INNER JOIN #TMP_KPIDay D WITH(NOLOCK)
 ON FRP.ReleaseProblemStartDayID = D.DayID
WHERE DS.IsCAT = 0
AND DS.IsDeliveredToBranch = 0
AND DS.AssignmentTypeCode not in ('TOW', 'TTl')		
GROUP BY D.FiscalWeekID, DS.BranchNumber


INSERT INTO #TMP_Inventory
(	FiscalWeekID ,
	AdministrativeBranchNumber,
	PipelineInventory,
	PipelineInventoryNonCAT
	)
SELECT	D.FiscalWeekID,
		FI.AdministrativeBranchNumber,
		SUM (FI.PipelineInventory) AS PipelineInventory,
		SUM (FI.PipelineInventoryNonCAT) AS PipelineInventoryNonCAT
FROM	dbo.FactInventory FI with (nolock) 
INNER JOIN #TMP_KPIDay D with (nolock)
ON  D.DayID = FI.DayID 
WHERE D.DayID = @CurrentRunDayID 
GROUP BY FiscalWeekID,Administrativebranchnumber
	
	
INSERT INTO #TMP_Sale
(	FiscalWeekID,
	AdministrativeBranchNumber,
	UnitSoldCount,
	PAUnitSold,
	TotalSalePrice,
	ProviderRevenue,
	PARevenue,
	ProviderNetReturn,
	BuyerRevenue,
	TowCost,
	UnitSoldNonCAT,
	PAUnitSoldNonCAT,
	TotalSalePriceNonCAT,
	ProviderRevenueNonCAT,
	PARevenueNonCAT,
	ProviderNetRevenueNonCAT,
	BuyerRevenueNonCAT,
	TowCostNonCAT
	)	
SELECT	D.FiscalWeekID,
		DS.AdministrativeBranchNumber,
		SUM(CASE WHEN DS.IsStockSold = 1 THEN 1 ELSE 0 END) AS UnitSoldCount,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DS.AgreementTermTypeCode = 'PAA'  THEN 1 ELSE 0 END)  AS PAUnitSold ,
		SUM(CASE WHEN DS.IsStockSold = 1 THEN DS.SalePrice ELSE 0 END) AS TotalSalePrice,
		SUM(CASE WHEN DS.IsStockSold = 1 THEN ProviderTotalCharge ELSE 0 END) AS ProviderRevenue,
		SUM (CASE WHEN DS.IsStockSold = 1 AND DS.AgreementTermTypeCode = 'PAA' THEN ISNULL(DS.SalePrice,0) 
		      - ISNULL(DS.PAPrice,0) ELSE 0 END 
		     ) AS PARevenue,
		SUM (CASE WHEN DS.IsStockSold = 1 THEN 
				ISNULL((CASE WHEN DS.IsStockSold = 1 AND DS.AgreementTermTypeCode = 'PAA' THEN ISNULL(DS.SalePrice,0)
				- ISNULL(DS.PAPrice,0) ELSE NULL END),0) +  ISNULL((FSCE.ProviderTotalCharge),0) - ISNULL((FSCE.ProviderTotalExpense),0)
			END) AS ProviderNetReturn,     
		SUM(CASE WHEN DS.IsStockSold = 1 THEN FSCE.BuyerTotalCharge ELSE 0 END) AS BuyerRevenue,
		SUM(CASE WHEN DS.IsStockSold = 1 THEN FSCE.ProviderTowExpense ELSE 0 END ) AS TowCost,
		--NonCAT
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 THEN 1 ELSE 0 END) AS UnitSoldNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND DS.AgreementTermTypeCode = 'PAA'  THEN 1 ELSE 0 END)  AS PAUnitSoldNonCAT ,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 THEN DS.SalePrice ELSE 0 END) AS TotalSalePriceNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 THEN ProviderTotalCharge ELSE 0 END) AS ProviderRevenueNonCAT,
		SUM (CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND DS.AgreementTermTypeCode = 'PAA' THEN ISNULL(DS.SalePrice,0) 
		      - ISNULL(DS.PAPrice,0) ELSE 0 END 
		     ) AS PARevenueNonCAT,
		SUM (CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 THEN 
				ISNULL((CASE WHEN DS.IsStockSold = 1 AND DS.AgreementTermTypeCode = 'PAA' THEN ISNULL(DS.SalePrice,0)
				- ISNULL(DS.PAPrice,0) ELSE NULL END),0) +  ISNULL((FSCE.ProviderTotalCharge),0) - ISNULL((FSCE.ProviderTotalExpense),0)
			END) AS ProviderNetRevenueNonCAT,     
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 THEN FSCE.BuyerTotalCharge ELSE 0 END) AS BuyerRevenueNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 THEN FSCE.ProviderTowExpense ELSE 0 END ) AS TowCostNonCAT
FROM dbo.DimStock DS WITH (NOLOCK)
INNER JOIN dbo.FactStockCycleTime FSCT WITH (NOLOCK)
 ON DS.StockID = FSCT.StockID
INNER JOIN dbo.FactStockChargeExpense FSCE WITH (NOLOCK)
 ON DS.StockID = FSCE.StockID
INNER JOIN dbo.DimBranch DB WITH (NOLOCK)
 ON DS.AdministrativeBranchNumber = DB.BranchNumber
INNER JOIN #TMP_KPIDay D WITH(NOLOCK) 
 ON FSCT.SoldDayID  = D.DayID
GROUP BY D.FiscalWeekID, DS.AdministrativeBranchNumber	


INSERT INTO #TMP_MinimumSalesPrice
(	FiscalWeekID,
	AdministrativeBranchNumber,
	MinimumSalesPrice,
	MinimumSalesPriceNonCAT
)	
SELECT	D.FiscalWeekID,
		DS.AdministrativeBranchNumber,
		MIN(CASE WHEN DS.IsBulkSale = 0 AND DS.IsIBF = 0 AND DS.StockType = 1 AND DS.SalePrice < 1000 
			 AND DS.DamageCodePrimary NOT IN('TB','S') AND DB.RegionName NOT IN ('IAA Services') 
			THEN DS.SalePrice ELSE NULL END) AS MinimumSalesPrice,
		--NonCAT
		MIN(CASE WHEN DS.IsCAT = 0 AND DS.IsBulkSale = 0 AND DS.IsIBF = 0 AND DS.StockType = 1 AND DS.SalePrice < 1000 
			 AND DS.DamageCodePrimary NOT IN('TB','S') AND DB.RegionName NOT IN ('IAA Services') 
			THEN DS.SalePrice ELSE NULL END) AS MinimumSalesPriceNonCAT			
FROM dbo.DimStock DS WITH (NOLOCK)
INNER JOIN dbo.FactStockCycleTime FSCT WITH (NOLOCK)
 ON DS.StockID = FSCT.StockID
INNER JOIN dbo.DimBranch DB WITH (NOLOCK)
 ON DS.AdministrativeBranchNumber = DB.BranchNumber
INNER JOIN #TMP_KPIDay D WITH(NOLOCK) 
 ON  FSCT.SoldDayID  = D.DayID
 AND FSCT.AuctionDayID = D.DayID
GROUP BY D.FiscalWeekID, DS.AdministrativeBranchNumber	


INSERT INTO #TMP_SetForSale
(	FiscalWeekID,
	BranchNumber,
	UnitsSetForSale,
	UnitsSetForSaleNonCAT
	)	
SELECT	D.FiscalWeekID,
		DA.branchnumber,
		SUM(CASE WHEN IsSetForSale = 1 THEN 1 ELSE 0 END) AS UnitsSetForSale, 
		--NonCAT
		SUM(CASE WHEN DS.IsCAT = 0 AND IsSetForSale = 1 THEN 1 ELSE 0 END) AS UnitsSetForSaleNonCAT 
FROM dbo.DimAuctionItem DAI WITH (NOLOCK)
INNER JOIN  dbo.DimAuction DA WITH (NOLOCK)
 ON DAI.AuctionID = DA.AuctionID
INNER JOIN dbo.DimStock DS WITH(NOLOCK)
 ON DAI.StockID = DS.StockID 
INNER JOIN #TMP_KPIDay D WITH(NOLOCK)
 ON DA.AuctionDayID = D.DayID
GROUP BY D.fiscalweekid, DA.branchnumber
	

INSERT INTO #TMP_VolumeProceeds
(	FiscalWeekID,
	AdministrativeBranchNumber,
	InsuranceUnitsSold,
	InsuranceSalePrice,
	InsuranceACV,
	CharityUnitsSold,
	CharityTotalSalePrice,
	RentalUnitsSold,
	RentalTotalSalePrice,
	VRDUnitsSold,
	VRDTotalSalePrice,
	OtherUnitsSold,
	OtherTotalSalePrice,
	InsuranceGrossReturn,
	InsuranceUnitsSoldNonCAT,
	InsuranceSalePriceNonCAT,
	InsuranceACVNonCAT,
	CharityUnitsSoldNonCAT,
	CharityTotalSalePriceNonCAT,
	RentalUnitsSoldNonCAT,
	RentalTotalSalePriceNonCAT,
	VRDUnitsSoldNonCAT,
	VRDTotalSalePriceNonCAT,
	OtherUnitsSoldNonCAT,
	OtherTotalSalePriceNonCAT,
	InsuranceGrossReturnNonCAT
	)	
SELECT	D.FiscalWeekID,
		DS.AdministrativeBranchNumber,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DP.ProviderTypeCode IN ('INS','ADJ') THEN 1 ELSE 0 END) AS InsuranceUnitsSold,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DP.ProviderTypeCode IN ('INS','ADJ') THEN DS.SalePrice ELSE 0 END) AS InsuranceSalePrice,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DP.ProviderTypeCode IN ('INS','ADJ') AND DS.IsACVOutlier = 0 THEN DS.ActualCashValue ELSE 0 END) AS InsuranceACV,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DP.ProviderTypeCode = 'CHR' THEN 1 ELSE 0 END) AS CharityUnitsSold,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DP.ProviderTypeCode = 'CHR' THEN DS.SalePrice ELSE 0 END) AS CharityTotalSalePrice,
		--SUM(CASE WHEN DS.IsStockSold = 1 and (DP.ProviderGroupName='VRD Rental' OR DP.ProviderTypeCode = 'RCC') THEN 1 ELSE 0 END) RentalUnitsSold,
		--SUM(CASE WHEN DP.ProviderGroupName='VRD Rental' OR DP.ProviderTypeCode = 'RCC' THEN DS.SalePrice ELSE 0 END) RentalTotalSalePrice,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DP.ProviderGroupName IN ('VRD Avis', 'VRD Rental', 'VRD Enterprise') THEN 1 ELSE 0 END) RentalUnitsSold,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DP.ProviderGroupName IN ('VRD Avis', 'VRD Rental', 'VRD Enterprise') THEN DS.SalePrice ELSE 0 END) RentalTotalSalePrice,
		--SUM(CASE WHEN DS.IsStockSold = 1 AND ((DP.ProviderTypeCode IN ('SDS','FIN') AND DP.ProviderGroupName NOT IN('VRD Rental') AND DS.IsINS = 0) OR (DP.ProviderTypeCode = 'DLR' AND DS.IsINS = 0 )) THEN 1 ELSE 0 END) AS VRDUnitsSold,
		--SUM(CASE WHEN DS.IsStockSold = 1 AND ((DP.ProviderTypeCode IN ('SDS','FIN') AND DP.ProviderGroupName NOT IN('VRD Rental') AND DS.IsINS = 0) OR (DP.ProviderTypeCode = 'DLR' AND DS.IsINS = 0 )) THEN DS.SalePrice ELSE 0 END) AS VRDTotalSalePrice,
		SUM(CASE WHEN DS.IsStockSold = 1 AND 
		              ((ProviderGroupName IN('VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto','VRD SRG', 
											  'VRD Titlemax','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Auto Depot','VRD Title Loan'))
					  OR (DP.ProviderTypeCode = 'DLR' AND DS.IsINS = 0 ))THEN 1 ELSE 0 END) AS VRDUnitsSold,
		SUM(CASE WHEN DS.IsStockSold = 1 AND 
					  ((ProviderGroupName IN('VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto','VRD SRG', 
											  'VRD Titlemax','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Auto Depot','VRD Title Loan'))  
					  OR (DP.ProviderTypeCode = 'DLR' AND DS.IsINS = 0 )) THEN DS.SalePrice ELSE 0 END) AS VRDTotalSalePrice,
		--SUM(CASE WHEN DS.IsStockSold = 1 AND (DP.ProviderTypeCode IN('COR','GOV','PPT','SAL') OR DS.IsINS = 1) THEN 1 ELSE 0 END) AS OtherUnitsSold,
		SUM(CASE WHEN DS.IsStockSold = 1 
		AND DP.ProviderGroupName NOT IN ('VRD Avis', 'VRD Rental', 'VRD Enterprise') 
		AND DP.ProviderTypeCode NOT IN ('INS','ADJ')
		AND (ProviderGroupName NOT IN('VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto','VRD SRG', 
											  'VRD Titlemax','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Auto Depot','VRD Title Loan')
                     OR DP.ProviderTypeCode = 'DLR' AND DS.IsINS = 1
		     )
        THEN 1 ELSE 0 END) AS OtherUnitsSold,
     	--SUM(CASE WHEN DS.IsStockSold = 1 AND (DP.ProviderTypeCode IN('COR','GOV','PPT','SAL') OR DS.IsINS = 1) THEN DS.SalePrice ELSE 0 END) AS OtherTotalSalePrice,	
		SUM(CASE WHEN DS.IsStockSold = 1 
		AND DP.ProviderGroupName NOT IN ('VRD Avis', 'VRD Rental', 'VRD Enterprise') 
		AND DP.ProviderTypeCode NOT IN ('INS','ADJ')
		AND (ProviderGroupName NOT IN('VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto','VRD SRG', 
											  'VRD Titlemax','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Auto Depot','VRD Title Loan')
                     OR DP.ProviderTypeCode = 'DLR' AND DS.IsINS = 1
		     )		
		THEN DS.SalePrice ELSE 0 END) AS OtherTotalSalePrice,
		SUM(CASE WHEN DP.ProviderTypeCode IN ('INS','ADJ') THEN DS.GrossReturn ELSE 0 END) AS InsuranceGrossReturn,
		--NonCAT
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND DP.ProviderTypeCode IN ('INS','ADJ') THEN 1 ELSE 0 END) AS InsuranceUnitsSoldNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND DP.ProviderTypeCode IN ('INS','ADJ') THEN DS.SalePrice ELSE 0 END) AS InsuranceSalePriceNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND DP.ProviderTypeCode IN ('INS','ADJ') AND DS.IsACVOutlier = 0 THEN DS.ActualCashValue ELSE 0 END) AS InsuranceACVNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND DP.ProviderTypeCode = 'CHR' THEN 1 ELSE 0 END) AS CharityUnitsSoldNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DP.ProviderTypeCode = 'CHR' THEN DS.SalePrice ELSE 0 END) AS CharityTotalSalePriceNonCAT,
		--SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 and (DP.ProviderGroupName='VRD Rental' OR DP.ProviderTypeCode = 'RCC') THEN 1 ELSE 0 END) RentalUnitsSoldNonCAT,
		--SUM(CASE WHEN DS.IsCAT = 0 AND DP.ProviderGroupName='VRD Rental' OR DP.ProviderTypeCode = 'RCC' THEN DS.SalePrice ELSE 0 END) RentalTotalSalePriceNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND DP.ProviderGroupName IN ('VRD Avis', 'VRD Rental', 'VRD Enterprise') THEN 1 ELSE 0 END) RentalUnitsSoldNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND DP.ProviderGroupName IN ('VRD Avis', 'VRD Rental', 'VRD Enterprise') THEN DS.SalePrice ELSE 0 END) RentalTotalSalePriceNonCAT,
		
		--SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND ((DP.ProviderTypeCode IN ('SDS','FIN') AND DP.ProviderGroupName NOT IN('VRD Rental') AND DS.IsINS = 0) OR (DP.ProviderTypeCode = 'DLR' AND DS.IsINS = 0 )) THEN 1 ELSE 0 END) AS VRDUnitsSoldNonCAT,
		--SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND ((DP.ProviderTypeCode IN ('SDS','FIN') AND DP.ProviderGroupName NOT IN('VRD Rental') AND DS.IsINS = 0) OR (DP.ProviderTypeCode = 'DLR' AND DS.IsINS = 0 )) THEN DS.SalePrice ELSE 0 END) AS VRDTotalSalePriceNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND 
					 ((ProviderGroupName IN('VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto','VRD SRG', 
											  'VRD Titlemax','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Auto Depot','VRD Title Loan','Dealers')) 		
					 AND DS.IsINS = 0 ) THEN 1 ELSE 0 END) AS VRDUnitsSoldNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND 
					  ((ProviderGroupName IN('VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto','VRD SRG', 
											  'VRD Titlemax','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Auto Depot','VRD Title Loan','Dealers')) 					 
					 AND DS.IsINS = 0 ) THEN DS.SalePrice ELSE 0 END) AS VRDTotalSalePriceNonCAT,
		--SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND (DP.ProviderTypeCode IN('COR','GOV','PPT','SAL') OR DS.IsINS = 1) THEN 1 ELSE 0 END) AS OtherUnitsSoldNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 
		AND DP.ProviderGroupName NOT IN ('VRD Avis', 'VRD Rental', 'VRD Enterprise') 
		AND DP.ProviderTypeCode NOT IN ('INS','ADJ','CHR')
		AND ( DP.ProviderGroupName NOT IN ('VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto','VRD SRG', 
											  'VRD Titlemax','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Auto Depot','VRD Title Loan','Dealers')
				OR 	DS.IsINS = 1
		     ) 
		THEN 1 ELSE 0 END) AS OtherUnitsSoldNonCAT,		
		
		--SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND (DP.ProviderTypeCode IN('COR','GOV','PPT','SAL') OR DS.IsINS = 1) THEN DS.SalePrice ELSE 0 END) AS OtherTotalSalePriceNonCAT,			
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 
		AND DP.ProviderGroupName NOT IN ('VRD Avis', 'VRD Rental', 'VRD Enterprise') 
		AND DP.ProviderTypeCode NOT IN ('INS','ADJ','CHR')
		AND ( DP.ProviderGroupName NOT IN ('VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto','VRD SRG', 
											  'VRD Titlemax','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Auto Depot','VRD Title Loan','Dealers')
				OR 	DS.IsINS = 1
		     ) 
		THEN DS.SalePrice ELSE 0 END) AS OtherTotalSalePriceNonCAT,			
					
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND DP.ProviderTypeCode IN ('INS','ADJ') THEN DS.GrossReturn ELSE 0 END) AS InsuranceGrossReturnNonCAT
FROM dbo.DimStock DS WITH (NOLOCK) 
INNER JOIN dbo.FactStockCycleTime FSCT WITH (NOLOCK)
 ON DS.StockID = FSCT.StockID
INNER JOIN dbo.DimProvider DP WITH (NOLOCK) 
 ON DP.ProviderID = DS.CurrentProviderID
INNER JOIN #TMP_KPIDay D WITH (NOLOCK)
 ON FSCT.SoldDayID  = D.DayID
GROUP BY D.FiscalWeekID, DS.AdministrativeBranchNumber	
	
INSERT INTO #TMP_BidTypeSold
(	FiscalWeekID,
	AdministrativeBranchNumber,
	IBIDSoldUnits,
	ProxySoldUnits,
	IBFSoldUnits,
	OutOfCountrySoldUnits,
	OutOfStateSoldUnits,
	SuccessfulBuyer,
	IBIDSoldUnitsNonCAT,
	ProxySoldUnitsNonCAT,
	IBFSoldUnitsNonCAT,
	OutofCountrySoldUnitsNonCAT,
	OutofStateSoldUnitsNonCAT,
	SuccessfulBuyerNonCAT
	)	
SELECT	D.FiscalWeekID,
		DS.AdministrativeBranchNumber,
		SUM(CASE WHEN DS.IsIBIDWon = 1 THEN 1 ELSE 0 END) AS IBIDSoldUnits,
		SUM(CASE WHEN DS.IsProxyBidWon = 1 THEN 1 ELSE 0 END) AS ProxySoldUnits,
		SUM(CASE WHEN DS.IsIBFSold = 1 THEN 1 ELSE 0 END) AS IBFSoldUnits,
		SUM(CASE WHEN DS.IsStockSold = 1 AND (DB.CountryAbbreviation <> 'US' OR (DB.CountryAbbreviation='US' AND DB.StateAbbreviation IN ('GU','PR','VI')))
			THEN 1 ELSE 0 END) AS OutofCountrySoldUnits,
		SUM(CASE WHEN DS.IsStockSold = 1 AND (DB.CountryAbbreviation = 'US' AND (DB.StateAbbreviation <> DBr.StateAbbreviation)) 
			THEN 1 ELSE 0 END) AS OutofStateSoldUnits,
		COUNT(Distinct CASE WHEN DS.IsStockSold = 1 THEN (CurrentbuyerID) ELSE 0 END)AS SuccessfulBuyer,
		--NonCAT
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsIBIDWon = 1 THEN 1 ELSE 0 END) AS IBIDSoldUnitsNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsProxyBidWon = 1 THEN 1 ELSE 0 END) AS ProxySoldUnitsNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsIBFSold = 1 THEN 1 ELSE 0 END) AS IBFSoldUnitsNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND (DB.CountryAbbreviation <> 'US' OR (DB.CountryAbbreviation='US' AND DB.StateAbbreviation IN ('GU','PR','VI')))
			THEN 1 ELSE 0 END) AS OutofCountrySoldUnitsNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND (DB.CountryAbbreviation = 'US' AND (DB.StateAbbreviation <> DBr.StateAbbreviation)) 
			THEN 1 ELSE 0 END) AS OutofStateSoldUnitsNonCAT,
		COUNT(Distinct CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 THEN (CurrentbuyerID) ELSE 0 END)AS SuccessfulBuyerNonCAT
FROM dbo.DimStock DS WITH (NOLOCK) 
INNER JOIN dbo.FactStockCycleTime FSCT WITH (NOLOCK)
 ON DS.StockID = FSCT.StockID
INNER JOIN dbo.DimBuyer DB WITH (NOLOCK) 
 ON DS.CurrentBuyerID = DB.BuyerID
INNER JOIN dbo.DimBranch DBr WITH (NOLOCK) 
 ON DS.AdministrativeBranchNumber = DBr.BranchNumber
INNER JOIN #TMP_KPIDay D WITH (NOLOCK)
 ON FSCT.SoldDayID = D.DayID
GROUP BY D.FiscalWeekID, DS.AdministrativeBranchNumber	
	
INSERT INTO #TMP_BuyerAttendance
(	FiscalWeekID,
	BranchNumber,
	BuyerAttendance,
	OutofCountryBuyerAttendance,
	GuestAttendance,
	BuyerAttendanceNonCAT,
	OutofCountryBuyerAttendanceNonCAT,
	GuestAttendanceNonCAT
	)	
SELECT	D.FiscalWeekID,
		DA.Branchnumber,
		SUM(FBA.BuyerAttendance) AS BuyerAttendance,
		SUM(FBA.OutOfCountryBuyerAttendance) AS OutofCountryBuyerAttendance,
		SUM(FBA.GuestAttendance) GuestAttendance,
		--NonCAT
		SUM(FBA.BuyerAttendance) AS BuyerAttendanceNonCAT,
		SUM(FBA.OutOfCountryBuyerAttendance) AS OutofCountryBuyerAttendanceNonCAT,
		SUM(FBA.GuestAttendance) GuestAttendanceNonCAT
FROM dbo.FactBuyerAttendance FBA WITH (NOLOCK)  
INNER JOIN dbo.DimAuction DA WITH (NOLOCK) 
 ON FBA.AuctionID = DA.AuctionID 
 AND FBA.BranchNumber = DA.BranchNumber
INNER JOIN #TMP_KPIDay D with (nolock)
 ON DA.AuctionDayID  = D.DayID
GROUP BY FiscalWeekID, DA.BranchNumber	

INSERT INTO #TMP_CountofPUwithIn100MilesOneBusinessDays
(	FiscalWeekID,
	BranchNumber,
	CountofPUwithIn100MilesOneBusinessDays,
	CountofPUwithIn100MilesOneBusinessDaysNonCAT
	)	
SELECT	D.FiscalWeekID,
		FST.BranchNumber,
		COUNT(FST.TowBillNumber) AS NumberofPickups,
		--NonCAT
		COUNT(CASE WHEN DS.IsCAT = 0 THEN FST.TowBillNumber ELSE 0 END) AS NumberofPickups
FROM  dbo.FactStockTow FST WITH(NOLOCK)   
INNER JOIN dbo.FactStockTowCycleTime FSC WITH(NOLOCK)  
 ON FSC.TowBillNumber  = FST.TowBillNumber
INNER JOIN #TMP_KPIDay D WITH(NOLOCK)  
 ON FSC.TowCompletedDayID = D.DayID
INNER JOIN dbo.DimStock DS WITH (NOLOCK)
 ON FST.StockID = DS.StockID
WHERE FST.TowTypeCode = 'PK'
AND FST.IsTowSuccessful = 1
AND DS.IsDeliveredToBranch = 0
AND FST.TowMiles <= 100 
GROUP BY D.FiscalWeekID, FST.BranchNumber


INSERT INTO #TMP_Sold_ACV
(	FiscalWeekID,
	BranchNumber,
	BlackbookACV,
	ACVNonCAT
	)
SELECT  D.FiscalWeekID,
		DS.Administrativebranchnumber,
		SUM(DS.BlackbookACV) AS BlackbookACV,
		--NonCAT
		SUM(CASE WHEN DS.IsCAT = 0 THEN DS.BlackbookACV ELSE 0 END) AS ACVNonCAT
FROM	dbo.DimStock DS WITH(NOLOCK)
INNER JOIN dbo.FactStockCycleTime FSCT WITH(NOLOCK)
 ON DS.StockID = FSCT.StockID
INNER JOIN #TMP_KPIDay D WITH(NOLOCK)
 ON FSCT.SoldDayID = D.DayID
GROUP BY D.FiscalWeekID,
		 DS.Administrativebranchnumber

INSERT INTO #TMP_SoldVehicleAtrributes
(	FiscalWeekID,
	BranchNumber,
	Keys,
	RunAndDriveAutoOnly,
	CarStart,
	Mileage,
	Enhancement,
	ReRun,
	VRDKeys,
	VRDRunAndDriveAutoOnly,
	VRDCarStart,
	VRDMileage,
	VRDEnhancement,
	VRDReRun,
	UnitsSoldsWithKeyNonCAT,
	UnitsSoldsWithKeyOnly,
	UnitsSoldRunAndDriveAutoOnlyNonCAT,
	UnitsSoldCarStartNonCAT,
	UnitsSoldMileageNonCAT,
	UnitsSoldEnhancementNonCAT,
	UnitsSoldReRunNonCAT,
	VRDUnitsSoldKeyNonCAT,
	VRDUnitsSoldRunAndDriveAutoOnlyNonCAT,
	VRDUnitsSoldCarStartNonCAT,
	VRDUnitsSoldMileageNonCAT,
	VRDUnitsSoldEnhancementNonCAT,
	VRDReRunNonCAT,
	VRDUnitsSoldKeyOnlyNonCAT
	)
SELECT  D.FiscalWeekID,
		DS.Administrativebranchnumber,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DS.IsSoldKeysApplicable = 1 THEN 1 ELSE 0 END ) AS Keys,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DS.IsRunAndDriveAuction = 1 AND DS.StockType = 1 THEN 1 ELSE 0 END ) AS RunAndDriveAutoOnly,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DS.EngineStatusCode IN('SWJ', 'CST') THEN 1 ELSE 0 END ) AS CarStart,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DS.Mileage >= 1 AND DS.Mileage <= 299999 AND DS.OdometerReadingTypeCode IN(0,6) AND DS.StockType IN (1,2) THEN 1 ELSE 0 END) AS Mileage,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DS.IsEnhancement = 1 THEN 1 ELSE 0 END ) AS Enhancement,
		SUM(CASE WHEN DS.IsStockSold = 1 and  vds.reruns > 0 then 1  ELSE 0 END ) AS ReRun,
		--VRD Fields
		/*
		SUM(CASE WHEN DS.IsStockSold = 1 AND DP.Providertypecode  IN ('DLR','SDS','RCC','FIN' ) AND DS.IsSoldKeysApplicable = 1 THEN 1 ELSE 0 END ) AS VRDKeys,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DP.Providertypecode  IN ('DLR','SDS','RCC','FIN' ) AND DS.IsRunAndDriveAuction = 1 AND DS.StockType = 1 THEN 1 ELSE 0 END ) AS VRDRunAndDriveAutoOnly,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DP.Providertypecode  IN ('DLR','SDS','RCC','FIN' ) AND DS.EngineStatusCode IN('SWJ', 'CST') THEN 1 ELSE 0 END ) AS VRDCarStart,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DP.Providertypecode  IN ('DLR','SDS','RCC','FIN' ) AND DS.Mileage >= 1 AND DS.Mileage <= 299999 AND DS.OdometerReadingTypeCode IN(0,6) AND DS.StockType IN (1,2) THEN DS.Mileage ELSE 0 END ) AS VRDMileage,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DP.Providertypecode  IN ('DLR','SDS','RCC','FIN' ) AND DS.IsEnhancement = 1 THEN 1 ELSE 0 END ) AS VRDEnhancement,
		SUM(CASE WHEN DS.IsStockSold = 1 AND ((DP.ProviderTypeCode IN ('SDS','FIN') AND DP.ProviderGroupName NOT IN('VRD Rental') AND DS.IsINS = 0) OR (DP.ProviderTypeCode = 'DLR' AND DS.IsINS = 0 ))  and  vds.reruns > 0  then 1  ELSE 0 END ) AS VRDReRun,
		*/
		SUM(CASE WHEN DS.IsStockSold = 1 AND 
					  ((DP.ProviderGroupName IN ('VRD Avis','VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto',
                                                 'VRD Rental','VRD SRG','VRD Titlemax','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Enterprise'))
					    OR (DP.ProviderGroupName = 'Dealers' AND DS.IsINS = 0 )) 
					AND DS.IsSoldKeysApplicable = 1 THEN 1 ELSE 0 END ) AS VRDKeys,
		SUM(CASE WHEN DS.IsStockSold = 1 AND 
				      ((DP.ProviderGroupName IN ('VRD Avis','VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto',
                                                 'VRD Rental','VRD SRG','VRD Titlemax','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Enterprise'))
					    OR (DP.ProviderGroupName = 'Dealers' AND DS.IsINS = 0 )) 
					  AND  DS.IsRunAndDriveAuction = 1 AND DS.StockType = 1 THEN 1 ELSE 0 END ) AS VRDRunAndDriveAutoOnly,
		SUM(CASE WHEN DS.IsStockSold = 1 AND 
					  ((DP.ProviderGroupName IN ('VRD Avis','VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto',
                                                 'VRD Rental','VRD SRG','VRD Titlemax','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Enterprise'))
					    OR (DP.ProviderGroupName = 'Dealers' AND DS.IsINS = 0 )) 
					  AND DS.EngineStatusCode IN('SWJ', 'CST') THEN 1 ELSE 0 END ) AS VRDCarStart,
		SUM(CASE WHEN DS.IsStockSold = 1 AND 
					  ((DP.ProviderGroupName IN ('VRD Avis','VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto',
                                                 'VRD Rental','VRD SRG','VRD Titlemax','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Enterprise'))
					    OR (DP.ProviderGroupName = 'Dealers' AND DS.IsINS = 0 )) 
					  AND DS.Mileage >= 1 AND DS.Mileage <= 299999 AND DS.OdometerReadingTypeCode IN(0,6) AND DS.StockType IN (1,2) THEN DS.Mileage ELSE 0 END ) AS VRDMileage,
		SUM(CASE WHEN DS.IsStockSold = 1 AND
					  ((DP.ProviderGroupName IN ('VRD Avis','VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto',
                                                 'VRD Rental','VRD SRG','VRD Titlemax','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Enterprise'))
					    OR (DP.ProviderGroupName = 'Dealers' AND DS.IsINS = 0 )) 
					  AND DS.IsEnhancement = 1 THEN 1 ELSE 0 END ) AS VRDEnhancement,
		SUM(CASE WHEN DS.IsStockSold = 1 AND 
					  ((DP.ProviderGroupName IN ('VRD Avis','VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto',
                                                 'VRD Rental','VRD SRG','VRD Titlemax','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Enterprise'))
					    OR (DP.ProviderGroupName = 'Dealers' AND DS.IsINS = 0 )) 
					AND vds.reruns > 0  then 1  ELSE 0 END ) AS VRDReRun,

		--NonCAT ----------------------------------------------------------------------------------------------------------------------------------
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND DS.IsSoldKeysApplicable = 1 THEN 1 ELSE 0 END ) AS UnitsSoldsWithKeyNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND DS.KeysRetainedCodeAtSale = 1 THEN 1 ELSE 0 END )  UnitsSoldsWithKeyOnly,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND DS.IsRunAndDriveAuction = 1 AND DS.StockType = 1 THEN 1 ELSE 0 END ) AS UnitsSoldRunAndDriveAutoOnlyNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND DS.EngineStatusCode IN('SWJ', 'CST') THEN 1 ELSE 0 END ) AS UnitsSoldCarStartNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND DS.Mileage >= 1 AND DS.Mileage <= 299999 AND DS.OdometerReadingTypeCode IN(0,6) AND DS.StockType IN (1,2) THEN 1 ELSE 0 END) AS UnitsSoldMileageNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND DS.IsEnhancement = 1 THEN 1 ELSE 0 END ) AS UnitsSoldEnhancementNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND  vds.reruns > 0 then 1  ELSE 0 END )  AS UnitsSoldReRunNonCAT,
		--VRD Fields
		/*
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND ((DP.ProviderTypeCode IN ('SDS','FIN') AND DP.ProviderGroupName NOT IN('VRD Rental') AND DS.IsINS = 0) OR (DP.ProviderTypeCode = 'DLR' AND DS.IsINS = 0 ))  AND DS.IsSoldKeysApplicable = 1 THEN 1 ELSE 0 END ) AS VRDUnitsSoldKeyNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND ((DP.ProviderTypeCode IN ('SDS','FIN') AND DP.ProviderGroupName NOT IN('VRD Rental') AND DS.IsINS = 0) OR (DP.ProviderTypeCode = 'DLR' AND DS.IsINS = 0 ))  AND DS.IsRunAndDriveAuction = 1 AND DS.StockType = 1 THEN 1 ELSE 0 END ) AS VRDUnitsSoldRunAndDriveAutoOnlyNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND ((DP.ProviderTypeCode IN ('SDS','FIN') AND DP.ProviderGroupName NOT IN('VRD Rental') AND DS.IsINS = 0) OR (DP.ProviderTypeCode = 'DLR' AND DS.IsINS = 0 ))  AND DS.EngineStatusCode IN('SWJ', 'CST') THEN 1 ELSE 0 END ) AS VRDUnitsSoldCarStartNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND DP.Providertypecode  IN ('DLR','SDS','RCC','FIN' ) AND DS.Mileage >= 1 AND DS.Mileage <= 299999 AND DS.OdometerReadingTypeCode IN(0,6) AND DS.StockType IN (1,2) THEN 1 ELSE 0 END ) AS VRDUnitsSoldMileageNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND ((DP.ProviderTypeCode IN ('SDS','FIN') AND DP.ProviderGroupName NOT IN('VRD Rental') AND DS.IsINS = 0) OR (DP.ProviderTypeCode = 'DLR' AND DS.IsINS = 0 ))  AND DS.IsEnhancement = 1 THEN 1 ELSE 0 END ) AS VRDUnitsSoldEnhancementNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND ((DP.ProviderTypeCode IN ('SDS','FIN') AND DP.ProviderGroupName NOT IN('VRD Rental') AND DS.IsINS = 0) OR (DP.ProviderTypeCode = 'DLR' AND DS.IsINS = 0 ))  and  vds.reruns > 0 then 1 ELSE 0 END ) AS VRDReRunNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND ((DP.ProviderTypeCode IN ('SDS','FIN') AND DP.ProviderGroupName NOT IN('VRD Rental') AND DS.IsINS = 0) OR (DP.ProviderTypeCode = 'DLR' AND DS.IsINS = 0 ))  AND DS.KeysRetainedCodeAtSale = 1 THEN 1 ELSE 0 END ) AS VRDUnitsSoldKeyOnlyNonCAT
		*/
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND 
		              ((DP.ProviderGroupName IN ('VRD Avis','VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto',
                                                 'VRD Rental','VRD SRG','VRD Titlemax','VRD Title loan','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Enterprise','Dealers'))
					    AND DS.IsINS = 0 )  
					  AND DS.IsSoldKeysApplicable = 1 THEN 1 ELSE 0 END ) AS VRDUnitsSoldKeyNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND 
		              ((DP.ProviderGroupName IN ('VRD Avis','VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto',
                                              'VRD Rental','VRD SRG','VRD Titlemax','VRD Title loan','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Enterprise','Dealers')) 
					    AND DS.IsINS = 0 ) AND 
					  DS.IsRunAndDriveAuction = 1 AND DS.StockType = 1 THEN 1 ELSE 0 END ) AS VRDUnitsSoldRunAndDriveAutoOnlyNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND 
		              ((DP.ProviderGroupName IN ('VRD Avis','VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto',
                                                 'VRD Rental','VRD SRG','VRD Titlemax','VRD Title loan','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Enterprise','Dealers')) 
					    AND DS.IsINS = 0 )  
					  AND DS.EngineStatusCode IN('SWJ', 'CST') THEN 1 ELSE 0 END ) AS VRDUnitsSoldCarStartNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND 
		              ((DP.ProviderGroupName IN ('VRD Avis','VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto',
                                               'VRD Rental','VRD SRG','VRD Titlemax','VRD Title loan','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Enterprise','Dealers'))  
					    AND DS.IsINS = 0 )
					  AND DS.Mileage >= 1 AND DS.Mileage <= 299999 AND DS.OdometerReadingTypeCode IN(0,6) AND 
					  DS.StockType IN (1,2) THEN 1 ELSE 0 END ) AS VRDUnitsSoldMileageNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND 
					( DS.IsBandageEnhancement =1 
                                     OR DS.IsDetailEnhancement = 1 OR DS.IsFloodServiceEnhancement = 1 OR DS.IsFullWrapEnhancement = 1
                                     OR DS.IsPatchEnhancement = 1  OR DS.IsKeyEnhancment = 1)
					AND
		              ((DP.ProviderGroupName IN ('VRD Avis','VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto',
                                                 'VRD Rental','VRD SRG','VRD Titlemax','VRD Title loan','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Enterprise','Dealers')) 
					    AND DS.IsINS = 0 )  
					  THEN 1 ELSE 0 END ) AS VRDUnitsSoldEnhancementNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND 
		              ((DP.ProviderGroupName IN ('VRD Avis','VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto',
                                                 'VRD Rental','VRD SRG','VRD Titlemax','VRD Title loan','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Enterprise','Dealers'))
					    AND DS.IsINS = 0 )  
					  AND vds.reruns > 0 then 1 ELSE 0 END ) AS VRDReRunNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND 
		              ((DP.ProviderGroupName IN ('VRD Avis','VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto',
                                                 'VRD Rental','VRD SRG','VRD Titlemax','VRD Title loan','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Enterprise','Dealers')) 
					    AND DS.IsINS = 0 )  
					  AND DS.KeysRetainedCodeAtSale = 1 THEN 1 ELSE 0 END ) AS VRDUnitsSoldKeyOnlyNonCAT
FROM dbo.DimStock DS WITH(NOLOCK) 
INNER JOIN dbo.FactStockCycleTime FSCT WITH(NOLOCK)
 ON DS.StockID = FSCT.StockID
INNER JOIN dbo.DimProvider DP WITH(NOLOCK) 
 ON DP.ProviderID = DS.CurrentProviderID
INNER JOIN #TMP_KPIDay D WITH(NOLOCK)
 ON FSCT.SoldDayID  = D.DayID
INNER JOIN v_DimStock vds
ON DS.StockID = vds.StockID
GROUP BY D.FiscalWeekid,
		 DS.Administrativebranchnumber	
			
INSERT INTO #TMP_IBF
( FiscalWeekID,
  BranchNumber,
  IBFUnitsSold,
  IBFSalePrice,
  IBFNextHighestHistoricalBid,
  IBFSoldUnitsNonCAT,
  IBFSalePriceNonCAT,
  IBFNextHighestHistoricalBidNonCAT  
)
SELECT  D.FiscalWeekID,
		DS.Administrativebranchnumber,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DS.IsIBFSold = 1 THEN 1 ELSE 0 END ) AS IBFUnitsSold,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DS.IsIBFSold = 1 THEN DS.SalePrice ELSE 0 END) AS IBFSalePrice,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DS.IsIBFSold = 1 THEN DS.NextHighestHistoricalBid ELSE 0 END) AS IBFNextHighestHistoricalBid,
		--NonCAT
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND DS.IsIBFSold = 1 THEN 1 ELSE 0 END ) AS IBFSoldUnitsNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND DS.IsIBFSold = 1 THEN DS.SalePrice ELSE 0 END) AS IBFSalePriceNonCAT,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND DS.IsIBFSold = 1 THEN DS.NextHighestHistoricalBid ELSE 0 END) AS IBFNextHighestHistoricalBidNonCAT		
FROM dbo.DimStock DS WITH(NOLOCK) 
INNER JOIN dbo.FactStockCycleTime FSCT WITH(NOLOCK)
 ON DS.StockID = FSCT.StockID
INNER JOIN #TMP_KPIDay D WITH(NOLOCK)
 ON FSCT.SoldDayID  = D.DayID
GROUP BY D.FiscalWeekID,
		 DS.Administrativebranchnumber


INSERT INTO #TMP_BuyerMetrics
(	FiscalWeekID,
	BranchNumber,
	PublicUnitsSold,
	UnitsSoldPublicNonCAT
	)
SELECT  D.FiscalWeekID,
		DS.Administrativebranchnumber,
		SUM(CASE WHEN DS.IsStockSold = 1 AND db.BuyerTypeCode = 'P' THEN 1 ELSE 0 END) AS PublicUnitsSold,
		--NonCAT
	    SUM(CASE WHEN DS.IsCAT = 0 AND  DS.IsStockSold = 1 AND db.BuyerTypeCode = 'P' THEN 1 ELSE 0 END) AS UnitsSoldPublicNonCAT
FROM   dbo.DimStock DS WITH(NOLOCK) 
INNER JOIN dbo.FactStockCycleTime FSCT WITH(NOLOCK)
 ON DS.StockID = FSCT.StockID
INNER JOIN dbo.DimBuyer DB WITH(NOLOCK) 
 ON DS.CurrentBuyerID = DB.BuyerID
INNER JOIN dbo.DimBranch DBR WITH (NOLOCK) 
 ON DS.AdministrativeBranchNumber = DBR.BranchNumber
INNER JOIN #TMP_KPIDay D with (nolock)
 ON FSCT.SoldDayID  = D.DayID
GROUP BY D.FiscalWeekID,
		 DS.Administrativebranchnumber
	 
INSERT INTO #TMP_CalendarDaysAssignmentToRelease
(	FiscalWeekID,
	BranchNumber,
	CalendarDaysAssignmentToRelease,
	CalendarDaysAssignmenttoReleaseNonCAT
	)			 
		 
SELECT	D.FiscalWeekID,
		FST.BranchNumber,
		SUM(FSCT.CalendarAssignmentToRelease) AS CalendarDaysAssignmentToRelease,
		--NonCAT
		SUM(CASE WHEN DS.IsCAT = 0 THEN FSCT.CalendarAssignmentToRelease ELSE 0 END) AS CalendarDaysAssignmenttoReleaseNonCAT
FROM  dbo.FactStockTow FST WITH(NOLOCK)   
INNER JOIN dbo.FactStockTowCycleTime FSC WITH(NOLOCK)  
ON FSC.TowBillNumber  = FST.TowBillNumber
INNER JOIN #TMP_KPIDay D WITH(NOLOCK)  
ON FSC.TowCompletedDayID = D.DayID
INNER JOIN dbo.DimStock DS WITH (NOLOCK)
 ON FST.StockID = DS.StockID
INNER JOIN dbo.FactStockCycleTime FSCT WITH(NOLOCK)
 ON DS.StockID = FSCT.StockID
WHERE FST.TowTypeCode = 'PK'
AND FST.IsTowSuccessful = 1
AND DS.IsDeliveredToBranch = 0
GROUP BY D.FiscalWeekID, FST.BranchNumber		 


INSERT INTO #TMP_PushedByPublic
(	FiscalWeekID,
	BranchNumber,
	PushedByPublic,
	UnitsSoldPushedByPublicNonCAT
	)
SELECT FiscalWeekID,
       AdministrativeBranchNumber,
      COUNT (distinct fullstocknumber ) AS  PushedByPublic,
      --NonCAT
     COUNT(Distinct (CASE WHEN DS.IsCAT = 0 THEN (DS.StockID) ELSE 0 END)) AS UnitsSoldPushedByPublicNonCAT
FROM   dbo.DimBidDetail BDF WITH (NOLOCK)
INNER JOIN dbo.dimauctionitem fbs with (nolock) 
 ON fbs.auctionitemid = bdf.auctionitemid
INNER JOIN dbo.dimbuyer db with (nolock) 
 ON bdf.BuyerID = db.BuyerID 
 and bidstatuscode <>'S'
INNER JOIN dbo.dimstock ds with (nolock) 
 ON ds.stockid = fbs.stockid 
INNER JOIN dbo.dimbuyer dbs with (nolock) 
 ON dbs.buyerid = ds.currentbuyerid 
INNER JOIN  dbo.FactStockCycleTime  fsc with (nolock) 
 ON fsc.stockid = ds.stockid
INNER JOIN dbo.dimbranch dib with (nolock) 
 ON dib.branchnumber = ds.AdministrativeBranchNumber
INNER JOIN #TMP_KPIDay D WITH(NOLOCK) 
 ON d.dayid = fsc.solddayid
WHERE  BDF.BidderType IS NOT NULL 
AND IsBidDetailShow = 1 
AND dbs.buyertypecode = 'B' 
AND   db.buyertypecode = 'P' 
AND bidamount/finalbid > .80
AND finalbid > 0 
GROUP BY FiscalWeekID,
		 AdministrativeBranchNumber
OPTION(MAXDOP 1)


INSERT INTO #TMP_UnitsWrappedWithin7DaysOfCheckIn
(	FiscalWeekID,
	BranchNumber,
	UnitsWrappedWithin7DaysOfCheckIn,
	UnitsWrappedWithin7DaysOfCheckInNonCAT
	)	

SELECT  D.FiscalWeekID,
		DS.Administrativebranchnumber,
		SUM(CASE WHEN DATEDIFF(d,FSCT.CheckInDateTime, FFT.TransactionDateTime) BETWEEN 0 AND 7 THEN 1 ELSE 0 END) AS UnitsWrappedWithin7DaysOfCheckIn,
		--NonCAT
		SUM(CASE WHEN DS.IsCAT = 0 AND DATEDIFF(d,FSCT.CheckInDateTime, FFT.TransactionDateTime) BETWEEN 0 AND 7 THEN 1 ELSE 0 END) AS UnitsWrappedWithin7DaysOfCheckInNonCAT
		
FROM dbo.FactFinancialTransaction FFT WITH(NOLOCK)
INNER JOIN #TMP_KPIDay D WITH(NOLOCK)
 ON FFT.TransactionDayID = D.DayID
 AND FFT.FinancialtransactionTypeId IN (270,271,272)
INNER JOIN  dbo.DimStock DS WITH(NOLOCK) 
 ON DS.StockID = FFT.StockID
INNER JOIN dbo.FactStockCycleTime FSCT WITH(NOLOCK)
 ON DS.StockID = FSCT.StockID
GROUP BY D.FiscalWeekID,
		 DS.Administrativebranchnumber
		 
INSERT INTO #TMP_PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApproval
(	FiscalWeekID,
	BranchNumber,
	PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApproval,
	PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApprovalNonCAT
	)			 
SELECT D.FiscalWeekID,
	   DS.Administrativebranchnumber,
	   SUM(CASE WHEN DATEDIFF(dd, fsct.CurrentProcessStatusStartDate,@CurrentRunDateTime)> 30 THEN 1 ELSE 0 END) AS PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApproval,
	   SUM(CASE WHEN DS.IsCAT = 0 AND DATEDIFF(dd, fsct.CurrentProcessStatusStartDate,@CurrentRunDateTime)> 30 THEN 1 ELSE 0 END) AS PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApprovalNonCAT
FROM dbo.DimStock DS WITH(NOLOCK)
INNER JOIN dbo.FactStockCycleTime FSCT WITH(NOLOCK)
 ON DS.StockID = FSCT.StockID
INNER JOIN #TMP_KPIDay D 
 ON D.DayID = @CurrentRunDayID
WHERE  DS.ProcessStatusCode = 'J42'
GROUP BY D.FiscalWeekID,
	     DS.Administrativebranchnumber
OPTION(MAXDOP 1)
		
INSERT INTO #TMP_BuyerRevnue
        ( FiscalWeekID ,
        BranchNumber ,		
		BuyerRevenueNonCAT  ,
		BuyerRevenue  )	
SELECT	FiscalWeekID,
        AdministrativeBranchNumber BranchNumber,		
		SUM(CASE WHEN DS.IsCAT = 0  and DS.IsStockSold = 1 THEN FSCE.BuyerTotalCharge 
		ELSE 0 END) AS BuyerRevenueNonCAT,
		SUM(CASE WHEN  DS.IsStockSold = 1 THEN FSCE.BuyerTotalCharge 
		ELSE 0 END)  BuyerRevenue	
FROM dbo.DimStock DS WITH (NOLOCK)
INNER JOIN dbo.FactStockCycleTime FSCT WITH (NOLOCK)
 ON DS.StockID = FSCT.StockID
INNER JOIN dbo.FactStockChargeExpense FSCE WITH (NOLOCK)
 ON DS.StockID = FSCE.StockID
INNER JOIN dbo.DimBranch DB WITH (NOLOCK)
 ON DS.AdministrativeBranchNumber = DB.BranchNumber
INNER JOIN DimDay D WITH(NOLOCK) 
 ON FSCT.SoldDayID  = D.DayID
where d.FiscalWeekID  between @3weeksbackWeekID and @CurrentRunWeekID
GROUP BY FiscalWeekID,DS.AdministrativeBranchNumber		

INSERT INTO #TMP_WeekendPickups
	(	FiscalWeekID,
		BranchNumber,
		NumberOfWeekendPickups,
		NumberOfWeekendPickupsNonCAT
	)
SELECT	D.FiscalWeekID,
		FST.BranchNumber,
		SUM(CASE WHEN (D.WeekEndFlag = 1) THEN 1 ELSE 0 END) AS NumberOfWeekendPickups,	
		--NonCAT
		SUM(CASE WHEN (DS.IsCAT = 0 AND D.WeekEndFlag = 1) THEN 1 ELSE 0 END) AS NumberOfWeekendPickupsNonCAT	
FROM dbo.FactStockTow FST WITH (NOLOCK)
INNER JOIN dbo.FactStockTowCycleTime FSTCT WITH (NOLOCK)
 ON FST.StockID = FSTCT.StockID
 AND FST.TowBillNumber = FSTCT.TowBillNumber
INNER JOIN dbo.DimDay D WITH(NOLOCK)
 ON FSTCT.TowCompletedDayID = D.DayID
INNER JOIN dbo.DimStock DS WITH (NOLOCK)
 ON FST.StockID = DS.StockID
WHERE D.FiscalWeekID BETWEEN @3weeksbackWeekID and @CurrentRunWeekID
AND FST.TowTypeCode ='PK'
AND FST.IsTowSuccessful = 1
AND DS.IsDeliveredToBranch = 0
GROUP BY D.FiscalWeekID,
		FST.BranchNumber	



insert into #TMP_KPIDayForLast4Weeks
(	
[DayID] ,
[FiscalWeekID] 
)
SELECT	
[DayID],	
[FiscalWeekID]	
FROM dbo.DimDay  a WITH (NOLOCK)
where FiscalWeekID between @Last4thRunWeekID and @CurrentRunWeekID


INSERT INTO #TMP_SoldVehicleAtrributes_RunAndDrive
(	FiscalWeekID,
	BranchNumber,
	RunAndDriveAutoOnly,
	VRDRunAndDriveAutoOnly,
	UnitsSoldRunAndDriveAutoOnlyNonCAT,
	VRDUnitsSoldRunAndDriveAutoOnlyNonCAT
	
)
SELECT  D.FiscalWeekID,
		DS.Administrativebranchnumber,
		SUM(CASE WHEN DS.IsStockSold = 1 AND DS.IsRunAndDriveAuction = 1 AND DS.StockType = 1 THEN 1 ELSE 0 END ) AS RunAndDriveAutoOnly,

		SUM(CASE WHEN DS.IsStockSold = 1 AND 
				      ((DP.ProviderGroupName IN ('VRD Avis','VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto',
                                                 'VRD Rental','VRD SRG','VRD Titlemax','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Enterprise'))
					    OR (DP.ProviderGroupName = 'Dealers' AND DS.IsINS = 0 )) 
					  AND  DS.IsRunAndDriveAuction = 1 AND DS.StockType = 1 THEN 1 ELSE 0 END ) AS VRDRunAndDriveAutoOnly,

		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND DS.IsRunAndDriveAuction = 1 AND DS.StockType = 1 THEN 1 ELSE 0 END ) AS UnitsSoldRunAndDriveAutoOnlyNonCAT,
		
		
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND 
		              ((DP.ProviderGroupName IN ('VRD Avis','VRD Chase Auto Fin','VRD Commercial','VRD Commercial Adv','VRD Government','VRD Hoke','VRD I.V. Auto',
                                              'VRD Rental','VRD SRG','VRD Titlemax','VRD Title loan','VRD Tow Association','VRD Towing','VRD Wholesale Dealer','VRD World Omni Group','VRD Enterprise','Dealers')) 
					    AND DS.IsINS = 0 ) AND 
					  DS.IsRunAndDriveAuction = 1 AND DS.StockType = 1 THEN 1 ELSE 0 END ) AS VRDUnitsSoldRunAndDriveAutoOnlyNonCAT
FROM dbo.DimStock DS WITH(NOLOCK) 
INNER JOIN dbo.FactStockCycleTime FSCT WITH(NOLOCK)
 ON DS.StockID = FSCT.StockID
INNER JOIN dbo.DimProvider DP WITH(NOLOCK) 
 ON DP.ProviderID = DS.CurrentProviderID
INNER JOIN #TMP_KPIDayForLast4Weeks D WITH(NOLOCK)
 ON FSCT.SoldDayID  = D.DayID
INNER JOIN v_DimStock vds
ON DS.StockID = vds.StockID
GROUP BY D.FiscalWeekid,
		 DS.Administrativebranchnumber	


		
UPDATE #TMP_KPI_Branch 
SET UnitsAssigned = ISNULL(A.UnitsAssigned,0),
    UnitsNetAssigned = ISNULL(A.UnitsNetAssigned,0),
    ElectronicAssignments = ISNULL(A.ElectronicAssignments,0),
    CancelledAssignments = ISNULL(A.CancelledAssignments,0),
    CancelledAssignmentsNonCAT = ISNULL(A.CancelledAssignmentsNonCAT,0),
	UnitsAssignedNonCAT = ISNULL(A.UnitsAssignedNonCAT,0),
	UnitsNetAssignedNonCAT = ISNULL(A.UnitsNetAssignedNonCAT,0),
	ElectronicAssignmentsNonCAT = ISNULL(A.ElectronicAssignmentsNonCAT,0),
    UnitsAssignedPlan  = ISNULL(B.PlannedUnitsAssigned,0),
    UnitSoldPlan = ISNULL(B.PlannedUnitsSold,0),
    UnitsAssignedPlanNonCAT = ISNULL(B.UnitsAssignedPlanNonCAT,0),
	UnitSoldPlanNonCAT = ISNULL(B.UnitSoldPlanNonCAT,0),
    NumberofPickups = ISNULL(RP.NumberofPickups,0),
    CalendarDaysReleasetoPickup = ISNULL(RP.CalendarDaysReleasetoPickup,0),
    NumberofPickupsNonCAT = ISNULL(RP.NumberofPickupsNonCAT,0),
	CalendarDaysReleasetoPickupNonCAT = ISNULL(RP.CalendarDaysReleasetoPickupNonCAT,0),	
    NumberofPickups100MileswithinoneBusinessDays = ISNULL(RTP.NumberofPickups100MileswithinoneBusinessDays,0),
    NumberofPickupsSameDayReleasedByNoonWithin35Miles=ISNULL(RTP.NumberofPickupsSameDayReleasedByNoonWithin35Miles,0),
    NumberOfWeekendPickups = ISNULL(RTP.NumberOfWeekendPickups,0),
    NumberofPickups100MileswithinoneBusinessDaysNonCAT = ISNULL(RTP.NumberofPickups100MileswithinoneBusinessDaysNonCAT,0),
	NumberofPickupsSameDayReleasedByNoonWithin35MilesNonCAT = ISNULL(RTP.NumberofPickupsSameDayReleasedByNoonWithin35MilesNonCAT,0),
	NumberOfWeekendPickupsNonCAT = ISNULL(RTP.NumberOfWeekendPickupsNonCAT,0),
    NumberofReleaseProblems = ISNULL(RLP.NumberofReleaseProblems,0),
    NumberofReleaseProblemsNonCAT = ISNULL(RLPC.NumberofReleaseProblemsNonCAT,0),
    PipelineInventory = ISNULL(I.PipelineInventory,0),
    PipelineInventoryNonCAT = ISNULL(I.PipelineInventoryNonCAT,0),
    UnitsSetForSale = ISNULL(SFS.UnitsSetForSale,0),
    UnitsSetForSaleNonCAT = ISNULL(SFS.UnitsSetForSaleNonCAT,0),
    UnitSold = ISNULL(S.UnitSoldCount,0),    
    ProviderRevenue = ISNULL(S.ProviderRevenue,0),
    ProviderNetRevenue = ISNULL(S.ProviderNetReturn,0),
    PARevenue = ISNULL(S.PARevenue,0),
    TowCost = ISNULL(S.TowCost,0),
    BuyerRevenue = ISNULL(S.BuyerRevenue,0),
    TotalSalePrice = ISNULL(S.TotalSalePrice,0),
    PAUnitSold = ISNULL(S.PAUnitSold,0),
    UnitSoldNonCAT = ISNULL(S.UnitSoldNonCAT,0),
	PAUnitSoldNonCAT = ISNULL(S.PAUnitSoldNonCAT,0),
	TotalSalePriceNonCAT = ISNULL(S.TotalSalePriceNonCAT,0),
	ProviderRevenueNonCAT = ISNULL(S.ProviderRevenueNonCAT,0),
	PARevenueNonCAT = ISNULL(S.PARevenueNonCAT,0),
	ProviderNetRevenueNonCAT = ISNULL(S.ProviderNetRevenueNonCAT,0),
	BuyerRevenueNonCAT = ISNULL(S.BuyerRevenueNonCAT,0),
	TowCostNonCAT = ISNULL(S.TowCostNonCAT,0),
    MinimumSalesPrice = ISNULL(MSP.MinimumSalesPrice,0),
    MinimumSalesPriceNonCAT = ISNULL(MSP.MinimumSalesPriceNonCAT,0),
    
    InsuranceUnitsSold = ISNULL(VP.InsuranceUnitsSold,0),
    InsuranceSalePrice = ISNULL(VP.InsuranceSalePrice,0),
    InsuranceACV = ISNULL(VP.InsuranceACV,0),
    CharityUnitsSold = ISNULL(VP.CharityUnitsSold,0),
    CharityTotalSalePrice = ISNULL(VP.CharityTotalSalePrice,0),
    VRDUnitsSold=ISNULL(VP.VRDUnitsSold,0),
    VRDTotalSalePrice= ISNULL(VP.VRDTotalSalePrice,0),
    RentalUnitsSold = ISNULL(VP.RentalUnitsSold,0),
    RentalTotalSalePrice = ISNULL(VP.RentalTotalSalePrice,0),
    OtherUnitsSold=ISNULL(VP.OtherUnitsSold,0),
    OtherTotalSalePrice=ISNULL(VP.OtherTotalSalePrice,0),
    InsuranceGrossReturn =  ISNULL(VP.InsuranceGrossReturn,0),
    InsuranceUnitsSoldNonCAT =  ISNULL(VP.InsuranceUnitsSoldNonCAT,0),
	InsuranceSalePriceNonCAT =  ISNULL(VP.InsuranceSalePriceNonCAT,0),
	InsuranceACVNonCAT =  ISNULL(VP.InsuranceACVNonCAT,0),
	CharityUnitsSoldNonCAT =  ISNULL(VP.CharityUnitsSoldNonCAT,0),
	CharityTotalSalePriceNonCAT =  ISNULL(VP.CharityTotalSalePriceNonCAT,0),
	VRDUnitsSoldNonCAT =  ISNULL(VP.VRDUnitsSoldNonCAT,0),
	VRDTotalSalePriceNonCAT =  ISNULL(VP.VRDTotalSalePriceNonCAT,0),
	RentalUnitsSoldNonCAT =  ISNULL(VP.RentalUnitsSoldNonCAT,0),
	RentalTotalSalePriceNonCAT =  ISNULL(VP.RentalTotalSalePriceNonCAT,0),
	OtherUnitsSoldNonCAT =  ISNULL(VP.OtherUnitsSoldNonCAT,0),
	OtherTotalSalePriceNonCAT =  ISNULL(VP.OtherTotalSalePriceNonCAT,0),
	InsuranceGrossReturnNonCAT =  ISNULL(VP.InsuranceGrossReturnNonCAT,0),
	IBIDSoldUnits = ISNULL(BT.IBIDSoldUnits,0),
    ProxySoldUnits = ISNULL(BT.ProxySoldUnits,0),
    IBFSoldUnits = ISNULL(BT.IBFSoldUnits,0),
    SuccessfulBuyer=ISNULL(BT.SuccessfulBuyer,0),
    OutofCountrySoldUnits = ISNULL(BT.OutofCountrySoldUnits,0),
    OutofStateSoldUnits = ISNULL(BT.OutofStateSoldUnits,0),
    IBIDSoldUnitsNonCAT = ISNULL(BT.IBIDSoldUnitsNonCAT,0),
	ProxySoldUnitsNonCAT = ISNULL(BT.ProxySoldUnitsNonCAT,0),
	OutofCountrySoldUnitsNonCAT = ISNULL(BT.OutofCountrySoldUnitsNonCAT,0),
	OutofStateSoldUnitsNonCAT = ISNULL(BT.OutofStateSoldUnitsNonCAT,0),
	SuccessfulBuyerNonCAT = ISNULL(BT.SuccessfulBuyerNonCAT,0),
    BuyerAttendance = ISNULL(BUA.BuyerAttendance,0),
    OutofCountryBuyerAttendance = ISNULL(BUA.OutofCountryBuyerAttendance,0),
    GuestAttendance = ISNULL(BUA.GuestAttendance,0), 
    BuyerAttendanceNonCAT = ISNULL(BUA.BuyerAttendanceNonCAT,0),
	OutofCountryBuyerAttendanceNonCAT = ISNULL(BUA.OutofCountryBuyerAttendanceNonCAT,0),
	GuestAttendanceNonCAT = ISNULL(BUA.GuestAttendanceNonCAT,0),
    CountofPUwithIn100MilesOneBusinessDays = ISNULL(CPU.CountofPUwithIn100MilesOneBusinessDays,0),
	CountofPUwithIn100MilesOneBusinessDaysNonCAT = ISNULL(CPU.CountofPUwithIn100MilesOneBusinessDaysNonCAT,0),
	ACV = ISNULL(SA.BlackbookACV,0),
	ACVNonCAT = ISNULL(SA.ACVNonCAT,0),
	UnitsSoldsWithKey = ISNULL(SVA.Keys,0),
    UnitsSoldRunAndDriveAutoOnly = ISNULL(SVA.RunAndDriveAutoOnly,0),
    UnitsSoldCarStart = ISNULL(SVA.CarStart,0),
    UnitsSoldMileage = ISNULL(SVA.Mileage,0),
    UnitsSoldEnhancement = ISNULL(SVA.Enhancement,0),
    UnitsSoldReRun = ISNULL(SVA.ReRun,0),
    UnitsSoldsWithKeyOnly = ISNULL(SVA.UnitsSoldsWithKeyOnly,0),
    VRDUnitsSoldKey = ISNULL(SVA.VRDKeys,0),
    VRDUnitsSoldRunAndDriveAutoOnly = ISNULL(SVA.VRDRunAndDriveAutoOnly,0),
    VRDUnitsSoldCarStart = ISNULL(SVA.VRDCarStart,0),
    VRDUnitsSoldMileage = ISNULL(SVA.VRDMileage,0),
    VRDUnitsSoldEnhancement = ISNULL(SVA.VRDEnhancement,0),
    VRDReRun = ISNULL(SVA.VRDReRun,0),
    UnitsSoldsWithKeyNonCAT = ISNULL(SVA.UnitsSoldsWithKeyNonCAT,0),
	UnitsSoldRunAndDriveAutoOnlyNonCAT = ISNULL(SVA.UnitsSoldRunAndDriveAutoOnlyNonCAT,0),
	UnitsSoldCarStartNonCAT = ISNULL(SVA.UnitsSoldCarStartNonCAT,0),
	UnitsSoldMileageNonCAT = ISNULL(SVA.UnitsSoldMileageNonCAT,0),
	UnitsSoldEnhancementNonCAT = ISNULL(SVA.UnitsSoldEnhancementNonCAT,0),
	UnitsSoldReRunNonCAT = ISNULL(SVA.UnitsSoldReRunNonCAT,0),
	VRDUnitsSoldKeyNonCAT = ISNULL(SVA.VRDUnitsSoldKeyNonCAT,0),
	VRDUnitsSoldRunAndDriveAutoOnlyNonCAT = ISNULL(SVA.VRDUnitsSoldRunAndDriveAutoOnlyNonCAT,0),
	VRDUnitsSoldCarStartNonCAT = ISNULL(SVA.VRDUnitsSoldCarStartNonCAT,0),
	VRDUnitsSoldMileageNonCAT = ISNULL(SVA.VRDUnitsSoldMileageNonCAT,0),
	VRDUnitsSoldEnhancementNonCAT = ISNULL(SVA.VRDUnitsSoldEnhancementNonCAT,0),
	VRDReRunNonCAT = ISNULL(SVA.VRDReRunNonCAT,0),
	VRDUnitsSoldKeyOnlyNonCAT = ISNULL(SVA.VRDUnitsSoldKeyOnlyNonCAT,0),
    IBFSalePrice = ISNULL(IBF.IBFSalePrice,0),
    IBFNextHighestHistoricalBid = ISNULL(IBF.IBFNextHighestHistoricalBid,0),
    IBFSoldUnitsNonCAT = ISNULL(IBF.IBFSoldUnitsNonCAT,0),
	IBFSalePriceNonCAT = ISNULL(IBF.IBFSalePriceNonCAT,0),
	IBFNextHighestHistoricalBidNonCAT = ISNULL(IBF.IBFNextHighestHistoricalBidNonCAT,0),  
    UnitsSoldPublic = ISNULL(BM.PublicUnitsSold,0),
    UnitsSoldPublicNonCAT = ISNULL(BM.UnitsSoldPublicNonCAT,0),
    UnitsSoldPushedByPublic = ISNULL(PB.PushedByPublic,0),
    UnitsSoldPushedByPublicNonCAT = ISNULL(PB.UnitsSoldPushedByPublicNonCAT,0),
    CalendarDaysAssignmentToRelease = ISNULL(CDA.CalendarDaysAssignmentToRelease,0),
	CalendarDaysAssignmenttoReleaseNonCAT = ISNULL(CDA.CalendarDaysAssignmenttoReleaseNonCAT,0),
	UnitsWrappedWithin7DaysOfCheckIn = ISNULL(WCI.UnitsWrappedWithin7DaysOfCheckIn,0),
	UnitsWrappedWithin7DaysOfCheckInNonCAT = ISNULL(WCI.UnitsWrappedWithin7DaysOfCheckInNonCAT,0),
	UsableAcreage = ISNULL(DB.UsableAcreage, 0),
	PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApproval = ISNULL(SF.PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApproval,0),
	PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApprovalNonCAT = ISNULL(SF.PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApprovalNonCAT,0)
FROM #TMP_KPI_Branch tk WITH(NOLOCK)
LEFT OUTER JOIN #TMP_Assignments A WITH(NOLOCK)
 ON tk.FiscalWeekID = a.FiscalWeekID  
 AND tk.BranchNumber = A.AssignmentBranchNumber 
LEFT OUTER JOIN #TMP_Budget B WITH(NOLOCK)
 ON tk.FiscalWeekID = b.FiscalWeekID  
 AND tk.BranchNumber = B.BranchNumber 
LEFT OUTER JOIN #TMP_ReleasePickup RP WITH(NOLOCK)
  ON tk.FiscalWeekID = RP.FiscalWeekID  
 AND tk.BranchNumber = RP.BranchNumber 
LEFT OUTER JOIN #TMP_ReleaseToPickup RTP WITH(NOLOCK)
 ON tk.FiscalWeekID = RTP.FiscalWeekID  
 AND tk.BranchNumber = RTP.BranchNumber 
LEFT OUTER JOIN #TMP_ReleaseProblem RLP WITH(NOLOCK)
 ON tk.FiscalWeekID = RLP.FiscalWeekID  
 AND tk.BranchNumber = RLP.BranchNumber 
 LEFT OUTER JOIN #TMP_ReleaseProblemNonCAT RLPC WITH(NOLOCK)
 ON tk.FiscalWeekID = RLPC.FiscalWeekID  
 AND tk.BranchNumber = RLPC.BranchNumber 
LEFT OUTER JOIN #TMP_Inventory I WITH(NOLOCK)
 ON tk.FiscalWeekID = I.FiscalWeekID  
 AND tk.BranchNumber = I.AdministrativeBranchNumber 
LEFT OUTER JOIN #TMP_Sale S WITH(NOLOCK)
 ON tk.FiscalWeekID = S.FiscalWeekID  
 AND tk.BranchNumber = S.AdministrativeBranchNumber
LEFT OUTER JOIN #TMP_MinimumSalesPrice MSP WITH(NOLOCK)
 ON tk.FiscalWeekID = MSP.FiscalWeekID  
 AND tk.BranchNumber = MSP.AdministrativeBranchNumber
LEFT OUTER JOIN #TMP_SetForSale SFS WITH(NOLOCK)
 ON tk.FiscalWeekID = SFS.FiscalWeekID  
 AND tk.BranchNumber = SFS.BranchNumber
LEFT OUTER JOIN #TMP_VolumeProceeds VP WITH(NOLOCK)
 ON tk.FiscalWeekID = VP.FiscalWeekID  
 AND tk.BranchNumber = VP.AdministrativeBranchNumber
LEFT OUTER JOIN #TMP_BidTypeSold BT WITH(NOLOCK)
 ON tk.FiscalWeekID = BT.FiscalWeekID  
 AND tk.BranchNumber = BT.AdministrativeBranchNumber
LEFT OUTER JOIN #TMP_BuyerAttendance BUA WITH(NOLOCK)
 ON tk.FiscalWeekID = BUA.FiscalWeekID  
 AND tk.BranchNumber = BUA.BranchNumber
LEFT OUTER JOIN #TMP_CountofPUwithIn100MilesOneBusinessDays CPU
 ON  tk.FiscalWeekID = CPU.FiscalWeekID  
 AND tk.BranchNumber = CPU.BranchNumber
LEFT OUTER JOIN #TMP_Sold_ACV SA
 ON  tk.FiscalWeekID = SA.FiscalWeekID  
 AND tk.BranchNumber = SA.BranchNumber
LEFT OUTER JOIN #TMP_SoldVehicleAtrributes SVA
 ON  tk.FiscalWeekID = SVA.FiscalWeekID  
 AND tk.BranchNumber = SVA.BranchNumber
LEFT OUTER JOIN #TMP_IBF IBF
 ON  tk.FiscalWeekID = IBF.FiscalWeekID  
 AND tk.BranchNumber = IBF.BranchNumber
LEFT OUTER JOIN #TMP_BuyerMetrics BM
 ON  tk.FiscalWeekID = BM.FiscalWeekID  
 AND tk.BranchNumber = BM.BranchNumber
LEFT OUTER JOIN #TMP_CalendarDaysAssignmentToRelease CDA
 ON  CDA.FiscalWeekID = BM.FiscalWeekID  
 AND CDA.BranchNumber = BM.BranchNumber
LEFT OUTER JOIN #TMP_PushedByPublic PB
 ON  PB.FiscalWeekID = BM.FiscalWeekID  
 AND PB.BranchNumber = BM.BranchNumber
LEFT OUTER JOIN #TMP_UnitsWrappedWithin7DaysOfCheckIn WCI
 ON  WCI.FiscalWeekID = BM.FiscalWeekID  
 AND WCI.BranchNumber = BM.BranchNumber
LEFT OUTER JOIN DimBranch DB WITH(NOLOCK)
 ON DB.BranchNumber = BM.BranchNumber
LEFT OUTER JOIN #TMP_PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApproval SF
 ON  SF.FiscalWeekID = BM.FiscalWeekID  
 AND SF.BranchNumber = BM.BranchNumber
 


   
INSERT INTO #TMP_KPI_Corporate
(	FiscalWeekID,
	LastdayOfweekid
)
SELECT DISTINCT D.FiscalWeekID,
				D.LastDayOfWeekDayID
FROM dbo.DimBranch DB WITH(NOLOCK)
CROSS JOIN #TMP_KPIDay D WITH(NOLOCK)
WHERE D.DayID = @CurrentRunDayID 

INSERT INTO  #TMP_Sale_Corporate
(	FiscalWeekID,
	InsuranceGrossReturn,
	InsuranceUnitsSold,
	InsuranceSalePrice,
	InsuranceACV,
	InsuranceGrossReturnNonCAT,
	InsuranceUnitsSoldNonCAT,
	InsuranceSalePriceNonCAT,
	InsuranceACVNonCAT
	)
SELECT	FiscalWeekID,
		SUM(CASE WHEN DS.IsStockSold = 1 AND Providertypecode IN ('INS','ADJ') THEN GrossReturn ELSE 0 END)  InsuranceGrossReturn,
		SUM(CASE WHEN DS.IsStockSold = 1 AND Providertypecode IN ('INS','ADJ') THEN 1 ELSE 0 END)  InsuranceUnitsSold,
		SUM(CASE WHEN DS.IsStockSold = 1 AND Providertypecode IN ('INS','ADJ') THEN SalePrice ELSE 0 END) InsuranceSalePrice,
		SUM(CASE WHEN DS.IsStockSold = 1 AND Providertypecode IN ('INS','ADJ') AND DS.ActualCashValue > 0 AND DS.IsACVOutlier = 0 THEN DS.ActualCashValue ELSE NULL END) InsuranceACV,
		--NonCAT
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND Providertypecode IN ('INS','ADJ') THEN GrossReturn ELSE 0 END)  InsuranceGrossReturn,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND Providertypecode IN ('INS','ADJ') THEN 1 ELSE 0 END)  InsuranceUnitsSold,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND Providertypecode IN ('INS','ADJ') THEN SalePrice ELSE 0 END) InsuranceSalePrice,
		SUM(CASE WHEN DS.IsCAT = 0 AND DS.IsStockSold = 1 AND Providertypecode IN ('INS','ADJ') AND DS.ActualCashValue > 0 AND DS.IsACVOutlier = 0 THEN DS.ActualCashValue ELSE NULL END) InsuranceACV
FROM   dbo.DimStock DS WITH (NOLOCK) 
INNER JOIN dbo.FactStockCycleTime FSCT WITH (NOLOCK)
 ON DS.StockID = FSCT.StockID
INNER JOIN dbo.DimProvider DP WITH (NOLOCK) 
 ON DP.ProviderID = DS.CurrentProviderID
INNER JOIN #TMP_KPIDay D WITH (NOLOCK)
 ON FSCT.SoldDayID  = D.DayID
GROUP BY FiscalWeekID	

UPDATE  KC 
SET InsuranceGrossReturn = ISNULL(SC.InsuranceGrossReturn,0) ,
	InsuranceUnitsSold = ISNULL(SC.InsuranceUnitsSold,0) ,
	InsuranceSalePrice = ISNULL(SC.InsuranceSalePrice,0) ,
	InsuranceACV = ISNULL(SC.InsuranceACV,0),
	InsuranceGrossReturnNonCAT = ISNULL(SC.InsuranceGrossReturnNonCAT,0),
	InsuranceUnitsSoldNonCAT = ISNULL(SC.InsuranceUnitsSoldNonCAT,0),
	InsuranceSalePriceNonCAT = ISNULL(SC.InsuranceSalePriceNonCAT,0),
	InsuranceACVNonCAT = ISNULL(SC.InsuranceACVNonCAT,0)
FROM #TMP_KPI_Corporate KC  
LEFT OUTER JOIN #TMP_Sale_Corporate SC
 ON KC.FiscalWeekID = SC.FiscalWeekID


INSERT INTO #TMP_Insurance_GrossReturn
(	FiscalWeekID,
	StockID,
	GrossReturn
	)		
SELECT	D.FiscalWeekID,
		DS.StockID,
		DS.GrossReturn
FROM dbo.DimStock DS WITH (NOLOCK)  
INNER JOIN dbo.FactStockCycleTime FSCT WITH (NOLOCK)
 ON ds.StockID = FSCT.StockID
INNER JOIN #TMP_KPIDay D WITH (NOLOCK)
 ON FSCT.SoldDayID  = D.DayID
INNER JOIN  dbo.DimProvider DP  WITH (NOLOCK)
 ON DP.ProviderID = DS.CurrentProviderID
WHERE ProviderTypeCode IN ('INS','ADJ')
	
 
 INSERT INTO #TMP_Top20Percent_StockID
(	[StockID]
)	
SELECT StockID   
FROM #TMP_Insurance_GrossReturn
WHERE GrossReturn in (
						SELECT top 20 Percent GrossReturn
						FROM #TMP_Insurance_GrossReturn
						order by GrossReturn desc
						)

INSERT INTO #TMP_Top20Percent
(	[FiscalWeekID],
	[InsuranceTop20Units],
	[InsuranceTop20GrossReturn]
	)	
	
SELECT	FiscalWeekID,
		COUNT(StockID) AS InsuranceTop20Units , 
		SUM(GrossReturn) AS InsuranceTop20GrossReturn  
FROM #TMP_Insurance_GrossReturn
WHERE StockID IN (SELECT DISTINCT StockID FROM #TMP_Top20Percent_StockID)
GROUP BY FiscalWeekID	

INSERT INTO #TMP_Bottom20Percent_StockID
(	[StockID]
	)
SELECT StockID   
FROM #TMP_Insurance_GrossReturn
WHERE GrossReturn in (
						SELECT top 20 Percent GrossReturn
						from #TMP_Insurance_GrossReturn
						order by GrossReturn ASC
						)		

INSERT INTO #TMP_Bottom20Percent
(	FiscalWeekID,
	InsuranceBottom20Units,
	InsuranceBottom20GrossReturn
	)	
	
SELECT	FiscalWeekID,
		COUNT (StockID) AS InsuranceBottom20Units , 
		SUM(GrossReturn) AS InsuranceBottom20GrossReturn   
FROM #TMP_Insurance_GrossReturn
where StockID IN (SELECT DISTINCT StockID FROM #TMP_Bottom20Percent_StockID)
GROUP BY FiscalWeekID

INSERT INTO #TMP_Middle60Percent_StockID
(	FiscalWeekID,
	StockID,
	InsuranceMiddle60GrossReturn
	)	
SElECT FiscalWeekID, stockid, GrossReturn  
FROM #TMP_Insurance_GrossReturn
WHERE StockID NOT IN (SELECT StockID FROM #TMP_Top20Percent_StockID union SELECT StockID FROM #TMP_Bottom20Percent_StockID)
ORDER BY FiscalWeekID, stockid, GrossReturn 
	
INSERT INTO #TMP_Middle60Percent
(	FiscalWeekID,
	InsuranceMiddle60units,
	InsuranceMiddle60GrossReturn
	)
SELECT	FiscalWeekID,
		COUNT (StockID) AS InsuranceMiddle60units , 
		SUM(InsuranceMiddle60GrossReturn)AS InsuranceMiddle60GrossReturn  
FROM #TMP_Middle60Percent_StockID
GROUP BY FiscalWeekID


UPDATE   #TMP_KPI_Corporate 
SET InsuranceBottom20Units = BOTTOM20.InsuranceBottom20Units ,
	InsuranceBottom20GrossReturn = BOTTOM20.InsuranceBottom20GrossReturn ,
	InsuranceTop20Units = TOP20.InsuranceTop20Units ,
	InsuranceTop20GrossReturn = TOP20.InsuranceTop20GrossReturn ,
	InsuranceMiddle60units = MIDDLE60.InsuranceMiddle60units ,
	InsuranceMiddle60GrossReturn = MIDDLE60.InsuranceMiddle60GrossReturn
FROM #TMP_KPI_Corporate KC  
INNER JOIN #TMP_Top20Percent TOP20
ON TOP20.FiscalWeekID = KC.FiscalWeekID
INNER JOIN #TMP_Bottom20Percent BOTTOM20
ON BOTTOM20.FiscalWeekID = KC.FiscalWeekID
INNER JOIN #TMP_Middle60Percent MIDDLE60
ON MIDDLE60.FiscalWeekID = KC.FiscalWeekID


INSERT INTO #TMP_Insurance_GrossReturn_NonCAT
(	FiscalWeekID,
	StockID,
	GrossReturn
	)		
SELECT	D.FiscalWeekID,
		DS.StockID,
		DS.GrossReturn
FROM dbo.DimStock DS WITH (NOLOCK)  
INNER JOIN dbo.FactStockCycleTime FSCT WITH (NOLOCK)
 ON DS.StockID = FSCT.StockID
INNER JOIN #TMP_KPIDay D WITH (NOLOCK)
 ON FSCT.SoldDayID  = D.DayID
INNER JOIN  dbo.DimProvider DP  WITH (NOLOCK)
 ON DP.ProviderID = DS.CurrentProviderID
WHERE DS.IsCAT = 0
AND DP.ProviderTypeCode IN ('INS','ADJ')
	
 
 INSERT INTO #TMP_Top20Percent_StockID_NonCAT
(	[StockID]
)	
SELECT StockID   
FROM #TMP_Insurance_GrossReturn_NonCAT
WHERE GrossReturn in (
						SELECT top 20 Percent GrossReturn
						FROM #TMP_Insurance_GrossReturn_NonCAT
						ORDER BY GrossReturn DESC
						)

INSERT INTO #TMP_Top20Percent_NonCAT
(	[FiscalWeekID],
	[InsuranceTop20Units],
	[InsuranceTop20GrossReturn]
	)	
	
SELECT	FiscalWeekID,
		COUNT (StockID) AS InsuranceTop20Units , 
		SUM(GrossReturn) AS InsuranceTop20GrossReturn  
FROM #TMP_Insurance_GrossReturn_NonCAT
WHERE StockID IN (SELECT DISTINCT StockID FROM #TMP_Top20Percent_StockID_NonCAT)
GROUP BY FiscalWeekID	

INSERT INTO #TMP_Bottom20Percent_StockID_NonCAT
(	[StockID]
	)
SELECT StockID   
FROM #TMP_Insurance_GrossReturn_NonCAT
WHERE GrossReturn in (
						SELECT top 20 Percent GrossReturn
						FROM #TMP_Insurance_GrossReturn_NonCAT
						ORDER BY GrossReturn ASC
						)		

INSERT INTO #TMP_Bottom20Percent_NonCAT
(	FiscalWeekID,
	InsuranceBottom20Units,
	InsuranceBottom20GrossReturn
	)	
	
SELECT	FiscalWeekID,
		COUNT (StockID) AS InsuranceBottom20Units , 
		SUM(GrossReturn) AS InsuranceBottom20GrossReturn   
FROM #TMP_Insurance_GrossReturn_NonCAT
WHERE StockID IN (SELECT DISTINCT StockID FROM #TMP_Bottom20Percent_StockID_NonCAT)
GROUP BY FiscalWeekID

INSERT INTO #TMP_Middle60Percent_StockID_NonCAT
(	FiscalWeekID,
	StockID,
	InsuranceMiddle60GrossReturn
	)	
SElECT FiscalWeekID, stockid, GrossReturn  
FROM #TMP_Insurance_GrossReturn_NonCAT
WHERE StockID NOT IN (SELECT StockID FROM #TMP_Top20Percent_StockID_NonCAT union SELECT StockID FROM #TMP_Bottom20Percent_StockID_NonCAT)
ORDER BY FiscalWeekID, StockID, GrossReturn 
	
INSERT INTO #TMP_Middle60Percent_NonCAT
(	FiscalWeekID,
	InsuranceMiddle60units,
	InsuranceMiddle60GrossReturn
	)
SELECT	FiscalWeekID,
		COUNT (StockID) AS InsuranceMiddle60units , 
		SUM(InsuranceMiddle60GrossReturn)AS InsuranceMiddle60GrossReturn  
FROM #TMP_Middle60Percent_StockID_NonCAT
GROUP BY FiscalWeekID


UPDATE	#TMP_KPI_Corporate 
SET InsuranceBottom20UnitsNonCAT = BOTTOM20.InsuranceBottom20Units,
	InsuranceBottom20GrossReturnNonCAT = BOTTOM20.InsuranceBottom20GrossReturn,
	InsuranceTop20UnitsNonCAT = TOP20.InsuranceTop20Units,
	InsuranceTop20GrossReturnNonCAT = TOP20.InsuranceTop20GrossReturn,
	InsuranceMiddle60unitsNonCAT = MIDDLE60.InsuranceMiddle60units,
	InsuranceMiddle60GrossReturnNonCAT = MIDDLE60.InsuranceMiddle60GrossReturn
FROM #TMP_KPI_Corporate KC  
INNER JOIN #TMP_Top20Percent_NonCAT TOP20
 ON TOP20.FiscalWeekID = KC.FiscalWeekID
INNER JOIN #TMP_Bottom20Percent_NonCAT BOTTOM20
 ON BOTTOM20.FiscalWeekID = KC.FiscalWeekID
INNER JOIN #TMP_Middle60Percent_NonCAT MIDDLE60
 ON MIDDLE60.FiscalWeekID = KC.FiscalWeekID

/*********************************************************************************************
MERGE the data from the temp table into the EDW KPI_Branch Table
This does Update and Insert only.  No Deletes
*********************************************************************************************/
BEGIN TRANSACTION 

	MERGE [dbo].[KPIBranch] AS KPIBr
    USING #TMP_KPI_Branch AS TKPI
          ON TKPI.FiscalWeekID = KPIBr.FiscalWeekID
          AND TKPI.BranchNumber = KPIBr.BranchNumber
    
    WHEN MATCHED THEN 
        UPDATE SET 
		[Lastdayofweekid] = TKPI.[Lastdayofweekid]
		,[UnitsAssigned] = TKPI.[UnitsAssigned]
		,[UnitsNetAssigned] = TKPI.[UnitsNetAssigned]
		,[UnitsAssignedPlan] = TKPI.[UnitsAssignedPlan]
		,[ElectronicAssignments] = TKPI.[ElectronicAssignments]
		,[CancelledAssignments] = TKPI.[CancelledAssignments]
		,[NumberofPickups] = TKPI.[NumberofPickups]
		,[CalendarDaysReleasetoPickup] = TKPI.[CalendarDaysReleasetoPickup]
		,[NumberofPickups100MileswithinoneBusinessDays] = TKPI.[NumberofPickups100MileswithinoneBusinessDays]
		,[NumberofPickupsSameDayReleasedByNoonWithin35Miles] = TKPI.[NumberofPickupsSameDayReleasedByNoonWithin35Miles]
		,[NumberofReleaseProblems] = TKPI.[NumberofReleaseProblems]
		,[PipelineInventory] = TKPI.[PipelineInventory]
		,[UnitSold] = TKPI.[UnitSold]
		,[UnitSoldPlan] = TKPI.[UnitSoldPlan]
		,[UnitsSetForSale] = TKPI.[UnitsSetForSale]
		,[ProviderRevenue] = TKPI.[ProviderRevenue]
		,[ProviderNetRevenue] = TKPI.[ProviderNetRevenue]
		,[PARevenue] = TKPI.[PARevenue]
		,[TowCost] = TKPI.[TowCost]
		,[BuyerRevenue] = TKPI.[BuyerRevenue]
		,[MinimumSalesPrice] = TKPI.[MinimumSalesPrice]
		,[TotalSalePrice] = TKPI.[TotalSalePrice]
		,[InsuranceUnitsSold] = TKPI.[InsuranceUnitsSold]
		,[InsuranceSalePrice] = TKPI.[InsuranceSalePrice]
		,[InsuranceACV] = TKPI.[InsuranceACV]
		,[CharityUnitsSold] = TKPI.[CharityUnitsSold]
		,[CharityTotalSalePrice] = TKPI.[CharityTotalSalePrice]
		,[VRDUnitsSold] = TKPI.[VRDUnitsSold]
		,[VRDTotalSalePrice] = TKPI.[VRDTotalSalePrice]
		,[OtherUnitsSold] = TKPI.[OtherUnitsSold]
		,[OtherTotalSalePrice] = TKPI.[OtherTotalSalePrice]
		,[IBIDSoldUnits] = TKPI.[IBIDSoldUnits]
		,[ProxySoldUnits] = TKPI.[ProxySoldUnits]
		,[IBFSoldUnits] = TKPI.[IBFSoldUnits]
		,[BuyerAttendance] = TKPI.[BuyerAttendance]
		,[SuccessfulBuyer] = TKPI.[SuccessfulBuyer]
		,[OutofCountryBuyerAttendance] = TKPI.[OutofCountryBuyerAttendance]
		,[OutofCountrySoldUnits] = TKPI.[OutofCountrySoldUnits]
		,[OutofStateSoldUnits] = TKPI.[OutofStateSoldUnits]
		,[GuestAttendance] = TKPI.[GuestAttendance]
		,[InsuranceGrossReturn] = TKPI.[InsuranceGrossReturn]
		,[RentalUnitsSold] = TKPI.[RentalUnitsSold]
		,[RentalTotalSalePrice] = TKPI.[RentalTotalSalePrice]
		,[PAUnitSold] = TKPI.[PAUnitSold]
		,[CountofPUwithIn100MilesOneBusinessDays] = TKPI.[CountofPUwithIn100MilesOneBusinessDays]
		,[ACV] = TKPI.[ACV]
		,[UnitsSoldsWithKey] = TKPI.[UnitsSoldsWithKey]
		,[UnitsSoldRunAndDriveAutoOnly] = TKPI.[UnitsSoldRunAndDriveAutoOnly]
		,[UnitsSoldCarStart] = TKPI.[UnitsSoldCarStart]
		,[UnitsSoldMileage] = TKPI.[UnitsSoldMileage]
		,[UnitsSoldEnhancement] = TKPI.[UnitsSoldEnhancement]
		,[UnitsSoldReRun] = TKPI.[UnitsSoldReRun]
		,[VRDUnitsSoldKey] = TKPI.[VRDUnitsSoldKey]
		,[VRDUnitsSoldRunAndDriveAutoOnly] = TKPI.[VRDUnitsSoldRunAndDriveAutoOnly] 
		,[VRDUnitsSoldCarStart] = TKPI.[VRDUnitsSoldCarStart]
		,[VRDUnitsSoldMileage] = TKPI.[VRDUnitsSoldMileage]
		,[VRDUnitsSoldEnhancement] = TKPI.[VRDUnitsSoldEnhancement]
		,[VRDReRun] = TKPI.[VRDReRun]
		,[IBFSalePrice] = TKPI.[IBFSalePrice]
		,[IBFNextHighestHistoricalBid] = TKPI.[IBFNextHighestHistoricalBid]
		,[UnitsSoldPublic] = TKPI.[UnitsSoldPublic]
		,[UnitsSoldPushedByPublic] = TKPI.[UnitsSoldPushedByPublic]
		,[NumberOfWeekendPickups] = TKPI.[NumberOfWeekendPickups]
		,[CalendarDaysAssignmentToRelease] = TKPI.[CalendarDaysAssignmentToRelease]
		,[UnitsWrappedWithin7DaysOfCheckIn] = TKPI.[UnitsWrappedWithin7DaysOfCheckIn] 
		,[UsableAcreage] = TKPI.[UsableAcreage]
		,[PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApproval] = TKPI.[PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApproval]
		,[ElectronicAssignmentsNonCAT] = TKPI.[ElectronicAssignmentsNonCAT]
		,[CancelledAssignmentsNonCAT] = TKPI.[CancelledAssignmentsNonCAT]
		,[NumberofPickupsNonCAT] = TKPI.[NumberofPickupsNonCAT]
		,[CalendarDaysReleasetoPickupNonCAT] = TKPI.[CalendarDaysReleasetoPickupNonCAT]
		,[NumberofPickups100MileswithinoneBusinessDaysNonCAT] = TKPI.[NumberofPickups100MileswithinoneBusinessDaysNonCAT]
		,[NumberofPickupsSameDayReleasedByNoonWithin35MilesNonCAT] = TKPI.[NumberofPickupsSameDayReleasedByNoonWithin35MilesNonCAT]
		,[NumberofReleaseProblemsNonCAT] = TKPI.[NumberofReleaseProblemsNonCAT]
		,[PipelineInventoryNonCAT] = TKPI.[PipelineInventoryNonCAT]
		,[UnitSoldNonCAT] = TKPI.[UnitSoldNonCAT]
		,[UnitsSetForSaleNonCAT] = TKPI.[UnitsSetForSaleNonCAT]
		,[ProviderRevenueNonCAT] = TKPI.[ProviderRevenueNonCAT]
		,[ProviderNetRevenueNonCAT] = TKPI.[ProviderNetRevenueNonCAT]
		,[PARevenueNonCAT] = TKPI.[PARevenueNonCAT]
		,[TowCostNonCAT] = TKPI.[TowCostNonCAT]
		,[BuyerRevenueNonCAT] = TKPI.[BuyerRevenueNonCAT]
		,[MinimumSalesPriceNonCAT] = TKPI.[MinimumSalesPriceNonCAT]
		,[TotalSalePriceNonCAT] = TKPI.[TotalSalePriceNonCAT]
		,[InsuranceUnitsSoldNonCAT] = TKPI.[InsuranceUnitsSoldNonCAT] 
		,[InsuranceSalePriceNonCAT] = TKPI.[InsuranceSalePriceNonCAT]
		,[InsuranceACVNonCAT] = TKPI.[InsuranceACVNonCAT]
		,[CharityUnitsSoldNonCAT] = TKPI.[CharityUnitsSoldNonCAT]
		,[CharityTotalSalePriceNonCAT] = TKPI.[CharityTotalSalePriceNonCAT]
		,[VRDUnitsSoldNonCAT] = TKPI.[VRDUnitsSoldNonCAT]
		,[VRDTotalSalePriceNonCAT] = TKPI.[VRDTotalSalePriceNonCAT]
		,[OtherUnitsSoldNonCAT] = TKPI.[OtherUnitsSoldNonCAT]
		,[OtherTotalSalePriceNonCAT] = TKPI.[OtherTotalSalePriceNonCAT]
		,[IBIDSoldUnitsNonCAT] = TKPI.[IBIDSoldUnitsNonCAT]
		,[ProxySoldUnitsNonCAT] = TKPI.[ProxySoldUnitsNonCAT]
		,[IBFSoldUnitsNonCAT] = TKPI.[IBFSoldUnitsNonCAT]
		,[BuyerAttendanceNonCAT] = TKPI.[BuyerAttendanceNonCAT]
		,[SuccessfulBuyerNonCAT] = TKPI.[SuccessfulBuyerNonCAT]
		,[OutofCountryBuyerAttendanceNonCAT] = TKPI.[OutofCountryBuyerAttendanceNonCAT]
		,[OutofCountrySoldUnitsNonCAT] = TKPI.[OutofCountrySoldUnitsNonCAT]
		,[OutofStateSoldUnitsNonCAT] = TKPI.[OutofStateSoldUnitsNonCAT]
		,[GuestAttendanceNonCAT] = TKPI.[GuestAttendanceNonCAT]
		,[InsuranceGrossReturnNonCAT] = TKPI.[InsuranceGrossReturnNonCAT]
		,[RentalUnitsSoldNonCAT] = TKPI.[RentalUnitsSoldNonCAT]
		,[RentalTotalSalePriceNonCAT] = TKPI.[RentalTotalSalePriceNonCAT]
		,[PAUnitSoldNonCAT] = TKPI.[PAUnitSoldNonCAT]
		,[ACVNonCAT] = TKPI.[ACVNonCAT]
		,[UnitsSoldsWithKeyNonCAT] = TKPI.[UnitsSoldsWithKeyNonCAT]
		,[UnitsSoldRunAndDriveAutoOnlyNonCAT] = TKPI.[UnitsSoldRunAndDriveAutoOnlyNonCAT]
		,[UnitsSoldCarStartNonCAT] = TKPI.[UnitsSoldCarStartNonCAT]
		,[UnitsSoldMileageNonCAT] = TKPI.[UnitsSoldMileageNonCAT]
		,[UnitsSoldEnhancementNonCAT] = TKPI.[UnitsSoldEnhancementNonCAT]
		,[UnitsSoldReRunNonCAT] = TKPI.[UnitsSoldReRunNonCAT]
		,[VRDUnitsSoldKeyNonCAT] = TKPI.[VRDUnitsSoldKeyNonCAT]
		,[VRDUnitsSoldRunAndDriveAutoOnlyNonCAT] = TKPI.[VRDUnitsSoldRunAndDriveAutoOnlyNonCAT]
		,[VRDUnitsSoldCarStartNonCAT] = TKPI.[VRDUnitsSoldCarStartNonCAT]
		,[VRDUnitsSoldMileageNonCAT] = TKPI.[VRDUnitsSoldMileageNonCAT]
		,[VRDUnitsSoldEnhancementNonCAT] = TKPI.[VRDUnitsSoldEnhancementNonCAT]
		,[VRDReRunNonCAT] = TKPI.[VRDReRunNonCAT]
		,[IBFSalePriceNonCAT] = TKPI.[IBFSalePriceNonCAT]
		,[IBFNextHighestHistoricalBidNonCAT] = TKPI.[IBFNextHighestHistoricalBidNonCAT]
		,[UnitsSoldPublicNonCAT] = TKPI.[UnitsSoldPublicNonCAT]
		,[UnitsSoldPushedByPublicNonCAT] = TKPI.[UnitsSoldPushedByPublicNonCAT]
		,[CurrentYearFiscalWeekMinAverageNonCAT] = TKPI.[CurrentYearFiscalWeekMinAverageNonCAT]
		,[PriorYearFiscalWeekMinAverageNonCAT] = TKPI.[PriorYearFiscalWeekMinAverageNonCAT]
		,[BranchMinimumSalesPriceIndNonCAT] = TKPI.[BranchMinimumSalesPriceIndNonCAT]
		,[CountofPUwithIn100MilesOneBusinessDaysNonCAT] = TKPI.[CountofPUwithIn100MilesOneBusinessDaysNonCAT]
		,[PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApprovalNonCAT] = TKPI.[PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApprovalNonCAT]
		,[TotalPipelineInventoryNonCAT] = TKPI.[TotalPipelineInventoryNonCAT]
		,[UnitsWrappedWithin7DaysOfCheckInNonCAT] = TKPI.[UnitsWrappedWithin7DaysOfCheckInNonCAT]
		,[CurrentYearFiscalWeekMinAverageRegionNonCAT] = TKPI.[CurrentYearFiscalWeekMinAverageRegionNonCAT]
		,[PriorYearFiscalWeekMinAverageRegionNonCAT]= TKPI.[PriorYearFiscalWeekMinAverageRegionNonCAT]
		,[NumberOfWeekendPickupsNonCAT] = TKPI.[NumberOfWeekendPickupsNonCAT]
		,[CalendarDaysAssignmenttoReleaseNonCAT] = TKPI.[CalendarDaysAssignmenttoReleaseNonCAT]
		,[UnitsAssignedNonCAT] = TKPI.[UnitsAssignedNonCAT]
		,[UnitsNetAssignedNonCAT] = TKPI.[UnitsNetAssignedNonCAT]
		,[UnitsAssignedPlanNonCAT] = TKPI.[UnitsAssignedPlanNonCAT]
		,[UnitSoldPlanNonCAT] = TKPI.[UnitSoldPlanNonCAT]
		,[UnitsSoldsWithKeyOnly] = TKPI.[UnitsSoldsWithKeyOnly]
		,[VRDUnitsSoldKeyOnlyNonCAT] = TKPI.[VRDUnitsSoldKeyOnlyNonCAT]
							
	WHEN NOT MATCHED THEN
    INSERT( 
		[BranchNumber]
		,[Lastdayofweekid]
		,[FiscalWeekID]
		,[UnitsAssigned]
		,[UnitsNetAssigned]
		,[UnitsAssignedPlan]
		,[ElectronicAssignments]
		,[CancelledAssignments]
		,[NumberofPickups]
		,[CalendarDaysReleasetoPickup]
		,[NumberofPickups100MileswithinoneBusinessDays]
		,[NumberofPickupsSameDayReleasedByNoonWithin35Miles]
		,[NumberofReleaseProblems]
		,[PipelineInventory]
		,[UnitSold]
		,[UnitSoldPlan]
		,[UnitsSetForSale]
		,[ProviderRevenue]
		,[ProviderNetRevenue]
		,[PARevenue]
		,[TowCost]
		,[BuyerRevenue]
		,[MinimumSalesPrice]
		,[TotalSalePrice]
		,[InsuranceUnitsSold]
		,[InsuranceSalePrice]
		,[InsuranceACV]
		,[CharityUnitsSold]
		,[CharityTotalSalePrice]
		,[VRDUnitsSold]
		,[VRDTotalSalePrice]
		,[OtherUnitsSold]
		,[OtherTotalSalePrice]
		,[IBIDSoldUnits]
		,[ProxySoldUnits]
		,[IBFSoldUnits]
		,[BuyerAttendance]
		,[SuccessfulBuyer]
		,[OutofCountryBuyerAttendance]
		,[OutofCountrySoldUnits]
		,[OutofStateSoldUnits]
		,[GuestAttendance]
		,[InsuranceGrossReturn]
		,[RentalUnitsSold]
		,[RentalTotalSalePrice]
		,[PAUnitSold]
		,[CountofPUwithIn100MilesOneBusinessDays]
		,[ACV]
		,[UnitsSoldsWithKey]
		,[UnitsSoldRunAndDriveAutoOnly]
		,[UnitsSoldCarStart]
		,[UnitsSoldMileage]
		,[UnitsSoldEnhancement]
		,[UnitsSoldReRun]
		,[VRDUnitsSoldKey]
		,[VRDUnitsSoldRunAndDriveAutoOnly]
		,[VRDUnitsSoldCarStart]
		,[VRDUnitsSoldMileage]
		,[VRDUnitsSoldEnhancement]
		,[VRDReRun]
		,[IBFSalePrice]
		,[IBFNextHighestHistoricalBid]
		,[UnitsSoldPublic]
		,[UnitsSoldPushedByPublic]
		,[NumberOfWeekendPickups]
		,[CalendarDaysAssignmentToRelease]
		,[UnitsWrappedWithin7DaysOfCheckIn]
		,[UsableAcreage]
		,[PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApproval]
		,[ElectronicAssignmentsNonCAT]
		,[CancelledAssignmentsNonCAT] 
		,[NumberofPickupsNonCAT] 
		,[CalendarDaysReleasetoPickupNonCAT] 
		,[NumberofPickups100MileswithinoneBusinessDaysNonCAT] 
		,[NumberofPickupsSameDayReleasedByNoonWithin35MilesNonCAT] 
		,[NumberofReleaseProblemsNonCAT] 
		,[PipelineInventoryNonCAT] 
		,[UnitSoldNonCAT] 
		,[UnitsSetForSaleNonCAT] 
		,[ProviderRevenueNonCAT] 
		,[ProviderNetRevenueNonCAT] 
		,[PARevenueNonCAT] 
		,[TowCostNonCAT] 
		,[BuyerRevenueNonCAT] 
		,[MinimumSalesPriceNonCAT] 
		,[TotalSalePriceNonCAT] 
		,[InsuranceUnitsSoldNonCAT] 
		,[InsuranceSalePriceNonCAT] 
		,[InsuranceACVNonCAT] 
		,[CharityUnitsSoldNonCAT] 
		,[CharityTotalSalePriceNonCAT] 
		,[VRDUnitsSoldNonCAT] 
		,[VRDTotalSalePriceNonCAT] 
		,[OtherUnitsSoldNonCAT] 
		,[OtherTotalSalePriceNonCAT] 
		,[IBIDSoldUnitsNonCAT] 
		,[ProxySoldUnitsNonCAT] 
		,[IBFSoldUnitsNonCAT] 
		,[BuyerAttendanceNonCAT] 
		,[SuccessfulBuyerNonCAT] 
		,[OutofCountryBuyerAttendanceNonCAT] 
		,[OutofCountrySoldUnitsNonCAT] 
		,[OutofStateSoldUnitsNonCAT] 
		,[GuestAttendanceNonCAT] 
		,[InsuranceGrossReturnNonCAT] 
		,[RentalUnitsSoldNonCAT] 
		,[RentalTotalSalePriceNonCAT] 
		,[PAUnitSoldNonCAT] 
		,[ACVNonCAT] 
		,[UnitsSoldsWithKeyNonCAT] 
		,[UnitsSoldRunAndDriveAutoOnlyNonCAT] 
		,[UnitsSoldCarStartNonCAT] 
		,[UnitsSoldMileageNonCAT] 
		,[UnitsSoldEnhancementNonCAT] 
		,[UnitsSoldReRunNonCAT] 
		,[VRDUnitsSoldKeyNonCAT] 
		,[VRDUnitsSoldRunAndDriveAutoOnlyNonCAT] 
		,[VRDUnitsSoldCarStartNonCAT] 
		,[VRDUnitsSoldMileageNonCAT] 
		,[VRDUnitsSoldEnhancementNonCAT] 
		,[VRDReRunNonCAT] 
		,[IBFSalePriceNonCAT] 
		,[IBFNextHighestHistoricalBidNonCAT] 
		,[UnitsSoldPublicNonCAT] 
		,[UnitsSoldPushedByPublicNonCAT] 
		,[CurrentYearFiscalWeekMinAverageNonCAT] 
		,[PriorYearFiscalWeekMinAverageNonCAT] 
		,[BranchMinimumSalesPriceIndNonCAT] 
		,[CountofPUwithIn100MilesOneBusinessDaysNonCAT] 
		,[PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApprovalNonCAT] 
		,[TotalPipelineInventoryNonCAT] 
		,[UnitsWrappedWithin7DaysOfCheckInNonCAT] 
		,[CurrentYearFiscalWeekMinAverageRegionNonCAT] 
		,[PriorYearFiscalWeekMinAverageRegionNonCAT]
		,[NumberOfWeekendPickupsNonCAT] 
		,[CalendarDaysAssignmenttoReleaseNonCAT] 
		,[UnitsAssignedNonCAT] 
		,[UnitsNetAssignedNonCAT] 
		,[UnitsAssignedPlanNonCAT] 
		,[UnitSoldPlanNonCAT]
		,[UnitsSoldsWithKeyOnly]
		,[VRDUnitsSoldKeyOnlyNonCAT]
		) 
	  VALUES (TKPI.[BranchNumber]
		,TKPI.[Lastdayofweekid]
		,TKPI.[FiscalWeekID]
		,TKPI.[UnitsAssigned]
		,TKPI.[UnitsNetAssigned]
		,TKPI.[UnitsAssignedPlan]
		,TKPI.[ElectronicAssignments]
		,TKPI.[CancelledAssignments]
		,TKPI.[NumberofPickups]
		,TKPI.[CalendarDaysReleasetoPickup]
		,TKPI.[NumberofPickups100MileswithinoneBusinessDays]
		,TKPI.[NumberofPickupsSameDayReleasedByNoonWithin35Miles]
		,TKPI.[NumberofReleaseProblems]
		,TKPI.[PipelineInventory]
		,TKPI.[UnitSold]
		,TKPI.[UnitSoldPlan]
		,TKPI.[UnitsSetForSale]
		,TKPI.[ProviderRevenue]
		,TKPI.[ProviderNetRevenue]
		,TKPI.[PARevenue]
		,TKPI.[TowCost]
		,TKPI.[BuyerRevenue]
		,TKPI.[MinimumSalesPrice]
		,TKPI.[TotalSalePrice]
		,TKPI.[InsuranceUnitsSold]
		,TKPI.[InsuranceSalePrice]
		,TKPI.[InsuranceACV]
		,TKPI.[CharityUnitsSold]
		,TKPI.[CharityTotalSalePrice]
		,TKPI.[VRDUnitsSold]
		,TKPI.[VRDTotalSalePrice]
		,TKPI.[OtherUnitsSold]
		,TKPI.[OtherTotalSalePrice]
		,TKPI.[IBIDSoldUnits]
		,TKPI.[ProxySoldUnits]
		,TKPI.[IBFSoldUnits]
		,TKPI.[BuyerAttendance]
		,TKPI.[SuccessfulBuyer]
		,TKPI.[OutofCountryBuyerAttendance]
		,TKPI.[OutofCountrySoldUnits]
		,TKPI.[OutofStateSoldUnits]
		,TKPI.[GuestAttendance]
		,TKPI.[InsuranceGrossReturn]
		,TKPI.[RentalUnitsSold]
		,TKPI.[RentalTotalSalePrice]
		,TKPI.[PAUnitSold]
		,TKPI.[CountofPUwithIn100MilesOneBusinessDays]
		,TKPI.[ACV]
		,TKPI.[UnitsSoldsWithKey]
		,TKPI.[UnitsSoldRunAndDriveAutoOnly]
		,TKPI.[UnitsSoldCarStart]
		,TKPI.[UnitsSoldMileage]
		,TKPI.[UnitsSoldEnhancement]
		,TKPI.[UnitsSoldReRun]
		,TKPI.[VRDUnitsSoldKey]
		,TKPI.[VRDUnitsSoldRunAndDriveAutoOnly]
		,TKPI.[VRDUnitsSoldCarStart]
		,TKPI.[VRDUnitsSoldMileage]
		,TKPI.[VRDUnitsSoldEnhancement]
		,TKPI.[VRDReRun]
		,TKPI.[IBFSalePrice]
		,TKPI.[IBFNextHighestHistoricalBid]
		,TKPI.[UnitsSoldPublic]
		,TKPI.[UnitsSoldPushedByPublic]	  
		,TKPI.[NumberOfWeekendPickups]
		,TKPI.[CalendarDaysAssignmentToRelease]
		,TKPI.[UnitsWrappedWithin7DaysOfCheckIn] 
		,TKPI.[UsableAcreage]
		,TKPI.[PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApproval]
		,TKPI.[ElectronicAssignmentsNonCAT]
		,TKPI.[CancelledAssignmentsNonCAT] 
		,TKPI.[NumberofPickupsNonCAT] 
		,TKPI.[CalendarDaysReleasetoPickupNonCAT] 
		,TKPI.[NumberofPickups100MileswithinoneBusinessDaysNonCAT] 
		,TKPI.[NumberofPickupsSameDayReleasedByNoonWithin35MilesNonCAT] 
		,TKPI.[NumberofReleaseProblemsNonCAT] 
		,TKPI.[PipelineInventoryNonCAT] 
		,TKPI.[UnitSoldNonCAT] 
		,TKPI.[UnitsSetForSaleNonCAT] 
		,TKPI.[ProviderRevenueNonCAT] 
		,TKPI.[ProviderNetRevenueNonCAT] 
		,TKPI.[PARevenueNonCAT] 
		,TKPI.[TowCostNonCAT] 
		,TKPI.[BuyerRevenueNonCAT] 
		,TKPI.[MinimumSalesPriceNonCAT] 
		,TKPI.[TotalSalePriceNonCAT] 
		,TKPI.[InsuranceUnitsSoldNonCAT] 
		,TKPI.[InsuranceSalePriceNonCAT] 
		,TKPI.[InsuranceACVNonCAT] 
		,TKPI.[CharityUnitsSoldNonCAT] 
		,TKPI.[CharityTotalSalePriceNonCAT] 
		,TKPI.[VRDUnitsSoldNonCAT] 
		,TKPI.[VRDTotalSalePriceNonCAT] 
		,TKPI.[OtherUnitsSoldNonCAT] 
		,TKPI.[OtherTotalSalePriceNonCAT] 
		,TKPI.[IBIDSoldUnitsNonCAT] 
		,TKPI.[ProxySoldUnitsNonCAT] 
		,TKPI.[IBFSoldUnitsNonCAT] 
		,TKPI.[BuyerAttendanceNonCAT] 
		,TKPI.[SuccessfulBuyerNonCAT] 
		,TKPI.[OutofCountryBuyerAttendanceNonCAT] 
		,TKPI.[OutofCountrySoldUnitsNonCAT] 
		,TKPI.[OutofStateSoldUnitsNonCAT] 
		,TKPI.[GuestAttendanceNonCAT] 
		,TKPI.[InsuranceGrossReturnNonCAT] 
		,TKPI.[RentalUnitsSoldNonCAT] 
		,TKPI.[RentalTotalSalePriceNonCAT] 
		,TKPI.[PAUnitSoldNonCAT] 
		,TKPI.[ACVNonCAT] 
		,TKPI.[UnitsSoldsWithKeyNonCAT] 
		,TKPI.[UnitsSoldRunAndDriveAutoOnlyNonCAT] 
		,TKPI.[UnitsSoldCarStartNonCAT] 
		,TKPI.[UnitsSoldMileageNonCAT] 
		,TKPI.[UnitsSoldEnhancementNonCAT] 
		,TKPI.[UnitsSoldReRunNonCAT] 
		,TKPI.[VRDUnitsSoldKeyNonCAT] 
		,TKPI.[VRDUnitsSoldRunAndDriveAutoOnlyNonCAT] 
		,TKPI.[VRDUnitsSoldCarStartNonCAT] 
		,TKPI.[VRDUnitsSoldMileageNonCAT] 
		,TKPI.[VRDUnitsSoldEnhancementNonCAT] 
		,TKPI.[VRDReRunNonCAT] 
		,TKPI.[IBFSalePriceNonCAT] 
		,TKPI.[IBFNextHighestHistoricalBidNonCAT] 
		,TKPI.[UnitsSoldPublicNonCAT] 
		,TKPI.[UnitsSoldPushedByPublicNonCAT] 
		,TKPI.[CurrentYearFiscalWeekMinAverageNonCAT] 
		,TKPI.[PriorYearFiscalWeekMinAverageNonCAT] 
		,TKPI.[BranchMinimumSalesPriceIndNonCAT] 
		,TKPI.[CountofPUwithIn100MilesOneBusinessDaysNonCAT] 
		,TKPI.[PipelineInvetoryMoreThan30DaysWaitingStateFarmTitleApprovalNonCAT] 
		,TKPI.[TotalPipelineInventoryNonCAT] 
		,TKPI.[UnitsWrappedWithin7DaysOfCheckInNonCAT] 
		,TKPI.[CurrentYearFiscalWeekMinAverageRegionNonCAT] 
		,TKPI.[PriorYearFiscalWeekMinAverageRegionNonCAT]
		,TKPI.[NumberOfWeekendPickupsNonCAT] 
		,TKPI.[CalendarDaysAssignmenttoReleaseNonCAT] 
		,TKPI.[UnitsAssignedNonCAT] 
		,TKPI.[UnitsNetAssignedNonCAT] 
		,TKPI.[UnitsAssignedPlanNonCAT] 
		,TKPI.[UnitSoldPlanNonCAT]
		,TKPI.[UnitsSoldsWithKeyOnly]
		,TKPI.[VRDUnitsSoldKeyOnlyNonCAT]
   	)
	     
	OUTPUT $action INTO #TMP_KPI_BranchRecordCount;


	

/*********************************************************************************************
Get the counts from the MERGE operation
*********************************************************************************************/
SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #TMP_KPI_BranchRecordCount

/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount


/*********************************************************************************************
MERGE the data from the temp table into the EDW KPI_Branch Table
This does Update and Insert only.  No Deletes
*********************************************************************************************/
	MERGE dbo.KPICorporate AS KPICorp
    USING #TMP_KPI_Corporate AS TKPICorp
          ON TKPICorp.FiscalWeekID = KPICorp.FiscalWeekID
   
    WHEN MATCHED THEN 
        UPDATE SET 
			[Lastdayofweekid] = TKPICorp.[Lastdayofweekid]
			,[InsuranceGrossReturn] = TKPICorp.[InsuranceGrossReturn]
			,[InsuranceUnitsSold] = TKPICorp.[InsuranceUnitsSold]
			,[InsuranceSalePrice] = TKPICorp.[InsuranceSalePrice]
			,[InsuranceACV] = TKPICorp.[InsuranceACV]
			,[InsuranceBottom20GrossReturn] = TKPICorp.[InsuranceBottom20GrossReturn]
			,[InsuranceBottom20Units] = TKPICorp.[InsuranceBottom20Units]
			,[InsuranceMiddle60GrossReturn] = TKPICorp.[InsuranceMiddle60GrossReturn]
			,[InsuranceMiddle60Units] = TKPICorp.[InsuranceMiddle60Units]
			,[InsuranceTop20GrossReturn] = TKPICorp.[InsuranceTop20GrossReturn]
			,[InsuranceTop20Units] = TKPICorp.[InsuranceTop20Units]
			,[InsuranceGrossReturnNonCAT] = TKPICorp.[InsuranceGrossReturnNonCAT]
			,[InsuranceUnitsSoldNonCAT] = TKPICorp.[InsuranceUnitsSoldNonCAT]
			,[InsuranceSalePriceNonCAT] = TKPICorp.[InsuranceSalePriceNonCAT]
			,[InsuranceACVNonCAT] = TKPICorp.[InsuranceACVNonCAT]
			,[InsuranceBottom20GrossReturnNonCAT] = TKPICorp.[InsuranceBottom20GrossReturnNonCAT]
			,[InsuranceBottom20UnitsNonCAT] = TKPICorp.[InsuranceBottom20UnitsNonCAT]
			,[InsuranceMiddle60GrossReturnNonCAT] = TKPICorp.[InsuranceMiddle60GrossReturnNonCAT]
			,[InsuranceMiddle60UnitsNonCAT] = TKPICorp.[InsuranceMiddle60UnitsNonCAT]
			,[InsuranceTop20GrossReturnNonCAT] = TKPICorp.[InsuranceTop20GrossReturnNonCAT]
			,[InsuranceTop20UnitsNonCAT] = TKPICorp.[InsuranceTop20UnitsNonCAT]
    						
	WHEN NOT MATCHED THEN
    INSERT(	[FiscalWeekID]
			,[Lastdayofweekid]
			,[InsuranceGrossReturn]
			,[InsuranceUnitsSold]
			,[InsuranceSalePrice]
			,[InsuranceACV]
			,[InsuranceBottom20GrossReturn]
			,[InsuranceBottom20Units]
			,[InsuranceMiddle60GrossReturn]
			,[InsuranceMiddle60Units]
			,[InsuranceTop20GrossReturn]
			,[InsuranceTop20Units]
			,[InsuranceGrossReturnNonCAT]
			,[InsuranceUnitsSoldNonCAT]
			,[InsuranceSalePriceNonCAT]
			,[InsuranceACVNonCAT]
			,[InsuranceBottom20GrossReturnNonCAT]
			,[InsuranceBottom20UnitsNonCAT]
			,[InsuranceMiddle60GrossReturnNonCAT]
			,[InsuranceMiddle60UnitsNonCAT]
			,[InsuranceTop20GrossReturnNonCAT]
			,[InsuranceTop20UnitsNonCAT]
			) 
	  VALUES (TKPICorp.[FiscalWeekID]
			,TKPICorp.[Lastdayofweekid]
			,TKPICorp.[InsuranceGrossReturn]
			,TKPICorp.[InsuranceUnitsSold]
			,TKPICorp.[InsuranceSalePrice]
			,TKPICorp.[InsuranceACV]
			,TKPICorp.[InsuranceBottom20GrossReturn]
			,TKPICorp.[InsuranceBottom20Units]
			,TKPICorp.[InsuranceMiddle60GrossReturn]
			,TKPICorp.[InsuranceMiddle60Units]
			,TKPICorp.[InsuranceTop20GrossReturn]
			,TKPICorp.[InsuranceTop20Units]
			,TKPICorp.[InsuranceGrossReturnNonCAT]
			,TKPICorp.[InsuranceUnitsSoldNonCAT]
			,TKPICorp.[InsuranceSalePriceNonCAT]
			,TKPICorp.[InsuranceACVNonCAT]
			,TKPICorp.[InsuranceBottom20GrossReturnNonCAT]
			,TKPICorp.[InsuranceBottom20UnitsNonCAT]
			,TKPICorp.[InsuranceMiddle60GrossReturnNonCAT]
			,TKPICorp.[InsuranceMiddle60UnitsNonCAT]
			,TKPICorp.[InsuranceTop20GrossReturnNonCAT]
			,TKPICorp.[InsuranceTop20UnitsNonCAT]
						)
	OUTPUT $action INTO #TMP_KPI_BranchRecordCount;

/*********************************************************************************************
Get the counts from the MERGE operation
*********************************************************************************************/
SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #TMP_KPI_BranchRecordCount

/*********************************************************************************************
Update IsCurrentWeek for KPIBranch and dbo.KPICorporate
*********************************************************************************************/
--BranchMinimumSalesPriceInd
UPDATE E
SET BranchMinimumSalesPriceInd = 0
from dbo.KPIBranch E

UPDATE E
SET BranchMinimumSalesPriceInd = 1
from dbo.KPIBranch E
WHERE E.MinimumSalesPrice IS NOT NULL AND E.MinimumSalesPrice > 0.00


--NonCAT   BranchMinimumSalesPriceIndNonCAT
UPDATE E
SET BranchMinimumSalesPriceIndNonCAT = 0
from dbo.KPIBranch E

UPDATE E
SET BranchMinimumSalesPriceIndNonCAT = 1
from dbo.KPIBranch E
WHERE E.MinimumSalesPriceNonCAT IS NOT NULL AND E.MinimumSalesPriceNonCAT > 0.00



--KPIBranch
UPDATE KPIBranch
SET IsCurrentWeek = 0
FROM dbo.KPIBranch WITH(NOLOCK)
WHERE IsCurrentWeek = 1


IF @CurrentRunDayID <> @CurrentWeekLastDayOfWeekDayID
BEGIN
	UPDATE B
	SET IsCurrentWeek = 1
	FROM dbo.KPIBranch B WITH(NOLOCK)
	INNER JOIN #TMP_KPIDay D WITH(NOLOCK)
	ON B.FiscalWeekID = D.FiscalWeekID
END


--KPICorporate
UPDATE KPICorporate
SET IsCurrentWeek = 0
FROM dbo.KPICorporate WITH(NOLOCK)
WHERE IsCurrentWeek = 1

IF @CurrentRunDayID <> @CurrentWeekLastDayOfWeekDayID
BEGIN
	UPDATE B
	SET IsCurrentWeek = 1
	FROM dbo.KPICorporate B WITH(NOLOCK)
	INNER JOIN #TMP_KPIDay D WITH(NOLOCK)
	ON B.FiscalWeekID = D.FiscalWeekID
END

/*********************************************************************************************
Update IsRolling13Week for KPIBranch and dbo.KPICorporate
*********************************************************************************************/

select distinct top 13 FiscalWeekID, 
	   IDENTITY(smallint, 1, 1) AS IsRolling13Week
into #TempKPIRolling13Week	   
from dbo.KPIBranch
where IsCurrentWeek <> 1
order by FiscalWeekID desc

--KPIBranch
UPDATE KPIBranch
SET IsRolling13Week = 0
FROM dbo.KPIBranch WITH(NOLOCK)


UPDATE KB
SET IsRolling13Week = T1.IsRolling13Week
FROM dbo.KPIBranch KB
INNER JOIN #TempKPIRolling13Week T1
ON KB.FiscalWeekID = T1.FiscalWeekID


--KPICorporate
UPDATE KPICorporate
SET IsRolling13Week = 0
FROM dbo.KPICorporate WITH(NOLOCK)


UPDATE KC
SET IsRolling13Week = T2.IsRolling13Week
FROM dbo.KPICorporate KC
INNER JOIN #TempKPIRolling13Week T2
ON KC.FiscalWeekID = T2.FiscalWeekID


-- Previous Year IsRolling13Week
select (FiscalWeekID - 100) AS FiscalWeekID, IsRolling13Week
into #TempKPIRolling13Week_PreviousYear
from #TempKPIRolling13Week	 

UPDATE KB
SET IsRolling13Week = PY.IsRolling13Week
FROM dbo.KPIBranch KB
INNER JOIN #TempKPIRolling13Week_PreviousYear PY
ON KB.FiscalWeekID = PY.FiscalWeekID

UPDATE KC
SET IsRolling13Week = PY.IsRolling13Week
FROM dbo.KPICorporate KC
INNER JOIN #TempKPIRolling13Week_PreviousYear PY
ON KC.FiscalWeekID = PY.FiscalWeekID

-- CurrentYearFiscalWeekMinAverage, PriorYearFiscalWeekMinAverage
SELECT	@CurrentYearFiscalWeekMinAverage = 
		(SUM(CASE WHEN KP.BranchMinimumSalesPriceInd = 1 THEN MinimumSalesPrice ELSE 0 END)) 
		/ (CASE WHEN (SUM(KP.BranchMinimumSalesPriceInd)) = 0 THEN 1 ELSE SUM(KP.BranchMinimumSalesPriceInd)END)
FROM dbo.KPIBranch KP
INNER JOIN #TempKPIRolling13Week TP
ON TP.FiscalWeekID = KP.FiscalWeekID

UPDATE KP
SET CurrentYearFiscalWeekMinAverage = @CurrentYearFiscalWeekMinAverage
FROM dbo.KPIBranch KP
INNER JOIN #TempKPIRolling13Week TP
ON TP.FiscalWeekID = KP.FiscalWeekID
WHERE KP.IsRolling13Week = 1

SELECT	 @PriorYearFiscalWeekMinAverage = 
		 (SUM(CASE WHEN KP.BranchMinimumSalesPriceInd = 1 THEN MinimumSalesPrice ELSE 0 END)) 
		/ (CASE WHEN (SUM(KP.BranchMinimumSalesPriceInd)) = 0 THEN 1 ELSE SUM(KP.BranchMinimumSalesPriceInd)END)
FROM dbo.KPIBranch KP WITH (NOLOCK)
INNER JOIN #TempKPIRolling13Week_PreviousYear TP
ON TP.FiscalWeekID = KP.FiscalWeekID

UPDATE KP
SET PriorYearFiscalWeekMinAverage = @PriorYearFiscalWeekMinAverage
FROM dbo.KPIBranch KP
INNER JOIN #TempKPIRolling13Week TP
ON TP.FiscalWeekID = KP.FiscalWeekID
WHERE KP.IsRolling13Week = 1

-- CurrentYearFiscalWeekMinAverageNonCAT,PriorYearFiscalWeekMinAverageNonCAT
SELECT	@CurrentYearFiscalWeekMinAverageNonCAT = 
		(SUM(CASE WHEN KP.BranchMinimumSalesPriceIndNonCAT = 1 THEN MinimumSalesPriceNonCAT ELSE 0 END)) 
		/ (CASE WHEN (SUM(KP.BranchMinimumSalesPriceIndNonCAT)) = 0 THEN 1 ELSE SUM(KP.BranchMinimumSalesPriceIndNonCAT)END)
FROM dbo.KPIBranch KP WITH (NOLOCK)
INNER JOIN #TempKPIRolling13Week TP
ON TP.FiscalWeekID = KP.FiscalWeekID

UPDATE KP
SET CurrentYearFiscalWeekMinAverageNonCAT = @CurrentYearFiscalWeekMinAverageNonCAT
FROM dbo.KPIBranch KP
INNER JOIN #TempKPIRolling13Week TP
ON TP.FiscalWeekID = KP.FiscalWeekID
WHERE KP.IsRolling13Week = 1

SELECT	 @PriorYearFiscalWeekMinAverageNonCAT = 
		 (SUM(CASE WHEN KP.BranchMinimumSalesPriceIndNonCAT = 1 THEN MinimumSalesPriceNonCAT ELSE 0 END)) 
		/ (CASE WHEN (SUM(KP.BranchMinimumSalesPriceIndNonCAT)) = 0 THEN 1 ELSE SUM(KP.BranchMinimumSalesPriceIndNonCAT)END)
FROM dbo.KPIBranch KP WITH (NOLOCK)
INNER JOIN #TempKPIRolling13Week_PreviousYear TP
ON TP.FiscalWeekID = KP.FiscalWeekID

UPDATE KP
SET PriorYearFiscalWeekMinAverageNonCAT = @PriorYearFiscalWeekMinAverageNonCAT
FROM dbo.KPIBranch KP
INNER JOIN #TempKPIRolling13Week TP
ON TP.FiscalWeekID = KP.FiscalWeekID
WHERE KP.IsRolling13Week = 1

UPDATE KP
SET BuyerRevenueNonCAT = TB.BuyerRevenueNonCAT,
    BuyerRevenue = TB.BuyerRevenue
FROM dbo.KPIBranch KP
INNER JOIN  #TMP_BuyerRevnue TB 
on (TB.BranchNumber = KP.BranchNumber and TB.FiscalWeekID = KP.FiscalWeekID ) 

-- Update Previous 3 Weeks NumberOfWeekEndPickUps
UPDATE KP 
SET NumberOfWeekendPickups = tmp.NumberOfWeekendPickups,
    NumberOfWeekendPickupsNonCAT = tmp.NumberOfWeekendPickupsNonCAT
FROM [dbo].[KPIBranch] KP 
inner join #TMP_WeekendPickups tmp
on tmp.BranchNumber = KP.BranchNumber 
and tmp.fiscalweekid = KP.FiscalWeekID


-- Update UnitsSoldLast13WeeksNonCAT, UnitsSoldLast13Weeks
UPDATE KB
SET UnitsSoldLast13WeeksNonCAT = T.UnitsSoldLast13WeeksNonCAT,
	UnitsSoldLast13Weeks = T.UnitsSoldLast13Weeks
FROM dbo.KPIBranch KB
INNER JOIN (SELECT	MAX(fiscalweekID) AS FiscalWeekID,
					BranchNumber,
					SUM(UnitSoldNonCAT) AS UnitsSoldLast13WeeksNonCAT,
					SUM(UnitSold) AS UnitsSoldLast13Weeks
			FROM	dbo.KPIBranch KPI
			WHERE	FiscalWeekID IN (SELECT  DISTINCT TOP 13 FiscalWeekID 
									 FROM  dbo.KPIBranch WHERE IsRolling13Week <> 0 ORDER BY FiscalWeekID DESC)
			GROUP BY BranchNumber
			) AS T
  ON	T.FiscalWeekID = KB.FiscalWeekID
  AND	T.BranchNumber = KB.BranchNumber 



-- PipelineInventory14WeeksAgoNonCAT and PipelineInventory14WeeksAgo
UPDATE KB
SET PipelineInventory14WeeksAgoNonCAT = T2.PipelineInventory14WeeksAgoNonCAT,
	PipelineInventory14WeeksAgo = T2.PipelineInventory14WeeksAgo
--select kb.fiscalweekid, kb.branchnumber, t2.BranchNumber, T2.PipelineInventory14WeeksAgoNonCAT
FROM dbo.KPIBranch KB
INNER JOIN (
			SELECT	FiscalWeekID,
					BranchNumber,
					SUM(PipelineInventoryNonCAT) AS PipelineInventory14WeeksAgoNonCAT,
					SUM(PipelineInventory) AS PipelineInventory14WeeksAgo
			FROM	dbo.KPIBranch KPI
			WHERE	FiscalWeekID IN (	SELECT Min(FiscalWeekID)
										FROM (	SELECT  DISTINCT TOP 14 FiscalWeekID 
											FROM  dbo.KPIBranch WHERE IsCurrentWeek =  0 ORDER BY FiscalWeekID DESC
										) AS T
									 )
			GROUP BY FiscalWeekID,
					 BranchNumber
		 )AS T2
ON T2.BranchNumber = KB.BranchNumber
WHERE KB.FiscalWeekID IN (Select MAX(FiscalWeekID) FROM dbo.KPIBranch KB WHERE IsRolling13Week <> 0) 


-- PipelineInventory13WeeksAgoNonCAT and PipelineInventory13WeeksAgo

UPDATE KB
SET PipelineInventory13WeeksAgoNonCAT = T2.PipelineInventory13WeeksAgoNonCAT,
	PipelineInventory13WeeksAgo = T2.PipelineInventory13WeeksAgo
--select kb.fiscalweekid, kb.branchnumber, t2.BranchNumber, T2.PipelineInventory14WeeksAgoNonCAT
FROM dbo.KPIBranch KB
INNER JOIN (
			SELECT	FiscalWeekID,
					BranchNumber,
					SUM(PipelineInventoryNonCAT) AS PipelineInventory13WeeksAgoNonCAT,
					SUM(PipelineInventory) AS PipelineInventory13WeeksAgo
			FROM	dbo.KPIBranch KPI
			WHERE	FiscalWeekID IN (	SELECT Min(FiscalWeekID)
										FROM (	SELECT  DISTINCT TOP 13 FiscalWeekID 
											FROM  dbo.KPIBranch WHERE IsCurrentWeek =  0 ORDER BY FiscalWeekID DESC
										) AS T
									 )
			GROUP BY FiscalWeekID,
					 BranchNumber
		 )AS T2
ON T2.BranchNumber = KB.BranchNumber
WHERE KB.FiscalWeekID IN (Select MAX(FiscalWeekID) FROM dbo.KPIBranch KB WHERE IsRolling13Week <> 0) 



Update  [dbo].[KPIBranch] 
Set 
		[UnitsSoldRunAndDriveAutoOnly] = TKPI.[RunAndDriveAutoOnly]			
	,[VRDUnitsSoldRunAndDriveAutoOnly] = TKPI.[VRDRunAndDriveAutoOnly] 		
	,[UnitsSoldRunAndDriveAutoOnlyNonCAT] = TKPI.[UnitsSoldRunAndDriveAutoOnlyNonCAT]		
	,[VRDUnitsSoldRunAndDriveAutoOnlyNonCAT] = TKPI.[VRDUnitsSoldRunAndDriveAutoOnlyNonCAT]		
From [dbo].[KPIBranch] KPIBr
    Inner Join #TMP_SoldVehicleAtrributes_RunAndDrive AS TKPI
				ON TKPI.FiscalWeekID = KPIBr.FiscalWeekID AND TKPI.BranchNumber = KPIBr.BranchNumber	


/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount



		COMMIT TRAN
		
		IF OBJECT_ID('tempdb..#TMP_KPI_Branch') IS NOT NULL DROP TABLE #TMP_KPI_Branch
		IF OBJECT_ID('tempdb..#TMP_KPI_BranchRecordCount') IS NOT NULL DROP TABLE #TMP_KPI_BranchRecordCount
		IF OBJECT_ID('tempdb..#TMP_KPIDay') IS NOT NULL DROP TABLE #TMP_KPIDay
		IF OBJECT_ID('tempdb..#TMP_Budget') IS NOT NULL DROP TABLE #TMP_Budget
		IF OBJECT_ID('tempdb..#TMP_Assignments') IS NOT NULL DROP TABLE #TMP_Assignments
		IF OBJECT_ID('tempdb..#TMP_ReleasePickup') IS NOT NULL DROP TABLE #TMP_ReleasePickup
		IF OBJECT_ID('tempdb..#TMP_ReleaseToPickup') IS NOT NULL DROP TABLE #TMP_ReleaseToPickup
		IF OBJECT_ID('tempdb..#TMP_ReleaseProblem') IS NOT NULL DROP TABLE #TMP_ReleaseProblem
		IF OBJECT_ID('tempdb..#TMP_Inventory') IS NOT NULL DROP TABLE #TMP_Inventory
		IF OBJECT_ID('tempdb..#TMP_Sale') IS NOT NULL DROP TABLE #TMP_Sale
		IF OBJECT_ID('tempdb..#TMP_Sale') IS NOT NULL DROP TABLE #TMP_MinimumSalesPrice		
		IF OBJECT_ID('tempdb..#TMP_SetForSale') IS NOT NULL DROP TABLE #TMP_SetForSale
		IF OBJECT_ID('tempdb..#TMP_VolumeProceeds') IS NOT NULL DROP TABLE #TMP_VolumeProceeds
		IF OBJECT_ID('tempdb..#TMP_BidTypeSold') IS NOT NULL DROP TABLE #TMP_BidTypeSold
		IF OBJECT_ID('tempdb..#TMP_BuyerAttendance') IS NOT NULL DROP TABLE #TMP_BuyerAttendance
		IF OBJECT_ID('tempdb..#TMP_Top20Percent') IS NOT NULL DROP TABLE #TMP_Top20Percent
		IF OBJECT_ID('tempdb..#TMP_Bottom20Percent') IS NOT NULL DROP TABLE #TMP_Bottom20Percent
		IF OBJECT_ID('tempdb..#TMP_Middle60Percent') IS NOT NULL DROP TABLE #TMP_Middle60Percent
		IF OBJECT_ID('tempdb..#TMP_Insurance_GrossReturn') IS NOT NULL DROP TABLE #TMP_Insurance_GrossReturn
		IF OBJECT_ID('tempdb..#TMP_CountofPUwithIn100MilesOneBusinessDays') IS NOT NULL DROP TABLE #TMP_CountofPUwithIn100MilesOneBusinessDays
		IF OBJECT_ID('tempdb..#TMP_BranchMinimumSalesPriceInd') IS NOT NULL DROP TABLE #TMP_BranchMinimumSalesPriceInd
		IF OBJECT_ID('tempdb..#TMP_Sold_ACV') IS NOT NULL DROP TABLE #TMP_Sold_ACV
		IF OBJECT_ID('tempdb..#TMP_SoldVehicleAtrributes') IS NOT NULL DROP TABLE #TMP_SoldVehicleAtrributes
		IF OBJECT_ID('tempdb..#TMP_IBF') IS NOT NULL DROP TABLE #TMP_IBF
		IF OBJECT_ID('tempdb..#TMP_BuyerMetrics') IS NOT NULL DROP TABLE #TMP_BuyerMetrics
		IF OBJECT_ID('tempdb..#TMP_Insurance_GrossReturn_NonCAT') IS NOT NULL DROP TABLE #TMP_Insurance_GrossReturn_NonCAT
		IF OBJECT_ID('tempdb..#TMP_Top20Percent_StockID_NonCAT') IS NOT NULL DROP TABLE #TMP_Top20Percent_StockID_NonCAT
		IF OBJECT_ID('tempdb..#TMP_Top20Percent_NonCAT') IS NOT NULL DROP TABLE #TMP_Top20Percent_NonCAT
		IF OBJECT_ID('tempdb..#TMP_Bottom20Percent_StockID_NonCAT') IS NOT NULL DROP TABLE #TMP_Bottom20Percent_StockID_NonCAT
		IF OBJECT_ID('tempdb..#TMP_Bottom20Percent_NonCAT') IS NOT NULL DROP TABLE #TMP_Bottom20Percent_NonCAT
		IF OBJECT_ID('tempdb..#TMP_Middle60Percent_StockID_NonCAT') IS NOT NULL DROP TABLE #TMP_Middle60Percent_StockID_NonCAT
		IF OBJECT_ID('tempdb..#TMP_Middle60Percent_NonCAT') IS NOT NULL DROP TABLE #TMP_Middle60Percent_NonCAT
		IF OBJECT_ID('tempdb..#TMP_ReleaseProblemNonCAT') IS NOT NULL DROP TABLE #TMP_ReleaseProblemNonCAT
		IF OBJECT_ID('tempdb..#TMP_BuyerRevnue') IS NOT NULL DROP TABLE #TMP_BuyerRevnue
		IF OBJECT_ID('tempdb..#TMP_WeekendPickups') IS NOT NULL DROP TABLE #TMP_WeekendPickups

		IF OBJECT_ID('tempdb..#TMP_KPIDayForLast4Weeks') IS NOT NULL DROP TABLE #TMP_KPIDayForLast4Weeks
		IF OBJECT_ID('tempdb..#TMP_SoldVehicleAtrributes_RunAndDrive') IS NOT NULL DROP TABLE #TMP_SoldVehicleAtrributes_RunAndDrive
		
		--EXEC sp_recompile '[dbo].[usp_ETLKPI]'

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
		
		IF OBJECT_ID('tempdb..#TMP_KPI_Branch') IS NOT NULL DROP TABLE #TMP_KPI_Branch
		IF OBJECT_ID('tempdb..#TMP_KPI_BranchRecordCount') IS NOT NULL DROP TABLE #TMP_KPIRecordCount
		IF OBJECT_ID('tempdb..#TMP_KPIDay') IS NOT NULL DROP TABLE #TMP_KPIDay
		IF OBJECT_ID('tempdb..#TMP_Budget') IS NOT NULL DROP TABLE #TMP_Budget
		IF OBJECT_ID('tempdb..#TMP_Assignments') IS NOT NULL DROP TABLE #TMP_Assignments
		IF OBJECT_ID('tempdb..#TMP_ReleasePickup') IS NOT NULL DROP TABLE #TMP_ReleasePickup
		IF OBJECT_ID('tempdb..#TMP_ReleaseToPickup') IS NOT NULL DROP TABLE #TMP_ReleaseToPickup
		IF OBJECT_ID('tempdb..#TMP_ReleaseProblem') IS NOT NULL DROP TABLE #TMP_ReleaseProblem
		IF OBJECT_ID('tempdb..#TMP_Inventory') IS NOT NULL DROP TABLE #TMP_Inventory
		IF OBJECT_ID('tempdb..#TMP_Sale') IS NOT NULL DROP TABLE #TMP_Sale
		IF OBJECT_ID('tempdb..#TMP_Sale') IS NOT NULL DROP TABLE #TMP_MinimumSalesPrice	
		IF OBJECT_ID('tempdb..#TMP_SetForSale') IS NOT NULL DROP TABLE #TMP_SetForSale
		IF OBJECT_ID('tempdb..#TMP_VolumeProceeds') IS NOT NULL DROP TABLE #TMP_VolumeProceeds
		IF OBJECT_ID('tempdb..#TMP_BidTypeSold') IS NOT NULL DROP TABLE #TMP_BidTypeSold
		IF OBJECT_ID('tempdb..#TMP_BuyerAttendance') IS NOT NULL DROP TABLE #TMP_BuyerAttendance
		IF OBJECT_ID('tempdb..#TMP_Top20Percent') IS NOT NULL DROP TABLE #TMP_Top20Percent
		IF OBJECT_ID('tempdb..#TMP_Bottom20Percent') IS NOT NULL DROP TABLE #TMP_Bottom20Percent
		IF OBJECT_ID('tempdb..#TMP_Middle60Percent') IS NOT NULL DROP TABLE #TMP_Middle60Percent
		IF OBJECT_ID('tempdb..#TMP_Insurance_GrossReturn') IS NOT NULL DROP TABLE #TMP_Insurance_GrossReturn
		IF OBJECT_ID('tempdb..#TMP_CountofPUwithIn100MilesOneBusinessDays') IS NOT NULL DROP TABLE #TMP_CountofPUwithIn100MilesOneBusinessDays
		IF OBJECT_ID('tempdb..#TMP_BranchMinimumSalesPriceInd') IS NOT NULL DROP TABLE #TMP_BranchMinimumSalesPriceInd
		IF OBJECT_ID('tempdb..#TMP_Sold_ACV') IS NOT NULL DROP TABLE #TMP_Sold_ACV
		IF OBJECT_ID('tempdb..#TMP_SoldVehicleAtrributes') IS NOT NULL DROP TABLE #TMP_SoldVehicleAtrributes
		IF OBJECT_ID('tempdb..#TMP_IBF') IS NOT NULL DROP TABLE #TMP_IBF
		IF OBJECT_ID('tempdb..#TMP_BuyerMetrics') IS NOT NULL DROP TABLE #TMP_BuyerMetrics
		IF OBJECT_ID('tempdb..#TMP_Insurance_GrossReturn_NonCAT') IS NOT NULL DROP TABLE #TMP_Insurance_GrossReturn_NonCAT
		IF OBJECT_ID('tempdb..#TMP_Top20Percent_StockID_NonCAT') IS NOT NULL DROP TABLE #TMP_Top20Percent_StockID_NonCAT
		IF OBJECT_ID('tempdb..#TMP_Top20Percent_NonCAT') IS NOT NULL DROP TABLE #TMP_Top20Percent_NonCAT
		IF OBJECT_ID('tempdb..#TMP_Bottom20Percent_StockID_NonCAT') IS NOT NULL DROP TABLE #TMP_Bottom20Percent_StockID_NonCAT
		IF OBJECT_ID('tempdb..#TMP_Bottom20Percent_NonCAT') IS NOT NULL DROP TABLE #TMP_Bottom20Percent_NonCAT
		IF OBJECT_ID('tempdb..#TMP_Middle60Percent_StockID_NonCAT') IS NOT NULL DROP TABLE #TMP_Middle60Percent_StockID_NonCAT
		IF OBJECT_ID('tempdb..#TMP_Middle60Percent_NonCAT') IS NOT NULL DROP TABLE #TMP_Middle60Percent_NonCAT
		IF OBJECT_ID('tempdb..#TMP_ReleaseProblemNonCAT') IS NOT NULL DROP TABLE #TMP_ReleaseProblemNonCAT
		IF OBJECT_ID('tempdb..#TMP_BuyerRevnue') IS NOT NULL DROP TABLE #TMP_BuyerRevnue
		IF OBJECT_ID('tempdb..#TMP_WeekendPickups') IS NOT NULL DROP TABLE #TMP_WeekendPickups

		IF OBJECT_ID('tempdb..#TMP_KPIDayForLast4Weeks') IS NOT NULL DROP TABLE #TMP_KPIDayForLast4Weeks
		IF OBJECT_ID('tempdb..#TMP_SoldVehicleAtrributes_RunAndDrive') IS NOT NULL DROP TABLE #TMP_SoldVehicleAtrributes_RunAndDrive

		RETURN -1
	END CATCH
END




GO


