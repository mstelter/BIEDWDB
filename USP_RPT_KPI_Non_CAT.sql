
/*     
USE EDW     
GO      
==============================================================================     
Author    : Yogitha Alwarsetty     
--Create date  : 10/03/2013     
Description  :        

==============================================================================     
Usage Example:     
Exec USP_RPT_KPI_Non_CAT 
==============================================================================     
*/ 

CREATE PROCEDURE [dbo].[USP_RPT_KPI_Non_CAT]
AS 
  BEGIN 
      SET nocount ON; 

      DECLARE @currentquarterid INT 
      DECLARE @currentweekid INT 
      DECLARE @currentmonthid INT 
      DECLARE @currentyearid INT 
      DECLARE @PreviousWeekid INT

      SET @currentquarterid = (SELECT DISTINCT Max(fiscalquarterid) 
                               FROM   kpibranch kb WITH (nolock) 
                                      INNER JOIN dimday dd WITH (nolock) 
                                              ON ( dd.dayid = kb.lastdayofweekid 
                                                 ) 
                               WHERE  isrolling13week = 1) 
      SET @currentyearid = (SELECT DISTINCT Max(fiscalyearid) 
                            FROM   kpibranch kb WITH (nolock) 
                                   INNER JOIN dimday dd WITH (nolock) 
                                           ON ( dd.dayid = kb.lastdayofweekid ) 
                            WHERE  isrolling13week = 1) 
      SET @currentweekid = (SELECT DISTINCT Max(kb.fiscalweekid) 
                            FROM   kpibranch kb WITH (nolock) 
                                   INNER JOIN dimday dd WITH (nolock) 
                                           ON ( dd.dayid = kb.lastdayofweekid ) 
                            WHERE  isrolling13week = 1) 
      SET @PreviousWeekid =(select Max (kb.FiscalWeekID) FROM   kpibranch kb WITH (nolock) 
                                   INNER JOIN dimday dd WITH (nolock) 
                                           ON ( dd.dayid = kb.lastdayofweekid ) 
                          where FiscalYearID = Year(getDate())-1
                            and Month(Daydate)=month(getDate())
                            and WeekNum = DATEPART(WEEK,(getdate()))
                                  and  isrolling13week = 1 )  
                      
      SET @currentmonthid = (SELECT DISTINCT Max(fiscalmonthid) 
                             FROM   kpibranch kb WITH (nolock) 
                                    INNER JOIN dimday dd WITH (nolock) 
                                            ON ( dd.dayid = kb.lastdayofweekid ) 
                             WHERE  isrolling13week = 1) 

      IF Object_id('tempdb..#tmp_fiscalweekid') IS NOT NULL 
        DROP TABLE #tmp_fiscalweekid 

      CREATE TABLE #tmp_fiscalweekid 
        ( 
           currentfiscalweekid [INT] NOT NULL
           
       ) 

      INSERT INTO #tmp_fiscalweekid 
                  (currentfiscalweekid ) 
      SELECT DISTINCT fiscalweekid currentfiscalweekid
      FROM   dimday 
      WHERE  yearid = Year(Getdate()) 

      IF Object_id('tempdb..#tmp_weekselection') IS NOT NULL 
        DROP TABLE #tmp_weekselection 

      CREATE TABLE #tmp_weekselection 
        ( 
           currentfiscalweekid [INT] NOT NULL,
           currentmonth        [INT], 
           currentyear         [INT] 
        ) 

      INSERT INTO #tmp_weekselection 
                  (currentfiscalweekid, 
                   
                   currentmonth, 
                   currentyear) 
      SELECT currentfiscalweekid, 
             
             CASE 
               WHEN fiscalmonthid = @currentmonthid THEN 1 
               ELSE 0 
             END            Currentmonth, 
             @currentyearid CurrentYear 
      FROM   #tmp_fiscalweekid tfw 
             INNER JOIN dimday dd WITH (nolock) 
                     ON ( dd.fiscalweekid = tfw.currentfiscalweekid ) 
      UNION 
      SELECT ( currentfiscalweekid - 100 ), 
             
             CASE 
               WHEN fiscalmonthid = @currentmonthid - 100 THEN 1 
               ELSE 0 
             END, 
             @currentyearid - 1 
      FROM   #tmp_fiscalweekid tfw 
             INNER JOIN dimday dd WITH (nolock) 
                     ON ( dd.fiscalweekid = tfw.currentfiscalweekid - 100 ) 

     

      SELECT dd.fiscalweekid, 
            
            CAST(Right(CAST(dd.FiscalWeekID AS VARCHAR(100)), 2) AS INT) As FiscalweekNum,
             dd.fiscalyearid, 
             Kb.isrolling13week, 
             fiscalquarterid, 
             regionname, 
             branchname, 
             currentmonth, 
             currentyear, 
             Sum(Isnull(unitsnetassignednoncat, 0)) 
             UnitsNetAssignedNonCAT, 
             Sum(Isnull(UnitsAssignedPlan, 0))             UnitsAssignedPlan, 
             Sum(Isnull(UnitsAssignedPlanNonCAT, 0))      UnitsAssignedPlanNonCAT      , 
             Sum(Isnull(electronicassignmentsnoncat, 0)) 
             ElectronicAssignmentsNonCAT 
             , 
             Sum(Isnull(cancelledassignmentsnoncat, 0)) 
             CancelledAssignmentsNonCAT, 
             Sum(Isnull(unitsassignednoncat, 0))           UnitsAssignedNonCAT, 
             Sum(Isnull(calendardaysreleasetopickupnoncat, 0)) 
             CalendarDaysReleasetoPickupNonCAT, 
             Sum(Isnull(numberofpickupsnoncat, 0))         NumberofPickupsNonCAT 
             , 
             Sum(Isnull( 
             numberofpickups100mileswithinonebusinessdaysnoncat, 0)) 
             NumberofPickups100MileswithinoneBusinessDaysNonCAT, 
             Sum(Isnull(countofpuwithin100milesonebusinessdaysnoncat, 0)) 
             CountofPUwithIn100MilesOneBusinessDaysNonCAT, 
             Sum(Isnull(numberofpickupssamedayreleasedbynoonwithin35milesnoncat, 
                 0 
                 )) 
             NumberofPickupsSameDayReleasedByNoonWithin35MilesNonCAT, 
             Sum(Isnull(numberofreleaseproblemsnoncat, 0)) 
             NumberofReleaseProblemsNonCAT, 
             Sum(Isnull(numberofweekendpickupsnoncat, 0)) 
             NumberOfWeekendPickupsNonCAT, 
             Sum(Isnull(unitsoldnoncat, 0))                UnitSoldNonCAT, 
             Sum(Isnull(unitsoldplan, 0))                  UnitSoldPlan, 
             Sum(Isnull(unitssetforsalenoncat, 0))         UnitsSetForSaleNonCAT 
             , 
             Sum(Isnull(totalsalepricenoncat, 0)) 
             TotalSalePriceNonCAT, 
             Sum(Isnull(providerrevenuenoncat, 0))         ProviderRevenueNonCAT 
             , 
             Sum(Isnull(parevenuenoncat, 0)) 
             PARevenueNonCAT, 
             Sum(Isnull(paunitsold, 0))                    PAUnitSoldNonCAT, 
             Sum(Isnull(providernetrevenuenoncat, 0)) 
             ProviderNetRevenueNonCAT, 
             Sum(Isnull(buyerrevenuenoncat, 0))            BuyerRevenueNonCAT, 
             Sum(Isnull(towcostnoncat, 0))                 TowCostNonCAT, 
             Sum(Isnull(minimumsalespricenoncat, 0)) 
             MinimumSalesPriceNonCAT 
             , 
             Sum(Isnull(branchminimumsalespriceindnoncat, 0)) 
             BranchMinimumSalesPriceIndNonCAT, 
             Sum(Isnull(kb.insuranceunitssoldnoncat, 0)) 
             InsuranceUnitsSoldNonCAT, 
             Sum(Isnull(kb.insurancesalepricenoncat, 0)) 
             InsuranceSalePriceNonCAT, 
             Sum(Isnull(kb.insuranceacvnoncat, 0))            InsuranceACVNonCAT, 
             Sum(Isnull(kb.insurancegrossreturnnoncat, 0)) 
             InsuranceGrossReturnNonCAT, 
             Sum(Isnull(charityunitssoldnoncat, 0)) 
             CharityUnitsSoldNonCAT, 
             Sum(Isnull(charitytotalsalepricenoncat, 0)) 
             CharityTotalSalePriceNonCAT 
             , 
             Sum(Isnull(rentalunitssoldnoncat, 0)) 
             RentalUnitsSoldNonCAT, 
             Sum(Isnull(rentaltotalsalepricenoncat, 0)) 
             RentalTotalSalePriceNonCAT, 
             Sum(Isnull(vrdunitssoldnoncat, 0))            VRDUnitsSoldNonCAT, 
             Sum(Isnull(vrdtotalsalepricenoncat, 0)) 
             VRDTotalSalePriceNonCAT 
             , 
             Sum(Isnull(otherunitssoldnoncat, 0)) 
             OtherUnitsSoldNonCAT, 
             Sum(Isnull(othertotalsalepricenoncat, 0)) 
             OtherTotalSalePriceNonCAT, 
             Sum(Isnull(ibidsoldunitsnoncat, 0))           IBIDSoldUnitsNonCAT, 
             Sum(Isnull(proxysoldunitsnoncat, 0))          ProxySoldUnitsNonCAT, 
             Sum(Isnull(ibfsoldunitsnoncat, 0))            IBFSoldUnitsNonCAT, 
             Sum(Isnull(buyerattendancenoncat, 0))         BuyerAttendanceNonCAT 
             , 
             Sum(Isnull(successfulbuyernoncat, 0)) 
             SuccessfulBuyerNonCAT, 
             Sum(Isnull(outofcountrybuyerattendancenoncat, 0)) 
             OutofCountryBuyerAttendanceNonCAT, 
             Sum(Isnull(outofcountrysoldunitsnoncat, 0)) 
             OutofCountrySoldUnitsNonCAT 
             , 
             Sum(Isnull(outofstatesoldunitsnoncat, 0)) 
             OutofStateSoldUnitsNonCAT, 
            
             Sum(Isnull(guestattendancenoncat, 0))         GuestAttendanceNonCAT ,
              Sum(Isnull(pipelineinventorynoncat, 0)) 
             PipelineInventoryNonCAT 
            
      FROM   dbo.kpibranch kb WITH (nolock) 
           
             INNER JOIN dimday dd WITH (nolock) 
                     ON dd.dayid = kb.lastdayofweekid 
             INNER JOIN dbo.DimBranch db WITH (nolock) 
                     ON ( db.branchnumber = kb.branchnumber ) 
             INNER JOIN #tmp_weekselection tws 
                     ON ( tws.currentfiscalweekid = kb.fiscalweekid ) 
     
      GROUP  BY dd.fiscalweekid, 
                kb.isrolling13week, 
                fiscalquarterid, 
                dd.fiscalyearid, 
                regionname, 
                branchname, 
                currentmonth, 
                currentyear,
               CAST(Right(CAST(dd.FiscalWeekID AS VARCHAR(100)), 2) AS INT)
               
               
      ORDER  BY 1 

     
 END












GO


