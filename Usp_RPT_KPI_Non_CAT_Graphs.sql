
/*        
USE EDW        
GO         
==============================================================================        
Author    : Yogitha Alwarsetty        
--Create date  : 10/09/2013        
Description  :           

==============================================================================        
Usage Example:        
Exec USP_RPT_KPI_Non_CAT_Graphs    
==============================================================================        
*/ 
CREATE PROCEDURE [dbo].[Usp_RPT_KPI_Non_CAT_Graphs] 
AS 
  BEGIN 
      SET nocount ON; 

      IF Object_id('tempdb..#TEMP_Currentyear ') IS NOT NULL 
        DROP TABLE #temp_currentyear 

      CREATE TABLE #temp_currentyear 
        ( 
           currentfiscalweeknum                          INT, 
           currentyearinsuranceacvnoncat                 MONEY, 
           currentyearinsuranceunitssoldnoncat           INT, 
           currentyearinsurancegrossreturnnoncat         MONEY, 
           currentyearinsurancebottom20grossreturnnoncat MONEY, 
           currentyearinsurancebottom20unitsnoncat       INT, 
           currentyearinsurancetop20grossreturnnoncat    MONEY, 
           currentyearinsurancetop20unitsnoncat          INT, 
           currentyearinsurancemiddle60grossreturnnoncat MONEY, 
           currentyearinsurancemiddle60unitsnoncat       INT 
        ) 

      IF Object_id('tempdb..#TEMP_Prioryear') IS NOT NULL 
        DROP TABLE #temp_prioryear 

      CREATE TABLE #temp_prioryear 
        ( 
           priorfiscalweeknum                          INT, 
           prioryearinsuranceacvnoncat                 MONEY, 
           prioryearinsuranceunitssoldnoncat           INT, 
           prioryearinsurancegrossreturnnoncat         MONEY, 
           prioryearinsurancebottom20grossreturnnoncat MONEY, 
           prioryearinsurancebottom20unitsnoncat       INT, 
           prioryearinsurancetop20grossreturnnoncat    MONEY, 
           prioryearinsurancetop20unitsnoncat          INT, 
           prioryearinsurancemiddle60grossreturnnoncat MONEY, 
           prioryearinsurancemiddle60unitsnoncat       INT 
        ) 

      INSERT INTO #temp_currentyear 
                  (currentfiscalweeknum, 
                   currentyearinsuranceacvnoncat, 
                   currentyearinsuranceunitssoldnoncat, 
                   currentyearinsurancegrossreturnnoncat, 
                   currentyearinsurancebottom20grossreturnnoncat, 
                   currentyearinsurancebottom20unitsnoncat, 
                   currentyearinsurancetop20grossreturnnoncat, 
                   currentyearinsurancetop20unitsnoncat, 
                   currentyearinsurancemiddle60grossreturnnoncat, 
                   currentyearinsurancemiddle60unitsnoncat) 
      SELECT Cast(RIGHT(Cast(kc.fiscalweekid AS VARCHAR(100)), 2) AS INT)AS 
             CurrentFiscalweekNum, 
             Sum(Isnull(insuranceacvnoncat, 0)) 
             CurrentyearInsuranceACVNonCAT, 
             Sum(Isnull(insuranceunitssoldnoncat, 0)) 
             CurrentyearInsuranceUnitsSoldNonCAT, 
             Sum(Isnull(insurancegrossreturnnoncat, 0)) 
             CurrentyearInsuranceGrossReturnNonCAT, 
             Sum(Isnull(insurancebottom20grossreturnnoncat, 0)) 
             CurrentyearInsuranceBottom20GrossReturnNonCAT, 
             Sum(Isnull(insurancebottom20unitsnoncat, 0)) 
             CurrentyearInsuranceBottom20UnitsNonCAT, 
             Sum(Isnull(insurancetop20grossreturnnoncat, 0)) 
             currentyearInsuranceTop20GrossReturnNonCAT, 
             Sum(Isnull(insurancetop20unitsnoncat, 0)) 
             currentyearInsuranceTop20UnitsNonCAT, 
             Sum(Isnull(insurancemiddle60grossreturnnoncat, 0)) 
             currentyearInsuranceMiddle60GrossReturnNonCAT, 
             Sum(Isnull(insurancemiddle60unitsnoncat, 0)) 
             currentyearInsuranceMiddle60UnitsNonCAT 
      FROM   dbo.kpicorporate Kc 
      WHERE  isrolling13week BETWEEN 1 AND 13 
             AND Cast(LEFT(Cast(kc.fiscalweekid AS VARCHAR(100)), 4) AS INT) = 
                 ( Year(Getdate()) ) 
      GROUP  BY Cast(RIGHT(Cast(kc.fiscalweekid AS VARCHAR(100)), 2) AS INT) 

      INSERT INTO #temp_prioryear 
                  (priorfiscalweeknum, 
                   prioryearinsurancebottom20grossreturnnoncat, 
                   prioryearinsurancebottom20unitsnoncat, 
                   prioryearinsurancetop20grossreturnnoncat, 
                   prioryearinsurancetop20unitsnoncat, 
                   prioryearinsurancemiddle60grossreturnnoncat, 
                   prioryearinsurancemiddle60unitsnoncat, 
                   prioryearinsuranceacvnoncat, 
                   prioryearinsuranceunitssoldnoncat, 
                   prioryearinsurancegrossreturnnoncat) 
      SELECT Cast(RIGHT(Cast(kc.fiscalweekid AS VARCHAR(100)), 2) AS INT)AS 
             PriorFiscalweekNum, 
             Sum(Isnull(insurancebottom20grossreturnnoncat, 0)) 
             PrioryearInsuranceBottom20GrossReturnNonCAT, 
             Sum(Isnull(insurancebottom20unitsnoncat, 0)) 
             PrioryearInsuranceBottom20UnitsNonCAT, 
             Sum(Isnull(insurancetop20grossreturnnoncat, 0)) 
             PrioryearInsuranceTop20GrossReturnNonCAT, 
             Sum(Isnull(insurancetop20unitsnoncat, 0)) 
             PrioryearInsuranceTop20UnitsNonCAT, 
             Sum(Isnull(insurancemiddle60grossreturnnoncat, 0)) 
             PrioryearInsuranceMiddle60GrossReturnNonCAT, 
             Sum(Isnull(insurancemiddle60unitsnoncat, 0)) 
             PrioryearInsuranceMiddle60UnitsNonCAT, 
             Sum(Isnull(insuranceacvnoncat, 0)) 
             PrioryearInsuranceACVNonCAT, 
             Sum(Isnull(insuranceunitssoldnoncat, 0)) 
             PrioryearInsuranceUnitsSoldNonCAT, 
             Sum(Isnull(insurancegrossreturnnoncat, 0)) 
             PrioryearInsuranceGrossReturnNonCAT 
      FROM   dbo.kpicorporate Kc 
      WHERE  isrolling13week BETWEEN 1 AND 13 
             AND Cast(LEFT(Cast(kc.fiscalweekid AS VARCHAR(100)), 4) AS INT) = ( 
                 Year(Getdate()) - 1 ) 
      GROUP  BY Cast(RIGHT(Cast(kc.fiscalweekid AS VARCHAR(100)), 2) AS INT) 

      SELECT * 
      FROM   #temp_currentyear cy 
             INNER JOIN #temp_prioryear py 
                     ON cy.currentfiscalweeknum = py.priorfiscalweeknum 
  END 


GO


