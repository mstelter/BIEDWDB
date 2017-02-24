/* 
USE EDW
GO 
==============================================================================
Author            : Sidd Murao
Create date : 01/30/2012
Description : Populate the EDW Inventory table every day
==============================================================================
REVISION History
2/7/2012    Sidd Murao    Added logic to use ETL Run Datetime to check for inventory
2/8/2012    Sidd Murao    Added logic to delete duplicate rows
3/7/2011    Sidd Murao    Added new logic for Physcial inventory
3/8/2011    Sidd Murao    Added Merge to take care of duplicate inserts and added roll back
3/9/2011    Sidd Murao    Added Filter for stock selection
4/13/2012	Sidd Murao	  Update logic for @currentDateTime
4/13/2012	Juan X Jacome	Update logic to soldId > currentDateTime
4/18/2012	Juan X Jacome	Update logic for #TMP_stock_selection, removed join to table LKStockID
5/03/2012	Juan X Jacome	Removed # for all fields used in sp
7/16/2012   Sidd Murao     Exclude BidFast Assignment
3/8/2013	Juan X. Jacome	Addeded PipelineInventoryNonCAT
==============================================================================
Usage Example:
Exec usp_ETLFactInventory
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLFactInventory]

AS
BEGIN
SET NOCOUNT ON;
BEGIN TRY

            DECLARE @intErrorCode INT
                    , @intRecordCount INT
                    ,@ETLLoadID int
              ,@currentDateTime DateTime
              ,@currentdayid int
              ,@InsertRowCount_sp int 
              ,@UpdateRowCount int
              ,@ExtractRowCount int

set @currentDateTime = 

(select 

case 

when datepart(hh,MaxDatetime) between 0 and 14 then DateAdd(HOUR,23,CONVERT(DATETIME,CONVERT(varchar(8), MaxDatetime - 1,112),114))

else MaxDatetime

end 

from dbo.ETLRUN with (Nolock)

where ETLJobName = 'DailyETLJob')


set @currentdayid = CONVERT(varchar(8), @currentDateTime,112)

--select @currentdayid 

--Run the Audit begin                 
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLFactInventory'


--IF OBJECT_ID('tempdb..#temp_FSCTRecordCount') IS NOT NULL DROP TABLE #temp_FSCTRecordCount
CREATE TABLE #temp_FSCTRecordCount
(ChangeType VARCHAR(30))

/* Stock Selection for both Physical and Pipeline Stocks.  */
CREATE TABLE dbo.#TMP_stock_selection(
stockid int,
AdministrativeBranchNumber int,
CurrentProviderID int,
Day_id int,
Physicaldatetime datetime,
Assignmentdatetime datetime,
TitleindayID int,
Titleind int,
isonhold int,
IsCAT int)
--drop table #TMP_stock_selection
--drop table #TMP_Physical
--drop table #TMP_Pipeline
/* This table stores Physical Inventory values  */
CREATE TABLE dbo.#TMP_Physical (
            AdministrativeBranchNumber int,
            CurrentProviderID int,
            Day_id int,
            PhysicalInventory int,
			PhysicalInventory030Days int,
            PhysicalInventory3160Days int,
            PhysicalInventory6190Days int,
            PhysicalInventory91120Days int,
            PhysicalInventory121180Days int,
            PhysicalInventory181365Days int,
            PhysicalInventory365Days int )   
            
/* This table stores Pipeline Inventory values  */
CREATE TABLE dbo.#TMP_Pipeline (
            AdministrativeBranchNumber int,
            Day_id int,
            CurrentProviderID int,
            PipelineInventory int,
			PipelineInventoryNonCAT int,
            PipelineInventorywoTitle int,
            PipelineInventory030Days int,
            PipelineInventory3160Days int,
            PipelineInventory6190Days int,
            PipelineInventory91120Days int,
            PipelineInventory121180Days int,
            PipelineInventory181365Days int,
            PipelineInventoryover365Days int,
            PipelineInventorywoTitleover365Days int,
            PipelineInventorywoTitle030Days int,
            PipelineInventorywoTitle3160Days int,
            PipelineInventorywoTitle6190Days int,
            PipelineInventorywoTitle91120Days int,
            PipelineInventorywoTitle121180Days int,
            PipelineInventorywoTitle181365Days int,
            PipelineInventoryExcludingHolds int,
            PipelineInventoryExcludingHolds030Days int,
            PipelineInventoryExcludingHolds3160Days INT,
            PipelineInventoryExcludingHolds6190Days int,
            PipelineInventoryExcludingHolds91120Days int,
            PipelineInventoryExcludingHolds121180Days int,
            PipelineInventoryExcludingHolds181365Days int,
            PipelineInventoryExcludingHoldsover365Days int
            
            )       


     INSERT INTO #TMP_stock_selection (
     ds.stockid ,
     AdministrativeBranchNumber ,
      CurrentProviderID ,
      Day_id ,
     Physicaldatetime,
     AssignmentDateTime,
     TitleindayID,
     Titleind,
     isonhold,
	 IsCAT)
     
     select 
             ds.stockid,
            ds.AdministrativeBranchNumber ,
            ds.CurrentProviderID ,
            @currentdayid,
            case when fstt.LogInDayID <> 29991231 then fstt.LogInDatetime
            when fstt.pickupDayID <> 29991231 then fstt.pickupDatetime
            when fstt.CheckinDayID <> 29991231 then fstt.CheckinDatetime
            else '12/31/2999' end Physicaldatetime,
            fstt.AssignmentDateTime,
            fstt.TitleindayID,
            Case when ds.CurrentStockStatusCode IN ('CKI','F17','F20','F22','F25','G20','H05','H10',
                                                                  'H15','H20','H25','H30','ITB','J05','J20','M73','M79','OSP','TOS')
            then 1 else 0 end ,
            ds.isonhold,
			DS.IsCAT
          from DimStock ds with (nolock) 
		  inner join dbo.FactStockCycleTime fstt with (nolock)
            on (ds.StockID=fstt.stockid)
          where fstt.assignmentDayID <= @currentdayid
            and fstt.canceldayid = 29991231
            and fstt.solddayid > @currentdayid
            and assignmenttypecode not in ('TOW','TTL','BFB')
            
            
            
            
            
      INSERT INTO  #TMP_Physical 
       (
           AdministrativeBranchNumber,
            CurrentProviderID ,
            Day_id ,
            PhysicalInventory ,
            PhysicalInventory030Days ,
            PhysicalInventory3160Days ,
            PhysicalInventory6190Days ,
            PhysicalInventory91120Days ,
            PhysicalInventory121180Days ,
            PhysicalInventory181365Days ,
            PhysicalInventory365Days  )
      
            select 
            fstt.AdministrativeBranchNumber,
            fstt.CurrentProviderID,
            CONVERT(VARCHAR(8), @currentDateTime, 112) Day_id,
            COUNT(*) AS PhysicalInventory,
            SUM(CASE WHEN DATEDIFF(dd, fstt.Physicaldatetime,@currentDateTime ) BETWEEN 0 AND 30
            THEN 1 ELSE 0 END)  AS PhysicalInventory030Days,
                        SUM(CASE WHEN DATEDIFF(dd, fstt.Physicaldatetime,@currentDateTime ) BETWEEN 31 AND 60
            THEN 1 ELSE 0 END)  AS PhysicalInventory3160Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.Physicaldatetime,@currentDateTime ) BETWEEN 61 AND 90
            THEN 1 ELSE 0 END)  AS PhysicalInventory6190Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.Physicaldatetime,@currentDateTime ) BETWEEN 91 AND 120
            THEN 1 ELSE 0 END)  AS PhysicalInventory91120Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.Physicaldatetime,@currentDateTime ) BETWEEN 121 AND 180
            THEN 1 ELSE 0 END)  AS PhysicalInventory121180Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.Physicaldatetime,@currentDateTime ) BETWEEN 181 AND 365
            THEN 1 ELSE 0 END)  AS PhysicalInventory181365Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.Physicaldatetime,@currentDateTime )>365
            THEN 1 ELSE 0 END)  AS PhysicalInventory365Days      
      
            from  #TMP_stock_selection fstt with (nolock)
            
            where CONVERT(VARCHAR(8),Physicaldatetime, 112) <= @currentdayid
      
            group by          AdministrativeBranchNumber,
            CurrentProviderID






            
            INSERT INTO #TMP_Pipeline (
            AdministrativeBranchNumber ,
            CurrentProviderID ,
            Day_id ,
            PipelineInventory,
			PipelineInventoryNonCAT,
            PipelineInventorywoTitle ,
            PipelineInventory030Days ,
            PipelineInventory3160Days ,
            PipelineInventory6190Days ,
            PipelineInventory91120Days ,
            PipelineInventory121180Days ,
            PipelineInventory181365Days ,
            PipelineInventoryover365Days ,
            PipelineInventorywoTitleover365Days ,
            PipelineInventorywoTitle030Days ,
            PipelineInventorywoTitle3160Days ,
            PipelineInventorywoTitle6190Days ,
            PipelineInventorywoTitle91120Days ,
            PipelineInventorywoTitle121180Days ,
            PipelineInventorywoTitle181365Days ,
            PipelineInventoryExcludingHolds ,
            PipelineInventoryExcludingHolds030Days ,
            PipelineInventoryExcludingHolds3160Days,
            PipelineInventoryExcludingHolds6190Days ,
            PipelineInventoryExcludingHolds91120Days ,
            PipelineInventoryExcludingHolds121180Days ,
            PipelineInventoryExcludingHolds181365Days ,
            PipelineInventoryExcludingHoldsover365Days  )
            
            select 
            fstt.AdministrativeBranchNumber,
            fstt.CurrentProviderID,
            CONVERT(VARCHAR(8), @currentDateTime, 112) Day_id,
            COUNT(*) AS PipelineInventory,
            SUM(CASE WHEN fstt.IsCAT = 0 THEN 1 ELSE 0 END) AS PipelineInventoryNonCAT,
			SUM(case when fstt.TitleindayID > CONVERT(VARCHAR(8),@currentDateTime, 112) OR  fstt.Titleind=1 then 1 else 0 end )
            AS PipelineInventorywoTitle,
            SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime ) BETWEEN 0 AND 30
            THEN 1 ELSE 0 END)  AS PipelineInventory030Days,
                        SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime ) BETWEEN 31 AND 60
            THEN 1 ELSE 0 END)  AS PipelineInventory3160Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime ) BETWEEN 61 AND 90
            THEN 1 ELSE 0 END)  AS PipelineInventory6190Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime) BETWEEN 91 AND 120
            THEN 1 ELSE 0 END)  AS PipelineInventory91120Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime ) BETWEEN 121 AND 180
            THEN 1 ELSE 0 END)  AS PipelineInventory121180Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime) BETWEEN 181 AND 365
            THEN 1 ELSE 0 END)  AS PipelineInventory181365Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime)>365
            THEN 1 ELSE 0 END)  AS PipelineInventoryover365Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime)>365 
            and (fstt.TitleindayID > CONVERT(VARCHAR(8), @currentDateTime, 112) OR fstt.Titleind=1)
            THEN 1 ELSE 0 END)  AS PipelineInventorywoTitleover365Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime ) BETWEEN 0 AND 30
                  and (fstt.TitleindayID > CONVERT(VARCHAR(8), @currentDateTime, 112) OR fstt.Titleind=1)
            THEN 1 ELSE 0 END)  AS PipelineInventorywoTitle030Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime ) BETWEEN 31 AND 60
            and (fstt.TitleindayID > CONVERT(VARCHAR(8), @currentDateTime, 112) OR fstt.Titleind=1)
            THEN 1 ELSE 0 END)  AS PipelineInventorywoTitle3160Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime ) BETWEEN 61 AND 90
            and (fstt.TitleindayID > CONVERT(VARCHAR(8), @currentDateTime, 112) OR fstt.Titleind=1)
            THEN 1 ELSE 0 END)  AS PipelineInventorywoTitle6190Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime ) BETWEEN 91 AND 120
            and (fstt.TitleindayID > CONVERT(VARCHAR(8), @currentDateTime, 112) OR fstt.Titleind=1)
            THEN 1 ELSE 0 END)  AS PipelineInventorywoTitle91120Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime ) BETWEEN 121 AND 180
            and (fstt.TitleindayID > CONVERT(VARCHAR(8), @currentDateTime, 112) OR fstt.Titleind=1)
            THEN 1 ELSE 0 END)  AS PipelineInventorywoTitle121180Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime ) BETWEEN 181 AND 365
            and (fstt.TitleindayID > CONVERT(VARCHAR(8), @currentDateTime, 112) OR fstt.Titleind=1)
            THEN 1 ELSE 0 END)  AS PipelineInventorywoTitle181365Days,
            SUM(CASE WHEN fstt.isonhold<>1 THEN 1 ELSE 0 END)  AS PipelineInventoryExcludingHolds,
            SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime ) BETWEEN 0 AND 30
            and fstt.isonhold <>1 THEN 1 ELSE 0 END)  AS PipelineInventoryExcludingHolds030Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime) BETWEEN 31 AND 60
            and fstt.isonhold <>1 THEN 1 ELSE 0 END)  AS PipelineInventoryExcludingHolds3160Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime ) BETWEEN 61 AND 90
            and fstt.isonhold <>1 THEN 1 ELSE 0 END)  AS PipelineInventoryExcludingHolds6190Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime) BETWEEN 91 AND 120
            and fstt.isonhold<>1 THEN 1 ELSE 0 END)  AS PipelineInventoryExcludingHolds91120Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime ) BETWEEN 121 AND 180
            and fstt.isonhold<>1 THEN 1 ELSE 0 END)  AS PipelineInventoryExcludingHolds121180Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime ) BETWEEN 181 AND 365
            and fstt.isonhold<>1 THEN 1 ELSE 0 END)  AS PipelineInventoryExcludingHolds181365Days,
            SUM(CASE WHEN DATEDIFF(dd, fstt.AssignmentDateTime,@currentDateTime )>365 and fstt.isonhold<>1
            THEN 1 ELSE 0 END)  AS PipelineInventoryExcludingHoldsover365Days
                  from #TMP_stock_selection fstt with (nolock)

      
            group by          
            AdministrativeBranchNumber,
            CurrentProviderID

            
            
            
          BEGIN TRANSACTION
MERGE dbo.FactInventory fc
using (
            SELECT 
            TPI.AdministrativeBranchNumber,
            TPI.CurrentProviderID ,
            TPI.Day_id ,
            isnull(TPH.PhysicalInventory,0) PhysicalInventory,
			isnull(TPH.PhysicalInventory030Days,0) PhysicalInventory030Days,
            isnull(TPH.PhysicalInventory3160Days,0) PhysicalInventory3160Days,
            isnull(TPH.PhysicalInventory6190Days,0) PhysicalInventory6190Days,
            isnull(TPH.PhysicalInventory91120Days,0) PhysicalInventory91120Days,
            isnull(TPH.PhysicalInventory121180Days,0) PhysicalInventory121180Days,
            isnull(TPH.PhysicalInventory181365Days,0) PhysicalInventory181365Days,
            isnull(TPH.PhysicalInventory365Days,0) PhysicalInventory365Days,
            isnull(TPI.PipelineInventory,0)PipelineInventory,
			isnull(TPI.PipelineInventoryNonCAT,0)PipelineInventoryNonCAT,
            isnull(TPI.PipelineInventorywoTitle,0) PipelineInventorywoTitle,
            isnull(TPI.PipelineInventory030Days,0) PipelineInventory030Days,
            isnull(TPI.PipelineInventory3160Days,0) PipelineInventory3160Days,
            isnull(TPI.PipelineInventory6190Days,0) PipelineInventory6190Days,
            isnull(TPI.PipelineInventory91120Days,0) PipelineInventory91120Days,
            isnull(TPI.PipelineInventory121180Days,0) PipelineInventory121180Days,
            isnull(TPI.PipelineInventory181365Days,0) PipelineInventory181365Days,
            isnull(TPI.PipelineInventoryover365Days,0) PipelineInventoryover365Days,
            isnull(TPI.PipelineInventorywoTitleover365Days,0) PipelineInventorywoTitleover365Days,
            isnull(TPI.PipelineInventorywoTitle030Days,0) PipelineInventorywoTitle030Days,
            isnull(TPI.PipelineInventorywoTitle3160Days,0) PipelineInventorywoTitle3160Days,
            isnull(TPI.PipelineInventorywoTitle6190Days,0) PipelineInventorywoTitle6190Days,
            isnull(TPI.PipelineInventorywoTitle91120Days,0) PipelineInventorywoTitle91120Days,
            isnull(TPI.PipelineInventorywoTitle121180Days,0) PipelineInventorywoTitle121180Days,
            isnull(TPI.PipelineInventorywoTitle181365Days,0) PipelineInventorywoTitle181365Days,
            isnull(TPI.PipelineInventoryExcludingHolds,0) PipelineInventoryExcludingHolds,
            isnull(TPI.PipelineInventoryExcludingHolds030Days,0) PipelineInventoryExcludingHolds030Days,
            isnull(TPI.PipelineInventoryExcludingHolds3160Days,0)PipelineInventoryExcludingHolds3160Days,
            isnull(TPI.PipelineInventoryExcludingHolds6190Days,0) PipelineInventoryExcludingHolds6190Days,
            isnull(TPI.PipelineInventoryExcludingHolds91120Days,0) PipelineInventoryExcludingHolds91120Days,
            isnull(TPI.PipelineInventoryExcludingHolds121180Days,0) PipelineInventoryExcludingHolds121180Days,
            isnull(TPI.PipelineInventoryExcludingHolds181365Days,0) PipelineInventoryExcludingHolds181365Days,
            isnull(TPI.PipelineInventoryExcludingHoldsover365Days,0)PipelineInventoryExcludingHoldsover365Days ,
            @ETLLoadID ETLLoadID
            from #TMP_Pipeline TPI left outer join #TMP_Physical TPH
            on 
            TPI.AdministrativeBranchNumber = TPH.AdministrativeBranchNumber
            AND TPI.CurrentProviderID = TPH.CurrentProviderID
            AND TPI.Day_id = TPH.Day_id) as Tmp_inventory
on 
Tmp_inventory.AdministrativeBranchNumber = fc.AdministrativeBranchNumber
AND Tmp_inventory.CurrentProviderID = fc.CurrentProviderID
AND Tmp_inventory.Day_id = fc.[DayID]

When Not Matched then 
INSERT (

            AdministrativeBranchNumber,
            CurrentProviderID ,
            [DayID] ,
            [PhysicalInventory] ,
            [PhysicalInventory030Days] ,
            [PhysicalInventory3160Days] ,
            [PhysicalInventory6190Days] ,
            [PhysicalInventory91120Days] ,
            [PhysicalInventory121180Days] ,
            [PhysicalInventory181365Days] ,
            [PhysicalInventory365Days],
            [PipelineInventory],
			[PipelineInventoryNonCAT],
            [PipelineInventorywoTitle] ,
            [PipelineInventory030Days] ,
            [PipelineInventory3160Days] ,
            [PipelineInventory6190Days] ,
            [PipelineInventory91120Days] ,
            [PipelineInventory121180Days] ,
            [PipelineInventory181365Days] ,
            [PipelineInventoryover365Days] ,
            [PipelineInventorywoTitleover365Days] ,
            [PipelineInventorywoTitle030Days] ,
            [PipelineInventorywoTitle3160Days] ,
            [PipelineInventorywoTitle6190Days] ,
            [PipelineInventorywoTitle91120Days] ,
            [PipelineInventorywoTitle121180Days] ,
            [PipelineInventorywoTitle181365Days] ,
            [PipelineInventoryExcludingHolds] ,
            [PipelineInventoryExcludingHolds030Days] ,
            [PipelineInventoryExcludingHolds3160Days],
            [PipelineInventoryExcludingHolds6190Days] ,
            [PipelineInventoryExcludingHolds91120Days] ,
            [PipelineInventoryExcludingHolds121180Days] ,
            [PipelineInventoryExcludingHolds181365Days] ,
            [PipelineInventoryExcludingHoldsover365Days] ,
            ETLLoadID
            )
            VALUES
            (
			AdministrativeBranchNumber,
            CurrentProviderID ,
            Day_id ,
            PhysicalInventory ,
            PhysicalInventory030Days ,
            PhysicalInventory3160Days ,
            PhysicalInventory6190Days ,
            PhysicalInventory91120Days ,
            PhysicalInventory121180Days ,
            PhysicalInventory181365Days ,
            PhysicalInventory365Days,
            PipelineInventory,
			PipelineInventoryNonCAT,
            PipelineInventorywoTitle ,
            PipelineInventory030Days ,
            PipelineInventory3160Days ,
            PipelineInventory6190Days ,
            PipelineInventory91120Days ,
            PipelineInventory121180Days ,
            PipelineInventory181365Days ,
            PipelineInventoryover365Days ,
            PipelineInventorywoTitleover365Days ,
            PipelineInventorywoTitle030Days ,
            PipelineInventorywoTitle3160Days ,
            PipelineInventorywoTitle6190Days ,
            PipelineInventorywoTitle91120Days ,
            PipelineInventorywoTitle121180Days ,
            PipelineInventorywoTitle181365Days ,
            PipelineInventoryExcludingHolds ,
            PipelineInventoryExcludingHolds030Days ,
            PipelineInventoryExcludingHolds3160Days,
            PipelineInventoryExcludingHolds6190Days ,
            PipelineInventoryExcludingHolds91120Days ,
            PipelineInventoryExcludingHolds121180Days ,
            PipelineInventoryExcludingHolds181365Days ,
            PipelineInventoryExcludingHoldsover365Days ,
            ETLLoadID
            )
            OUTPUT $action
INTO #temp_FSCTRecordCount; 
 COMMIT TRAN

          SELECT @InsertRowCount_sp=Count(*)
          FROM   #temp_FSCTRecordCount
          WHERE  ChangeType='INSERT'
                      EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ExtractRowCount =@ExtractRowCount  ,@InsertRowCount =
          @InsertRowCount_sp,@UpdateRowCount = @UpdateRowCount
          
             IF Object_id('tempdb..#TMP_Pipeline') IS NOT NULL DROP TABLE #TMP_Pipeline
          IF Object_id('tempdb..#TMP_Physical') IS NOT NULL DROP TABLE #TMP_Physical
          IF Object_id('tempdb..#TMP_stock_selection') IS NOT NULL   DROP TABLE #TMP_stock_selection
      
            
              Return 0
  
END TRY
BEGIN CATCH


          IF @@TRANCOUNT>0
            ROLLBACK TRAN
              EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ExtractRowCount = @ExtractRowCount,@InsertRowCount =
          @InsertRowCount_sp,@UpdateRowCount = @UpdateRowCount,@ErrorCode = @@ERROR

          DECLARE @ErrorMessage  NVARCHAR(4000),
                  @ErrorSeverity INT,
                  @ErrorState    INT;

          SELECT @ErrorMessage=Error_message(),
                 @ErrorSeverity=Error_severity(),
                 @ErrorState=Error_state();

          RAISERROR (@ErrorMessage,
                     @ErrorSeverity,
                     @ErrorState);

         IF Object_id('tempdb..#TMP_Pipeline') IS NOT NULL DROP TABLE #TMP_Pipeline
          IF Object_id('tempdb..#TMP_Physical') IS NOT NULL DROP TABLE #TMP_Physical
          IF Object_id('tempdb..#TMP_stock_selection') IS NOT NULL   DROP TABLE #TMP_stock_selection 

      Return -1

END CATCH

END;

GO



