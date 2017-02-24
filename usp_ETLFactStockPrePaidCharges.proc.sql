/* 
USE EDW
GO 
==============================================================================
Author            : Sidd Murao
Create date : 11/30/2012
Description : Populate the EDW FactStockPrePaidCharge
==============================================================================
REVISION History
ChangeDate  Developer         Release/Problem Number
12/3/2012   Sidd Murao        

==============================================================================
Usage Example:
Exec usp_ETLFactStockPrePaidCharge
==============================================================================
*/


CREATE PROCEDURE [dbo].[usp_ETLFactStockPrePaidCharge]
AS     
BEGIN


DECLARE @ETLLoadID INT,
            @ExtractRowCount_sp INT,
            @InsertRowCount INT,
            @UpdateRowCount INT,
            @DeleteRowCount INT,
            @StartLSN BINARY(10),
            @EndLSN BINARY(10)
            
SET NOCOUNT ON;
BEGIN TRY

----Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLFactStockPrePaidCharge'

--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WHERE ETLJobName = 'DailyETLJob'


/*********************************************************************************************
Create the temp tables 
*********************************************************************************************/
IF OBJECT_ID('tempdb..#TMP_StockID') IS NOT NULL DROP TABLE #TMP_StockID
CREATE TABLE #TMP_StockID
(	[StockID] [int] NOT NULL,
	[salvageid] [int] NOT NULL
)

IF OBJECT_ID('tempdb..#TMP_FactStockPrePaidCharge') IS NOT NULL DROP TABLE #TMP_FactStockPrePaidCharge
CREATE TABLE #TMP_FactStockPrePaidCharge
(	[StockID] [int] NOT NULL,
    [PrepaidAdvanceAdminCharge] [money] NULL, 
    [PrepaidAdvanceOtherCharge] [money] NULL,
    [PrepaidAdvanceStorageCharge] [money] NULL,
    [PrepaidAdvanceTeardownCharge] [money] NULL,
    [PrepaidAdvanceTowCharge] [money] NULL,
    [PrepaidAdvanceTotalCharge] [money] NULL,
    [NetReturnPrepaidAdvanceCharge] [money] NULL
)

IF OBJECT_ID('tempdb..#TMP_FactStockPrePaidChargeRecordCount') IS NOT NULL DROP TABLE #TMP_FactStockPrePaidChargeRecordCount
CREATE TABLE #TMP_FactStockPrePaidChargeRecordCount(ChangeType VARCHAR(10)) 



/*********************************************************************************************
Create a temp table to get all the stocks having a PrePaid change
*********************************************************************************************/
INSERT INTO #TMP_StockID
(     StockID,
salvageid
)
SELECT DISTINCT StockID,ds.asapsalvageid
FROM dbo.Dimstock ds WITH (NOLOCK) 
inner join [bi_asap_rep].cdc.dbo_Salvage_Provider_Charge_Prepaid_CT sppcdc with (nolock)
on (ds.ASAPSalvageID = sppcdc.Salvage_ID)
and StockID <> 1000000  
                                
INSERT INTO #TMP_FactStockPrePaidCharge
(   StockID,
      PrepaidAdvanceAdminCharge,    
      PrepaidAdvanceOtherCharge,
      PrepaidAdvanceStorageCharge,
      PrepaidAdvanceTeardownCharge,
      PrepaidAdvanceTowCharge,
      PrepaidAdvanceTotalCharge,
      NetReturnPrepaidAdvanceCharge
)
      
select   ds.stockid ,
 PrepaidAdvanceAdminCharge,
 PrepaidAdvanceOtherCharge, 
 PrepaidAdvanceStorageCharge,
 PrepaidAdvanceTeardownCharge,
 PrepaidAdvanceTowCharge, 
 PrepaidAdvanceTotalCharge,
 ( (IsNULL(DS.GrossReturn,0)- IsNULL(Fcse.AdvanceTotalCharge,0) - ISNULL(Fcse.ProviderTotalCharge,0) ) - PrepaidAdvanceTotalCharge)
from  DimStock  ds  with (nolock)  inner join 
(SELECT  #TMP_StockID.StockID,
		sum(case when spcp.Charge_Type_ID  IN (275) THEN ISNULL( spcp.[Amount],0) ELSE 0 END) PrepaidAdvanceAdminCharge,
		sum(case when spcp.Charge_Type_ID  IN (282) THEN ISNULL( spcp.[Amount],0) ELSE 0 END) PrepaidAdvanceOtherCharge,
		sum(case when spcp.Charge_Type_ID  IN (289) THEN ISNULL( spcp.[Amount],0) ELSE 0 END) PrepaidAdvanceStorageCharge,
		sum(case when spcp.Charge_Type_ID  IN (376) THEN ISNULL( spcp.[Amount],0) ELSE 0 END) PrepaidAdvanceTeardownCharge,
		sum(case when spcp.Charge_Type_ID  IN (290) THEN ISNULL( spcp.[Amount],0) ELSE 0 END) PrepaidAdvanceTowCharge,
		sum(case when spcp.Charge_Type_ID  IN (290,376,289,282,275) THEN ISNULL(spcp.[Amount],0)ELSE 0 END) PrepaidAdvanceTotalCharge 
FROM #TMP_StockID 
INNER JOIN [BI_ASAP_REP].dbo.Salvage_Provider_Charge_Prepaid  spcp with (nolock)
on (spcp.Salvage_ID = #TMP_StockID.salvageid) Group by #TMP_StockID.StockID ) PrePaid on (prepaid.StockID = ds.StockID)
left outer  join factstockchargeexpense fcse with (nolock) on (fcse.StockID = prepaid.StockID)
 

/*********************************************************************************************
MERGE the data from the temp table into the EDW FactStockPrePaidCharge table
This does Update and Insert only.  No Deletes
*********************************************************************************************/
BEGIN TRANSACTION 
  
    MERGE [dbo].FactStockPrePaidCharge AS FSPC
    USING #TMP_FactStockPrePaidCharge AS Tmp_FSTPC 
          ON Tmp_FSTPC.StockID = FSPC.StockID
    
    WHEN MATCHED THEN 
    UPDATE SET 
        FSPC.PrepaidAdvanceAdminCharge = Tmp_FSTPC.PrepaidAdvanceAdminCharge,
        FSPC.PrepaidAdvanceOtherCharge = Tmp_FSTPC.PrepaidAdvanceOtherCharge,
        FSPC.PrepaidAdvanceStorageCharge = Tmp_FSTPC.PrepaidAdvanceStorageCharge, 
        FSPC.PrepaidAdvanceTeardownCharge = Tmp_FSTPC.PrepaidAdvanceTeardownCharge,
        FSPC.PrepaidAdvanceTowCharge = Tmp_FSTPC.PrepaidAdvanceTowCharge,
        FSPC.PrepaidAdvanceTotalCharge = Tmp_FSTPC.PrepaidAdvanceTotalCharge,
        FSPC.NetReturnPrepaidAdvanceCharge = Tmp_FSTPC.NetReturnPrepaidAdvanceCharge,
        FSPC.ETLLoadUpdateID = @ETLLoadID   
   WHEN NOT MATCHED THEN
   INSERT(	StockID,
			PrepaidAdvanceAdminCharge,    
			PrepaidAdvanceOtherCharge,
			PrepaidAdvanceStorageCharge,
			PrepaidAdvanceTeardownCharge,
			PrepaidAdvanceTowCharge,
			PrepaidAdvanceTotalCharge,
			NetReturnPrepaidAdvanceCharge,
                  ETLLoadID,
                  ETLLoadUpdateID
            )     
   VALUES(	StockID,
			PrepaidAdvanceAdminCharge,    
			PrepaidAdvanceOtherCharge,
			PrepaidAdvanceStorageCharge,
			PrepaidAdvanceTeardownCharge,
			PrepaidAdvanceTowCharge,
			PrepaidAdvanceTotalCharge,
			NetReturnPrepaidAdvanceCharge,
            @ETLLoadID,
            @ETLLoadID
           )
           
      OUTPUT $action INTO #TMP_FactStockPrePaidChargeRecordCount;
        
  /*********************************************************************************************
Get the counts from the MERGE operation
*********************************************************************************************/
SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #TMP_FactStockPrePaidChargeRecordCount

/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

            COMMIT TRAN
            IF OBJECT_ID('tempdb..#TMP_StockID') IS NOT NULL DROP TABLE #TMP_StockID
            IF OBJECT_ID('tempdb..#TMP_FactStockPrePaidCharge') IS NOT NULL DROP TABLE #TMP_FactStockPrePaidCharge

            
            RETURN 0
      END TRY     

      BEGIN CATCH
            SET NOCOUNT OFF
            IF @@TRANCOUNT > 0 
                  ROLLBACK TRAN
            
            IF OBJECT_ID('tempdb..#TMP_StockID') IS NOT NULL DROP TABLE #TMP_StockID
            IF OBJECT_ID('tempdb..#TMP_FactStockPrePaidCharge') IS NOT NULL DROP TABLE #TMP_FactStockPrePaidCharge  
            IF OBJECT_ID('tempdb..#TMP_FactStockPrePaidChargeRecordCount') IS NOT NULL DROP TABLE #TMP_FactStockPrePaidCharge 
            
            EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ErrorCode = @@ERROR, @UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

            DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
            SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
            RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

            RETURN -1
      END CATCH
      END


GO

GO


