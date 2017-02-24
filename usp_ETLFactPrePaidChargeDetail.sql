/* 
USE EDW
GO 
==============================================================================
Author            : Juan X. Jacome
Create date : 12/4/2012
Description : Populate the EDW FactPrePaidChargeDetail
==============================================================================
REVISION History
ChangeDate  Developer         Release/Problem Number
12/17/2012	Juan X. Jacome	  -- Added CountOfPrepaidAuthorizedChanges, CountOfPrepaidAuthorizedChanges
==============================================================================
Usage Example:
Exec usp_ETLFactPrePaidChargeDetail
==============================================================================
*/

CREATE PROCEDURE [dbo].[usp_ETLFactPrePaidChargeDetail]
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
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLFactPrePaidChargeDetail'

--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WHERE ETLJobName = 'DailyETLJob'


/*********************************************************************************************
Create the temp tables 
*********************************************************************************************/

IF OBJECT_ID('tempdb..#TMP_FactPrePaidChargeDetail') IS NOT NULL DROP TABLE #TMP_FactPrePaidChargeDetail
CREATE TABLE [#TMP_FactPrePaidChargeDetail](
[StockID] [int] NOT NULL,
[Salvage_Id] [int] NOT NULL,
[ProviderID] [int] NOT NULL,
[BranchNumber] [int] NOT NULL,
[DayId] [int] NOT NULL,
[PrePaidChargeRecordInd] [int] NULL, 
[PrepaidAdvanceAdminCharge] [money] NULL,
[PrepaidAdvanceOtherCharge] [money] NULL,
[PrepaidAdvanceStorageCharge] [money] NULL,
[PrepaidAdvanceTeardownCharge] [money] NULL,
[PrepaidAdvanceTowCharge] [money] NULL,
[PrepaidAdvanceChargeAuthorizedBy] [varchar](100) NULL,
[PrepaidAdvanceChargeAuthorizedUser_id] [int] NULL,
[PrepaidAdvanceChargeUpdateDateTime] [datetime] NULL,
[CountOfPrepaidAuthorizedChanges] [int] NULL,
[MaximumPrepaidAuthorizedDateTime] [datetime] NULL,
[MaximumPrepaidAuthorizedAmount] [money] NULL,
[MaximumPrepaidAuthorizedBy] [varchar](100) NULL,
[IAAAuthorizedToPayDetailInd] [int] NULL,
[CountOfMaximumPrepaidAuthorizedChanges] [int] NULL
)

IF OBJECT_ID('tempdb..#TMP_FactStockPrePaidChargeRecordCount') IS NOT NULL DROP TABLE #TMP_FactStockPrePaidChargeRecordCount
CREATE TABLE #TMP_FactStockPrePaidChargeRecordCount(ChangeType VARCHAR(10))


/*********************************************************************************************
Create a temp table to get all the stocks having a FactPrePaidChargeDetail change
*********************************************************************************************/
INSERT INTO [#TMP_FactPrePaidChargeDetail](
[StockID],
[Salvage_Id],
[ProviderID],
[BranchNumber],
[DayID],
[PrePaidChargeRecordInd],
[PrepaidAdvanceAdminCharge],
[PrepaidAdvanceOtherCharge],
[PrepaidAdvanceStorageCharge],
[PrepaidAdvanceTeardownCharge],
[PrepaidAdvanceTowCharge],
[PrepaidAdvanceChargeAuthorizedUser_id],
[PrepaidAdvanceChargeUpdateDateTime],
[CountOfPrepaidAuthorizedChanges]
)

select	DS.StockID,
		SP.Salvage_Id,
		DS.CurrentProviderID,
		DS.AdministrativeBranchNumber,
		CONVERT(INT, CONVERT(CHAR(8),SP.Update_DateTime, 112)) AS DayID,
		1, 
		MAX(CASE WHEN SP.Charge_Type_Id = 275 THEN SP.Amount ELSE 0 END) AS PrepaidAdvanceAdminCharge,
		MAX(CASE WHEN SP.Charge_Type_Id = 282 THEN SP.Amount ELSE 0 END) AS PrepaidAdvanceOtherCharge,
		MAX(CASE WHEN SP.Charge_Type_Id = 289 THEN SP.Amount ELSE 0 END) AS PrepaidAdvanceStorageCharge,
		MAX(CASE WHEN SP.Charge_Type_Id = 290 THEN SP.Amount ELSE 0 END) AS PrepaidAdvanceTowCharge,
		MAX(CASE WHEN SP.Charge_Type_Id = 376 THEN SP.Amount ELSE 0 END) AS PrepaidAdvanceTeardownCharge,
		MAX(CASE WHEN SP.Charge_Type_Id = 289 THEN Update_User_ID ELSE 0 END)AS PrepaidAdvanceChargeAuthorizedUser_id,
		SP.Update_DateTime,
		1
FROM BI_ASAP_Rep.dbo.Salvage_Provider_Charge_Prepaid_History  SP 
INNER JOIN DimStock DS WITH(NOLOCK)
ON SP.Salvage_Id = DS.ASAPSalvageID
LEFT OUTER JOIN DimUser DU WITH(NOLOCK)
ON SP.Update_User_id = DU.UserID
GROUP by DS.StockID,
		SP.Salvage_Id, 
		DS.CurrentProviderID,
		DS.AdministrativeBranchNumber,
		CONVERT(INT, CONVERT(CHAR(8),SP.Update_DateTime, 112)),
		SP.Update_DateTime
		
UPDATE TF	
SET [PrepaidAdvanceChargeAuthorizedBy] = DU.FirstName + ' '+ DU.LastName
FROM #TMP_FactPrePaidChargeDetail TF WITH(NOLOCK)
INNER JOIN DimUser DU WITH(NOLOCK)
 ON TF.PrepaidAdvanceChargeAuthorizedUser_id = DU.UserID 	
		

INSERT INTO [#TMP_FactPrePaidChargeDetail](
[StockID],
[Salvage_Id],
[ProviderID],
[BranchNumber],
[DayID],
[PrePaidChargeRecordInd],
[MaximumPrepaidAuthorizedDateTime],
[MaximumPrepaidAuthorizedAmount],
[MaximumPrepaidAuthorizedBy],
[IAAAuthorizedToPayDetailInd],
[CountOfMaximumPrepaidAuthorizedChanges]
)
SELECT DS.StockID,
		SA.Salvage_Id,
		DS.CurrentProviderID,
		DS.AdministrativeBranchNumber,
		CONVERT(INT, CONVERT(CHAR(8),SA.Update_DateTime, 112)) AS DayID,
		0,
		SA.Update_DateTime,
		ISNULL(SA.Max_Authorized_Pay_Amount,0),
		DU.FirstName + ' '+ DU.LastName,
		SA.IAA_Authorized_To_Pay,
		1
FROM [BI_ASAP_Rep].dbo.Salvage_Authorized_Amount_history SA WITH(NOLOCK)
INNER JOIN DimStock DS WITH(NOLOCK)
ON SA.Salvage_Id = DS.ASAPSalvageID
LEFT OUTER JOIN DimUser DU WITH(NOLOCK)
 ON SA.Update_User_ID = DU.UserID 

/*********************************************************************************************
MERGE the data from the temp table into the EDW FactStockPrePaidCharge table
This does Update and Insert only.  No Deletes
*********************************************************************************************/
BEGIN TRANSACTION 

MERGE [dbo].FactPrePaidChargeDetail AS FPPCD
    USING #TMP_FactPrePaidChargeDetail AS Tmp_FPPCD
          ON Tmp_FPPCD.StockID = FPPCD.StockID
          AND Tmp_FPPCD.PrePaidChargeRecordInd = FPPCD.PrePaidChargeRecordInd
          AND ISNULL(Tmp_FPPCD.PrepaidAdvanceChargeUpdateDateTime,'12/31/2999') = FPPCD.PrepaidAdvanceChargeUpdateDateTime
          AND ISNULL(Tmp_FPPCD.MaximumPrepaidAuthorizedDateTime,'12/31/2999') = FPPCD.MaximumPrepaidAuthorizedDateTime
    

   WHEN NOT MATCHED THEN
   INSERT(	StockID,
			ProviderID,
			BranchNumber,
			DayId,
			PrePaidChargeRecordInd,
			PrepaidAdvanceAdminCharge,
			PrepaidAdvanceOtherCharge,
			PrepaidAdvanceStorageCharge,
			PrepaidAdvanceTeardownCharge,
			PrepaidAdvanceTowCharge,
			PrepaidAdvanceChargeAuthorizedBy,
			PrepaidAdvanceChargeUpdateDateTime,
			CountOfPrepaidAuthorizedChanges,
			MaximumPrepaidAuthorizedDateTime,
			MaximumPrepaidAuthorizedAmount,
			MaximumPrepaidAuthorizedBy,
			CountOfMaximumPrepaidAuthorizedChanges,
			IAAAuthorizedToPayDetailInd,
            ETLLoadID,
            ETLLoadUpdateID
            )     
   VALUES(	StockID,
			ProviderID,
			BranchNumber,
			DayId,
			PrePaidChargeRecordInd,
			PrepaidAdvanceAdminCharge,
			PrepaidAdvanceOtherCharge,
			PrepaidAdvanceStorageCharge,
			PrepaidAdvanceTeardownCharge,
			PrepaidAdvanceTowCharge,
			PrepaidAdvanceChargeAuthorizedBy,
			ISNULL(PrepaidAdvanceChargeUpdateDateTime,'12/31/2999'),
			CountOfPrepaidAuthorizedChanges,
			ISNULL(MaximumPrepaidAuthorizedDateTime,'12/31/2999'),
			MaximumPrepaidAuthorizedAmount,
			MaximumPrepaidAuthorizedBy,
			CountOfMaximumPrepaidAuthorizedChanges,
			IAAAuthorizedToPayDetailInd,
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

-- Update Counts
UPDATE FP
SET CountOfPrepaidAuthorizedChanges = 0
FROM FactPrePaidChargeDetail FP WITH(NOLOCK)
INNER JOIN (SELECT StockID,
			MIN(PrepaidAdvanceChargeUpdateDateTime) AS PrepaidAdvanceChargeUpdateDateTime
			FROM FactPrePaidChargeDetail WITH(NOLOCK)
			WHERE PrePaidChargeRecordInd = 1
			GROUP BY StockID) AS FP2
ON FP.StockID = FP2.StockID
AND FP.PrepaidAdvanceChargeUpdateDateTime = FP2.PrepaidAdvanceChargeUpdateDateTime

UPDATE FP
SET	CountOfMaximumPrepaidAuthorizedChanges = 0
FROM FactPrePaidChargeDetail FP WITH(NOLOCK)
INNER JOIN (SELECT StockID,
			MIN(MaximumPrepaidAuthorizedDateTime) AS MaximumPrepaidAuthorizedDateTime
			FROM FactPrePaidChargeDetail WITH(NOLOCK)
			WHERE PrePaidChargeRecordInd = 0
			GROUP BY StockID) AS FP3
ON FP.StockID = FP3.StockID
AND FP.MaximumPrepaidAuthorizedDateTime = FP3.MaximumPrepaidAuthorizedDateTime

/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

            COMMIT TRAN
            IF OBJECT_ID('tempdb..#TMP_FactPrePaidChargeDetail') IS NOT NULL DROP TABLE #TMP_FactPrePaidChargeDetail
            IF OBJECT_ID('tempdb..#TMP_FactStockPrePaidChargeRecordCount') IS NOT NULL DROP TABLE #TMP_FactStockPrePaidChargeRecordCount

            
            RETURN 0
      END TRY     

      BEGIN CATCH
            SET NOCOUNT OFF
            IF @@TRANCOUNT > 0 
                  ROLLBACK TRAN
            
            IF OBJECT_ID('tempdb..#TMP_FactPrePaidChargeDetail') IS NOT NULL DROP TABLE #TMP_FactPrePaidChargeDetail
            IF OBJECT_ID('tempdb..#TMP_FactStockPrePaidChargeRecordCount') IS NOT NULL DROP TABLE #TMP_FactStockPrePaidChargeRecordCount

            EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ErrorCode = @@ERROR, @UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

            DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
            SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
            RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

            RETURN -1
      END CATCH
      END



GO


