
/* 
USE EDW
GO 
==============================================================================
Author		: Preetham Duvva
Create date	: 3/30/2012
Description	: Populate the EDW FactAssignment table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer	
4/2/2012    Preetham Duvva           Added AdministrativeBranchNumber.		
4/3/2012	Juan X Jacome			 Rename sp and table to FactAssignment
4/12/2012	Juan X Jacome			 DBA Code Review
5/4/2012	Juan X Jacome			 Updated Branch Number to AssignmentBranchNumber

==============================================================================
Usage Example:
Exec usp_ETLFactAssignment
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLFactAssignment]
AS     
BEGIN

DECLARE @ExtractRowCount int
		,@ETLLoadID int
		,@InsertRowCount int
		,@UpdateRowCount int
		,@MinLSN Binary(10)
		,@MaxLSN Binary (10)
		
				
SET NOCOUNT ON;

BEGIN TRY
            
--Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLFactAssignment'

--Get the start and end LSNs
SELECT  @MinLSN = MinLSN, @MaxLSN = MaxLSN FROM dbo.ETLRun WHERE ETLJobName = 'DailyETLJob'

/*********************************************************************************************
Create the temp table to consolidate the Assigned and Cancelled Stocks
*********************************************************************************************/

-- Create Table for New Assignments
IF OBJECT_ID('tempdb..#TMP_New_Assigned_Cancelled') IS NOT NULL DROP TABLE #TMP_New_Assigned_Cancelled
CREATE TABLE #TMP_New_Assigned_Cancelled
			( SalvageID INT NULL,
			  AssignedDateTime Datetime NULL,
			  AssignedDayID INT NOT NULL,
			  AssignedQuantity INT NULL,
			)

---Create #FactAssignment

IF OBJECT_ID('tempdb..#TMP_FactAssignment') IS NOT NULL DROP TABLE #TMP_FactAssignment
CREATE TABLE #TMP_FactAssignment
			( StockID INT NULL,
			  AssignedDateTime Datetime NULL,
			  AssignedDayID INT NULL,
			  AssignedQuantity INT NULL	,
			  AssignmentBranchNumber INT NULL,
			  CancelledAssignments INT NULL,
			  CancelledManualAssignments INT NULL,
			  CancelledElectronicAssignments INT NULL,
			  UnitsAssignedGross INT NULL,
			  UnitsAssignedGrossManual INT NULL,
			  UnitsAssignedGrossElectronic INT NULL,
			  UnitsAssigned INT NULL,
			  ElectronicAssignments INT NULL,
			  ManualAssignments INT NULL,
			  FactAssignmentAssignmentID BIGINT NULL
			)

INSERT INTO #TMP_New_Assigned_Cancelled
(
              SalvageID ,
			  AssignedDateTime ,
			  AssignedDayID,
			  AssignedQuantity 
)

SELECT   Salvage_ID
        ,ISNULL(SI.Assignment_DateTime,'2999-12-31') As AssignedDateTime
        ,ISNULL(CONVERT(INT, CONVERT(CHAR(8), SI.Assignment_DateTime, 112)), 29991231) As AssignedDayID
        ,1 As AssignedQuantity
FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_salvage_info](@MinLSN, @MaxLSN, 'all with merge') SI
where [__$operation] = 5

Union 

SELECT   Salvage_ID
        ,ISNULL(SEH.Event_DateTime ,'2999-12-31' ) As AssignedDateTime
        ,ISNULL(CONVERT(INT, CONVERT(CHAR(8),(SEH.Event_DateTime), 112)), 29991231) As AssignedDayID
        ,-1 As AssignedQuantity
FROM   [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Event_History](@MinLSN, @MaxLSN, 'all with merge') SEH
where [__$operation] = 5 and SEH.Salvage_Event_Code = 'ASCN'
Order By Salvage_ID


INSERT INTO #TMP_FactAssignment
(
			  StockID
			  ,AssignedDateTime
			  ,AssignedDayID
			  ,AssignedQuantity
			  ,AssignmentBranchNumber
			  ,CancelledAssignments
			  ,CancelledManualAssignments
			  ,CancelledElectronicAssignments
			  ,UnitsAssignedGross
			  ,UnitsAssignedGrossManual
			  ,UnitsAssignedGrossElectronic
			  ,UnitsAssigned
			  ,ElectronicAssignments
			  ,ManualAssignments
			  ,FactAssignmentAssignmentID
			  
   )
SELECT         DS.StockID
              ,NAC.AssignedDateTime
			  ,NAC.AssignedDayID
			  ,NAC.AssignedQuantity 
			  ,DS.AdministrativeBranchNumber
	          ,CASE WHEN NAC.AssignedQuantity = -1 THEN NAC.AssignedQuantity ELSE 0 END As CancelledAssignments
			  ,CASE WHEN DS.AssignmentMethod In ('ASAP') THEN 
              (CASE WHEN NAC.AssignedQuantity = -1 THEN -1 ELSE 0 end) ELSE 0 END As CancelledManualAssignments
              ,CASE WHEN DS.AssignmentMethod In ('CSA','EBIZ') THEN 
			  (CASE WHEN NAC.AssignedQuantity = -1 THEN -1 ELSE 0 END) ELSE 0 END As CancelledElectronicAssignments
              ,(CASE WHEN NAC.AssignedQuantity = -1 THEN 0 ELSE 1 END) As UnitsAssignedGross
              ,CASE WHEN DS.AssignmentMethod In ('ASAP') THEN 
              (CASE WHEN NAC.AssignedQuantity = -1 THEN 0 ELSE 1 END) ELSE 0 END As UnitsAssignedGrossManual
              ,CASE WHEN DS.AssignmentMethod In ('CSA','EBIZ') THEN 
			  (CASE WHEN NAC.AssignedQuantity = -1 THEN 0 ELSE 1 END) ELSE 0 END As UnitsAssignedGrossElectronic
			  ,(NAC.AssignedQuantity) 'Units Assigned'
			  ,CASE WHEN DS.AssignmentMethod In ('CSA','EBIZ') THEN (NAC.AssignedQuantity)ELSE 0 END As ElectronicAssignments
			  ,CASE WHEN DS.AssignmentMethod In ('ASAP') THEN (NAC.AssignedQuantity) ELSE 0 END As ManualAssignments
			  ,0
FROM #TMP_New_Assigned_Cancelled NAC WITH (NOLOCK)
INNER JOIN DimStock DS WITH (NOLOCK) 
	ON NAC.SalvageID = DS.ASAPSalvageID 
	
	
UPDATE TFA
SET FactAssignmentAssignmentID = FA.AssignmentID
FROM #TMP_FactAssignment TFA WITH (NOLOCK)
INNER JOIN FactAssignment FA WITH (NOLOCK)
	ON TFA.StockID = FA.StockID
	AND TFA.AssignedQuantity = FA.AssignedQuantity

		

IF OBJECT_ID('tempdb..#TMP_FactAssignmentRecordCount') IS NOT NULL DROP TABLE #TMP_FactAssignmentRecordCount
CREATE TABLE #TMP_FactAssignmentRecordCount(ChangeType VARCHAR(10)) 

SELECT @ExtractRowCount=COUNT(*) FROM #TMP_FactAssignment


/*********************************************************************************************
MERGE the data from the temp table into the EDW FactAssignment table
This does Update and Insert only.  No Deletes
*********************************************************************************************/


BEGIN TRANSACTION 
--Synchronize source data with target
	MERGE dbo.FactAssignment AS FA
	USING #TMP_FactAssignment AS TFA    
		ON FA.StockID = TFA.StockID 
		AND FA.AssignmentID = TFA.FactAssignmentAssignmentID
		
		
	WHEN MATCHED THEN
	UPDATE SET
	 AssignedDateTime = TFA.AssignedDateTime
    ,AssignedDayID = TFA.AssignedDayID
    ,AssignedQuantity = TFA.AssignedQuantity
    ,[AssignmentBranchNumber] = TFA.AssignmentBranchNumber
    ,CancelledAssignments = TFA.CancelledAssignments
    ,CancelledManualAssignments = TFA.CancelledManualAssignments
    ,CancelledElectronicAssignments = TFA.CancelledElectronicAssignments
    ,UnitsAssignedGross = TFA.UnitsAssignedGross
    ,UnitsAssignedGrossManual = TFA.UnitsAssignedGrossManual
    ,UnitsAssignedGrossElectronic = TFA.UnitsAssignedGrossElectronic
    ,UnitsAssigned = TFA.UnitsAssigned
    ,ElectronicAssignments = TFA.ElectronicAssignments
    ,ManualAssignments = TFA.ManualAssignments
    ,[ETLLoadUpdateID] = @ETLLoadID
  
	WHEN NOT MATCHED  THEN 
	INSERT (
	StockID
    ,AssignedDateTime
    ,AssignedDayID
    ,AssignedQuantity
    ,AssignmentBranchNumber
    ,CancelledAssignments
    ,CancelledManualAssignments
    ,CancelledElectronicAssignments
    ,UnitsAssignedGross
    ,UnitsAssignedGrossManual
    ,UnitsAssignedGrossElectronic
    ,UnitsAssigned
    ,ElectronicAssignments
    ,ManualAssignments
    ,[ETLLoadID]
    ,[ETLLoadUpdateID])
	
	Values(   
    StockID
    ,TFA.AssignedDateTime
    ,TFA.AssignedDayID
    ,TFA.AssignedQuantity
    ,TFA.AssignmentBranchNumber
    ,TFA.CancelledAssignments
    ,TFA.CancelledManualAssignments
    ,TFA.CancelledElectronicAssignments
    ,TFA.UnitsAssignedGross
    ,TFA.UnitsAssignedGrossManual
    ,TFA.UnitsAssignedGrossElectronic
    ,TFA.UnitsAssigned
    ,TFA.ElectronicAssignments
    ,TFA.ManualAssignments
    ,@ETLLoadID
    ,@ETLLoadID
	)
  

	
OUTPUT $action INTO #TMP_FactAssignmentRecordCount;


SELECT @InsertRowCount = COUNT(*)  FROM #TMP_FactAssignmentRecordCount WHERE ChangeType = 'INSERT'
SELECT @UpdateRowCount = COUNT(*)  FROM #TMP_FactAssignmentRecordCount WHERE ChangeType = 'UPDATE'


	EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID,
		@ExtractRowCount = @ExtractRowCount,
		@InsertRowCount = @InsertRowCount,
		@UpdateRowCount = @UpdateRowCount
COMMIT TRAN
	
	IF OBJECT_ID('tempdb..#TMP_New_Assigned_Cancelled') IS NOT NULL DROP TABLE #TMP_New_Assigned_Cancelled
	IF OBJECT_ID('tempdb..#TMP_FactAssignment') IS NOT NULL DROP TABLE #TMP_FactAssignment
		
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
		
		IF OBJECT_ID('tempdb..#TMP_New_Assigned_Cancelled') IS NOT NULL DROP TABLE #TMP_New_Assigned_Cancelled
		IF OBJECT_ID('tempdb..#TMP_FactAssignment') IS NOT NULL DROP TABLE #TMP_FactAssignment

		RETURN -1
	

END CATCH

END

GO


