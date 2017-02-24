/* 
USE EDW
GO 
==============================================================================
Author		: Somesh Velupalli
Create date	: 08/19/2011
Description	: Populate the EDW DimStockTowType table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number	
==============================================================================
Usage Example:
Exec usp_ETLDimTowType
==============================================================================
*/

CREATE PROCEDURE [dbo].[usp_ETLDimTowType]
AS     
BEGIN


DECLARE @ExtractRowCount_sp int
		,@ETLLoadID_sp int
		,@InsertRowCount_sp int
		,@UpdateRowCount_sp int
		,@MinLSN Binary(10)
		,@MaxLSN Binary (10)
		
SET NOCOUNT ON;
BEGIN TRY

--Run the Audit begin
EXEC @ETLLoadID_sp = [dbo].[usp_ETLLoadAudit] 
		@StartEnd = 'S',
		@PackageName = 'usp_ETLDimTowType'
		
--Get the Min and Max LSN's
SELECT @MinLSN = MinLSN, @MaxLSN = MaxLSN FROM dbo.ETLRun WHERE ETLJobName = 'DailyETLJob'

/*********************************************************************************************
Create a temp table and Insert the values from the CDC Current Tow_Type table.
*********************************************************************************************/

IF OBJECT_ID('tempdb..#TMP_Tow_Type') IS NOT NULL DROP TABLE #TMP_Tow_type
CREATE TABLE #TMP_Tow_Type
(Tow_Type_Code CHAR(2) NOT NULL,
 Tow_Type_Description VARCHAR(40) NULL,
 Default_Responsible_Party_Code CHAR(10) NULL,
 Responsible_Party_Code_Description varchar(30) null
)


IF OBJECT_ID('tempdb..#temp_TowTypeRecordCount') IS NOT NULL DROP TABLE #temp_TowTypeRecordCount
CREATE TABLE #temp_TowTypeRecordCount(ChangeType VARCHAR(30)) 

INSERT INTO #TMP_Tow_Type
           (Tow_Type_Code,
            Tow_Type_Description,
			Default_Responsible_Party_Code,
			Responsible_Party_Code_Description) 	
SELECT TT.Tow_Type_Code,
       TT.Tow_Type_Description,
	   TT.Default_Responsible_Party_Code,
	   RP.Responsible_Party_Description
	   
FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Tow_Type]  (
   @MinLSN,@MaxLSN,'all with merge') AS TT 
INNER JOIN BI_ASAP_Rep.dbo.Responsible_Party AS RP 
   on TT.Default_Responsible_Party_Code = RP.Responsible_Party_Code
   WHERE [__$operation] = 5	 
 

SELECT @ExtractRowCount_sp = COUNT(*) FROM #TMP_Tow_Type


/*********************************************************************************************
MERGE the data from the temp table into the EDW DimTowType table
This does Update and Insert only.  No Deletes
*********************************************************************************************/

BEGIN TRANSACTION 
  
    MERGE [dbo].[DimTowType] AS DTT
    USING #TMP_Tow_Type AS SRC 
          ON SRC.Tow_Type_Code = DTT.TowTypeCode
    
    WHEN MATCHED THEN 
    UPDATE SET 
         DTT.[TowTypeDescription] = SRC.[Tow_Type_Description]
        ,DTT.[DefaultTowTypeReponsiblePartyCode] = SRC.[Default_Responsible_Party_Code]
        ,DTT.[DefaultTowTypeResponsiblePartyDescription] = SRC.Responsible_Party_Code_Description
        ,ETLLoadUpdateID = @ETLLoadID_Sp
        
   WHEN NOT MATCHED THEN
   INSERT(TowTypeCode
         ,TowTypeDescription
         ,DefaultTowTypeReponsiblePartyCode
	     ,DefaultTowTypeResponsiblePartyDescription
	     ,[ETLLoadID]
	     ,[ETLLoadUpdateID]) 
	     VALUES
	      (Tow_Type_Code,
	       Tow_Type_Description,
	       Default_Responsible_Party_Code,
	       Responsible_Party_Code_Description,
	       @ETLLoadID_sp,
	       @ETLLoadID_sp)
	     
	  OUTPUT $action INTO #temp_TowTypeRecordCount;
	  
  
  
  SELECT @InsertRowCount_sp = COUNT(*)  FROM #temp_TowTypeRecordCount WHERE ChangeType = 'INSERT'
  SELECT @UpdateRowCount_sp = COUNT(*)  FROM #temp_TowTypeRecordCount WHERE ChangeType = 'UPDATE'


  EXEC [dbo].[usp_ETLLoadAudit]
	   @StartEnd = 'E',
	   @ETLLoadID = @ETLLoadID_sp,
	   @ExtractRowCount = @ExtractRowCount_sp,
	   @InsertRowCount = @InsertRowCount_sp,
	   @UpdateRowCount = @UpdateRowCount_sp
	   
	   
COMMIT TRAN

    RETURN 0
END TRY

BEGIN CATCH

	SET NOCOUNT OFF
	IF @@TRANCOUNT > 0 
		ROLLBACK TRAN
	
	EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID_sp,
		@ExtractRowCount = @ExtractRowCount_sp,
		@InsertRowCount = @InsertRowCount_sp, 
		@UpdateRowCount = @UpdateRowCount_sp,
		@ErrorCode = @@ERROR
		
		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

		RETURN -1
	 
 END CATCH

END
