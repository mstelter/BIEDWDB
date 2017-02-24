

/* 
USE EDW
GO 
==============================================================================
Author		: Sidd Murao
Create date	: 06/4/2011
Description	: Populate the EDW DimProviderQuestion and FactProviderQuestionResponse table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer			Release/Problem Number	
6/4/2012	Juan X Jacome		-- Removed [ProviderQuestionResponseID] from #TMP_FactProviderResponse
6/15/2012   Preetham Duvva      Added check for NULLS
7/10/2012   Sidd Murao          Added ParentProviderID 
3/13/2013	Matt Stelter		Max 40 Char to fit table    
1/13/2014	Matt Stelter		Set IsCurrentProvider to 0                           
==============================================================================
Usage Example:
Exec [usp_ETLFactProviderQuestionResponse]
==============================================================================
*/

CREATE PROCEDURE [dbo].[usp_ETLFactProviderQuestionResponse]
AS     
BEGIN


DECLARE @ETLLoadID        INT,
		@ExtractRowCount_sp INT,
		@InsertRowCount INT,
		@UpdateRowCount INT,
		@DeleteRowCount INT,
		@InsertRowCount1 INT,
		@UpdateRowCount1 INT,
		@DeleteRowCount1 INT,
		@StartLSN BINARY(10),
		@EndLSN BINARY(10)
				
	SET NOCOUNT ON;

BEGIN TRY

--Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S', @PackageName = 'usp_ETLFactProviderQuestionResponse'

--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WITH (NOLOCK) WHERE ETLJobName = 'DailyETLJob'

/*********************************************************************************************
Create the temp table to consolidate DimProviderQuestion
*********************************************************************************************/

CREATE TABLE #TMP_DimProviderQuestion
(	[QuestionID] [int] NOT NULL,
    [ProviderID] [int] NOT NULL,
	[QuestionText] [nvarchar] (255) NULL,
	[QuestionSequenceNumber] [smallint] NULL,
	[IsQuestionRequired] [tinyint] NOT NULL,
	[ResponseTypeCode] [char] (2) NULL,
	[ResponseTypeDescription] [nvarchar] (40) NULL,
	[ParentProviderID] [int] NOT NULL
	)

	

CREATE TABLE #TMP_DimProviderQuestionCount(ChangeType VARCHAR(30)) 
 

/*********************************************************************************************
Create the temp table to consolidate FactProviderResponse
*********************************************************************************************/

CREATE TABLE #TMP_FactProviderResponse
(	[QuestionID] [int] NOT NULL,
    [StockID] [int] NOT NULL,
    [ProviderID] [int] NOT NULL,
	[ResponseValue] [nvarchar] (40) NULL,
	[ResponseQuantity] [int] NULL,
	[IsResponse] [tinyint] NOT NULL,
	[IsCurrentSalvageProviderQuestion] [tinyint] NOT NULL,
	[ParentProviderID] [int] NOT NULL
	)

	

CREATE TABLE #TMP_FactProviderResponseCount(ChangeType VARCHAR(30)) 

/*********************************************************************************************
Create a temp table and Insert the values from the CDC Salvage Provider Question table.
*********************************************************************************************/
INSERT INTO #TMP_DimProviderQuestion
   (
    [QuestionID],
    [ProviderID] ,
	[QuestionText],
	[QuestionSequenceNumber] ,
	[IsQuestionRequired] ,
	[ResponseTypeCode],
	[ParentProviderID]
	) 	
SELECT	
    [Question_ID],
    PQR.[Salvage_Provider_ID],
    [Question_Text],
    [Question_Sequence_Number],
    [Question_Required_Ind],
    [Response_Type_Code] ,
    isnull(Sp.Parent_Salvage_Provider_ID,0)  
FROM [BI_ASAP_Rep].cdc.fn_cdc_get_net_changes_dbo_Salvage_Provider_Question(
   @StartLSN,@EndLSN,'all with merge')  PQR
   left outer join [bi_asap_rep].dbo.salvage_provider sp with (nolock) on (sp.salvage_provider_id = PQR.Salvage_Provider_ID)
   WHERE [__$operation] = 5
   

SELECT @ExtractRowCount_sp = COUNT(*) FROM #TMP_DimProviderQuestion


/*********************************************************************************************
Create a temp table and Insert the values from the CDC Salvage Provider Question table.
*********************************************************************************************/
INSERT INTO #TMP_FactProviderResponse
   (

    [QuestionID]  ,
    [StockID] ,
    [ProviderID]  ,
	[ResponseValue],
	[ResponseQuantity] ,
	[IsResponse] ,
	[IsCurrentSalvageProviderQuestion],
	[ParentProviderID]
	) 	
SELECT	
    distinct SPR.Question_ID ,
    DS.stockid,
    SPR.Salvage_Provider_ID,
		   ISNULL( coalesce(case when SPR.Salvage_Provider_Question_Valid_Response_ID is not null
         then LEFT(PQVR.Response_Text,40) else LEFT(SPR.Response_Value,40) end  ,
         cast (LEFT(SPR.Response_Quantity,40) as nvarchar(40)),           
           case when PQ.Response_Type_Code='TF' 
 then ( case when SPR.Response_Ind  =1 
 then 'Yes' else 'No' end ) end ,
 cast (LEFT(SPR.Salvage_Provider_Question_Valid_Response_ID,40) as nvarchar(40))),''), 

    SPR.Response_Quantity,
    SPR.Response_Ind,
    Case when SPR.Salvage_Provider_ID = DS.CurrentproviderID then 1 else 0 end,
    isnull(Sp.Parent_Salvage_Provider_ID,0)
FROM [BI_ASAP_Rep].cdc.fn_cdc_get_net_changes_dbo_Salvage_Provider_Response(
   @StartLSN,@EndLSN,'all with merge') 
   SPR  Inner join  DimStock DS with (nolock) on (DS.asapsalvageid= SPR.Salvage_ID)
   LEFT OUTER JOIN [BI_ASAP_REP].dbo.Salvage_Provider_Question_Valid_Response AS PQVR WITH (NOLOCK)
ON SPR.Salvage_Provider_Question_Valid_Response_ID = PQVR.Salvage_Provider_Question_Valid_Response_ID
LEFT OUTER JOIN [BI_ASAP_Rep].dbo.Salvage_Provider_Question PQ with (nolock) 
on PQ.Question_id= SPR.Question_ID 
left outer join [bi_asap_rep].dbo.salvage_provider sp with (nolock) on (sp.salvage_provider_id = SPR.Salvage_Provider_ID)   
      
   WHERE [__$operation] = 5

SELECT @ExtractRowCount_sp = COUNT(*) FROM #TMP_FactProviderResponse


/*********************************************************************************************
MERGE the data from the temp table into the EDW DimProviderQuestion table

*********************************************************************************************/
BEGIN TRANSACTION 
  
    MERGE  [dbo].[DimProviderQuestion] AS DPQ
    USING #TMP_DimProviderQuestion AS TPQ
          ON (TPQ.ProviderID = DPQ.ProviderID
              AND TPQ.QuestionID = DPQ.QuestionID)
    
    WHEN MATCHED THEN 
        UPDATE SET 
		DPQ.QuestionText = ISNULL(TPQ.QuestionText,''),
		DPQ.QuestionSequenceNumber= ISNULL(TPQ.QuestionSequenceNumber,0),
		DPQ.IsQuestionRequired= ISNULL(TPQ.IsQuestionRequired,0),
		DPQ.ResponseTypeCode = ISNULL(TPQ.ResponseTypeCode,''),
		DPQ.[ParentProviderID] = ISNULL(TPQ.[ParentProviderID],0),
		DPQ.ETLLoadUpdateID = @ETLLoadID 
			
	WHEN NOT MATCHED THEN
    INSERT(
            QuestionID,
            ProviderID,
            QuestionText,
            QuestionSequenceNumber,
            IsQuestionRequired,
            ResponseTypeCode,
            ETLLoadID,
            ETLLoadUpdateID,
           	ParentProviderID
			) 
	  VALUES ( TPQ.QuestionID,
            TPQ.ProviderID,
            Isnull(TPQ.QuestionText,''),
            Isnull(TPQ.QuestionSequenceNumber,0),
            Isnull(TPQ.IsQuestionRequired,0),
            Isnull(TPQ.ResponseTypeCode,''),
			@ETLLoadID,
			@ETLLoadID,
			ISNULL(TPQ.[ParentProviderID],0)
				)
	     
	OUTPUT $action INTO #TMP_DimProviderQuestionCount;

COMMIT TRANSACTION 
	
	
	/*********************************************************************************************
Get the counts from the MERGE operation
*********************************************************************************************/
SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #TMP_DimProviderQuestionCount


/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount


/*********************************************************************************************
MERGE the data from the temp table into the EDW FactProviderQuestionResponse table

*********************************************************************************************/
 BEGIN TRANSACTION 

	UPDATE FPQR SET
		[IsCurrentSalvageProviderQuestion]=0, 
		[ETLLoadUpdateID] = @ETLLoadID
	FROM [dbo].[FactProviderQuestionResponse] AS FPQR
	INNER JOIN #TMP_FactProviderResponse AS FPR
	ON (FPR.QuestionID = FPQR.QuestionID
		AND FPR.StockID = FPQR.StockID)

    MERGE [dbo].[FactProviderQuestionResponse] AS FPQR
    USING #TMP_FactProviderResponse AS FPR
          ON (FPR.ProviderID = FPQR.ProviderID
              AND FPR.QuestionID = FPQR.QuestionID
              AND FPR.StockID = FPQR.StockID)
    
    WHEN MATCHED THEN 
        UPDATE SET 
	        [ResponseValue]=ISNULL(FPR.[ResponseValue],''),
	        [ResponseQuantity]=ISNULL(FPR.[ResponseQuantity],0),
			[IsResponse]=ISNULL(FPR.[IsResponse],0),
			[IsCurrentSalvageProviderQuestion]=ISNULL(FPR.[IsCurrentSalvageProviderQuestion],0),  
 			[ETLLoadUpdateID] = @ETLLoadID,
 			[ParentProviderID] = ISNULL(FPR.[ParentProviderID],0)
			
	WHEN NOT MATCHED THEN
    INSERT(
            [QuestionID],
            [StockID],
            [ProviderID] ,
	        [ResponseValue],
	        [ResponseQuantity],
			[IsResponse],
			[IsCurrentSalvageProviderQuestion],
 			[ETLLoadID],
            [ETLLoadUpdateID],
            [ParentProviderID]			
			) 
	  VALUES ( 
	        FPR.[QuestionID],
            FPR.[StockID],
            FPR.[ProviderID] ,
	        ISNULL(FPR.[ResponseValue],''),
	        ISNULL(FPR.[ResponseQuantity],0),
			ISNULL(FPR.[IsResponse],0),
			ISNULL(FPR.[IsCurrentSalvageProviderQuestion],0),  
 			@ETLLoadID,
            @ETLLoadID,
            ISNULL(FPR.[ParentProviderID],0)
				)
	     
	OUTPUT $action INTO #TMP_FactProviderResponseCount;
COMMIT TRANSACTION 
	



SELECT 
@UpdateRowCount1 = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount1 = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #TMP_FactProviderResponseCount


EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount1,@InsertRowCount = @InsertRowCount1

		IF OBJECT_ID('tempdb..#TMP_DimProviderQuestion') IS NOT NULL DROP TABLE #TMP_DimProviderQuestion
		IF OBJECT_ID('tempdb..#TMP_DimProviderQuestionCount') IS NOT NULL DROP TABLE #TMP_DimProviderQuestionCount
		IF OBJECT_ID('tempdb..#TMP_FactProviderResponse') IS NOT NULL DROP TABLE #TMP_FactProviderResponse
		IF OBJECT_ID('tempdb..#TMP_FactProviderResponseCount') IS NOT NULL DROP TABLE #TMP_FactProviderResponseCount
		
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
		
		IF OBJECT_ID('tempdb..#TMP_DimProviderQuestion') IS NOT NULL DROP TABLE #TMP_DimProviderQuestion
		IF OBJECT_ID('tempdb..#TMP_DimProviderQuestionCount') IS NOT NULL DROP TABLE #TMP_DimProviderQuestionCount
		IF OBJECT_ID('tempdb..#TMP_FactProviderResponse') IS NOT NULL DROP TABLE #TMP_FactProviderResponse
		IF OBJECT_ID('tempdb..#TMP_FactProviderResponseCount') IS NOT NULL DROP TABLE #TMP_FactProviderResponseCount
		RETURN -1
	END CATCH
	END









