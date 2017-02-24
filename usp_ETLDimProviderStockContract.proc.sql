/* 
USE EDW
GO 
==============================================================================
Author		: Juan X. Jacome
Create date	: 08/23/2011
Description	: Populate the EDW DimProviderStockContract table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number	
10/28/2011	Juan X. Jacome		Added code to drop temp Tables (DBA Code Review)
06/05/2012  Venkata Vempali     Updated the Default dates for ProviderStockContractEffectiveDateTime, 
									ProviderStockContractExpirationDateTime, IAAFeeEffectiveDateTime, 
									IAAFeeExpriationDateTime
5/30/2014	Matt Stelter		Added Settlement Approval Group
7/29/2014	Juan X. Jacome		Added distinct to insert into #TMP_ProviderStockContract
==============================================================================
Usage Example:
Exec usp_ETLDimProviderStockContract
==============================================================================
*/

CREATE PROCEDURE [dbo].[usp_ETLDimProviderStockContract]
AS     
BEGIN

DECLARE @ETLLoadID        INT,
		@InsertRowCount INT,
		@UpdateRowCount INT,
		@DeleteRowCount INT,
		@StartLSN BINARY(10),
		@EndLSN BINARY(10)
				
	SET NOCOUNT ON;

BEGIN TRY

--Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'uspETLDimProviderStockContract'

--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WITH (NOLOCK) WHERE ETLJobName = 'DailyETLJob'


/*********************************************************************************************
Create the temp table to consolidate #Salvage_Contract_Info
*********************************************************************************************/
IF OBJECT_ID('tempdb..#TMP_Salvage_Contract_Info') IS NOT NULL DROP TABLE #TMP_Salvage_Contract_Info
CREATE TABLE #TMP_Salvage_Contract_Info
(	[Salvage_Contract_Info_ID] [int] NOT NULL,
	[Salvage_ID] [int] NOT NULL,
	[Agreement_Term_Type_Code] [char](3) NOT NULL,
	[Update_DateTime] [datetime] NULL,
	[Update_User_ID] [int] NULL,
	[Manually_Revalued_Ind] [bit] NOT NULL,
	[Settle_By_Date] [datetime] NULL,
	[Flipped_to_PA_Ind] [bit] NOT NULL,
	[Salvage_Contract_ID] [int] NULL,
	[Agreement_Changed_Ind] [bit] NOT NULL,
	[Fee_ID] [int] NULL,
	[Hybrid_Agreement_Ind] [bit] NOT NULL,
	[Revalue_At_Title_Received_Ind] [bit] NULL
	)
	
/*********************************************************************************************
Create the temp table to consolidate #Salvage_Contract_Info
*********************************************************************************************/
IF OBJECT_ID('tempdb..#TMP_ProviderStockContract') IS NOT NULL DROP TABLE #TMP_ProviderStockContract
CREATE TABLE #TMP_ProviderStockContract
(	[StockID] [int] NOT NULL,
	[ProviderID] [int] NULL,
	[SalvageContractID] [int] NOT NULL,
	[IAAFeeID] [int] NOT NULL,
	[IAAFeeLevel] [nvarchar](15) NOT NULL,
	[RateGroupID] [int] NOT NULL,
	[RateGroupCriteriaID] [int] NOT NULL,
	[IsRateGroup] [int] NOT NULL,
	[ModelYearChangeBasisDate] [nvarchar](50) NULL,
	[ModelYearChangeDayNumber] [smallint] NULL,
	[ModelYearChangeMonthNumber] [smallint] NULL,
	[IBNRunThreshold] [smallint] NULL,
	[IBNRunChargeThreshold] [smallint] NULL,
	[ETLLoadID] [int] NOT NULL,
	[ETLLoadUpdateID] [int] NOT NULL,
	[ProviderStockContractEffectiveDateTime] [datetime] NULL,
	[ProviderStockContractEffectiveDayId] [int] NULL,
	[ProviderStockContractExpirationDateTime] [datetime] NULL,
	[ProviderStockContractExpirationDayId] [int] NULL,
	[IAAFeeEffectiveDateTime] [datetime] NULL,
	[IAAFeeEffectiveDayId] [int] NULL,
	[IAAFeeExpriationDateTime] [datetime] NULL,
	[IAAFeeExpriationDayId] [int] NULL,
	[ProviderAgreementTermTypeDescription] [nvarchar](40) NULL,
	[ProviderAgreementTermMethodTypeDescription] [nvarchar](40) NULL,
	SettlementApprovalGroupDescription [nvarchar](40) NULL
	)

IF OBJECT_ID('tempdb..#DimProviderStockContractRecordCount') IS NOT NULL DROP TABLE #DimProviderStockContractRecordCount
CREATE TABLE #DimProviderStockContractRecordCount (ChangeType VARCHAR(10)) 
/*********************************************************************************************
Create a temp table and Insert the values from the CDC Salvage Provider table.
*********************************************************************************************/
INSERT INTO #TMP_Salvage_Contract_Info
   ([Salvage_Contract_Info_ID],
	[Salvage_ID],
	[Agreement_Term_Type_Code],
	[Update_DateTime],
	[Update_User_ID],
	[Manually_Revalued_Ind],
	[Settle_By_Date],
	[Flipped_to_PA_Ind],
	[Salvage_Contract_ID],
	[Agreement_Changed_Ind],
	[Fee_ID],
	[Hybrid_Agreement_Ind],
	[Revalue_At_Title_Received_Ind]
	) 	
SELECT	
	[Salvage_Contract_Info_ID],
	[Salvage_ID],
	[Agreement_Term_Type_Code],
	[Update_DateTime],
	[Update_User_ID],
	[Manually_Revalued_Ind],
	[Settle_By_Date],
	[Flipped_to_PA_Ind],
	[Salvage_Contract_ID],
	[Agreement_Changed_Ind],
	[Fee_ID],
	[Hybrid_Agreement_Ind],
	[Revalue_At_Title_Received_Ind]
FROM [BI_ASAP_Rep].cdc.[fn_cdc_get_net_changes_dbo_Salvage_Contract_Info]
   (@StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5
   

--SELECT @ExtractRowCount_sp = COUNT(*) FROM #TMP_Salvage_Contract_Info


/*********************************************************************************************
Build #TMP_ProviderStockContract.
*********************************************************************************************/
INSERT INTO #TMP_ProviderStockContract
   ([StockID],
	[ProviderID],
	[SalvageContractID],
	[IAAFeeID],
	[IAAFeeLevel],
	[RateGroupID],
	[RateGroupCriteriaID],
	[IsRateGroup],
	[ModelYearChangeBasisDate],
	[ModelYearChangeDayNumber],
	[ModelYearChangeMonthNumber],
	[IBNRunThreshold],
	[IBNRunChargeThreshold],
	[ETLLoadID],
	[ETLLoadUpdateID],
	[ProviderStockContractEffectiveDateTime],
	[ProviderStockContractEffectiveDayId],
	[ProviderStockContractExpirationDateTime],
	[ProviderStockContractExpirationDayId],
	[IAAFeeEffectiveDateTime],
	[IAAFeeEffectiveDayId],
	[IAAFeeExpriationDateTime],
	[IAAFeeExpriationDayId],
	[ProviderAgreementTermTypeDescription],
	[ProviderAgreementTermMethodTypeDescription],
	SettlementApprovalGroupDescription
	) 	
SELECT	Distinct
	S.StockID
	,ISNULL(SPI.Salvage_Provider_ID, 0)
	,ISNULL(TSCI.Salvage_Contract_ID, 0)
	,ISNULL(IAA_Fee.Fee_ID, 0)
	,ISNULL(IAA_Fee_Level.IAA_Fee_Level, 0)
	,ISNULL(RG.Rate_Group_ID, 0)
	,ISNULL(RGC.Rate_Group_Criteria_ID, 0)
	,ISNULL(CONVERT(INT, SCII.IAA_Rate_Group_Ind), 0)
	,ISNULL(RG.Model_Year_Change_Basis_Date, '')
	,ISNULL(SPC.Model_Year_Change_Day_Number, 0)
	,ISNULL(SPC.Model_Year_Change_Month_Number, 0)
	,ISNULL(RG.IBN_Run_Threshold, 0)
	,ISNULL(RG.IBN_Run_Charge_Threshold, 0)
	,@ETLLoadID
	,@ETLLoadID
	,ISNULL(SPC.Effective_Date, '1980-01-01')
	,ISNULL(cast(convert(varchar(8),SPC.Effective_Date,112) as int),19800101)
	,ISNULL(SPC.Expiration_Date, '2030-12-31')
	,ISNULL(cast(convert(varchar(8),SPC.Expiration_Date,112) as int),20301231)
	,ISNULL(IAA_Fee.Effective_Date, '1980-01-01')
	,ISNULL(cast(convert(varchar(8),IAA_Fee.Effective_Date,112) as int),29991231)
	,ISNULL(IAA_Fee.Expiration_Date, '2030-12-31')
	,ISNULL(cast(convert(varchar(8),IAA_Fee.Expiration_Date,112) as int),20301231)
	,ISNULL(ATT.Agreement_Term_Type_Description, '')
	,''AS [ProviderAgreementTermMethodTypeDescription]
	,ISNULL(SAG.[Settlement_Approval_Group_Description],'') AS SettlementApprovalGroupDescription
FROM #TMP_Salvage_Contract_Info TSCI
INNER JOIN dbo.DimStock S WITH(NOLOCK)
	ON TSCI.Salvage_ID = S.ASAPSalvageID
INNER JOIN [BI_ASAP_REP].dbo.Salvage_Contract_Info_Item AS SCII WITH(NOLOCK)
	ON TSCI.Salvage_Contract_Info_ID = SCII.Salvage_Contract_Info_ID
	AND SCII.Sequence_Number = 1	
INNER JOIN [BI_ASAP_REP].dbo.Salvage_Provider_Info SPI WITH(NOLOCK)
		ON TSCI.Salvage_ID = SPI.Salvage_ID	
INNER JOIN (
			select Fee_ID, 'National' AS 'IAA_Fee_Level' from [BI_ASAP_REP].dbo.IAA_Fee WITH(NOLOCK)
			where national_ind = 1
			UNION ALL
			select Fee_ID, 'Region' AS 'IAA_Fee_Level' from [BI_ASAP_REP].dbo.IAA_Fee WITH(NOLOCK)
			where Region_Number is not null
			UNION ALL
			select Fee_ID, 'Branch' AS 'IAA_Fee_Level' from [BI_ASAP_REP].dbo.IAA_Fee WITH(NOLOCK)
			where Branch_Number is not null
			) AS IAA_Fee_Level
	ON TSCI.Fee_ID = IAA_Fee_Level.Fee_ID	
INNER JOIN [BI_ASAP_REP].dbo.IAA_Fee WITH(NOLOCK)  
	ON TSCI.Fee_ID = IAA_Fee.Fee_ID
INNER JOIN [BI_ASAP_REP].dbo.Rate_Group RG WITH(NOLOCK)
	ON SCII.Rate_Group_ID = RG.Rate_Group_ID
INNER JOIN BI_ASAP_REP.dbo.Rate_Group_Criteria RGC WITH(NOLOCK)
	ON SCII.Rate_Group_Criteria_ID = RGC.Rate_Group_Criteria_ID
LEFT OUTER JOIN [BI_ASAP_REP].dbo.[Salvage_Provider_Contract] SPC WITH(NOLOCK)
	ON TSCI.Salvage_Contract_ID = SPC.Salvage_Contract_ID
LEFT OUTER JOIN [BI_ASAP_REP].dbo.Agreement_Term_Type ATT WITH(NOLOCK)
   ON TSCI.Agreement_Term_Type_Code = ATT.Agreement_Term_Type_Code
--LEFT OUTER JOIN [BI_ASAP_REP].dbo.dbo.Salvage_Contract_Detail_Item SCDI ;
LEFT OUTER JOIN  [BI_ASAP_REP].dbo.Rate_Group_Iaa_Branch  RGIB WITH (NOLOCK)
	ON RG.Rate_Group_ID = RGIB.Rate_Group_ID
		AND S.AdministrativeBranchNumber = RGIB.[Branch_Number]
LEFT OUTER JOIN  [BI_ASAP_REP].dbo.Settlement_Approval_Group SAG WITH (NOLOCK)
	ON RGIB.[Settlement_Approval_Group_ID] = SAG.[Settlement_Approval_Group_ID]
   
--/*********************************************************************************************
--MERGE the data from the temp table into the EDW DimProviderStockContract table
--This does Update and Insert only.  No Deletes
--*********************************************************************************************/

BEGIN TRANSACTION 
  
    MERGE [dbo].DimProviderStockContract AS DPSC
    USING #TMP_ProviderStockContract AS TPSC
          ON TPSC.[StockID] = DPSC.[StockID]
	WHEN MATCHED THEN 
     UPDATE SET 
    [ProviderID] = TPSC.[ProviderID],
	[SalvageContractID] = TPSC.[SalvageContractID],
	[IAAFeeID] = TPSC.[IAAFeeID],
	[IAAFeeLevel] = TPSC.[IAAFeeLevel] ,
	[RateGroupID] = TPSC.[RateGroupID],
	[RateGroupCriteriaID] = TPSC.[RateGroupCriteriaID],
	[IsRateGroup]= TPSC.[IsRateGroup],
	[ModelYearChangeBasisDate] = TPSC.[ModelYearChangeBasisDate],
	[ModelYearChangeDayNumber] = TPSC.[ModelYearChangeDayNumber],
	[ModelYearChangeMonthNumber] = TPSC.[ModelYearChangeMonthNumber],
	[IBNRunThreshold] = TPSC.[IBNRunThreshold],
	[IBNRunChargeThreshold] = TPSC.[IBNRunChargeThreshold],
	[ETLLoadUpdateID] = TPSC.[ETLLoadUpdateID],
	[ProviderStockContractEffectiveDateTime] = TPSC.[ProviderStockContractEffectiveDateTime],
	[ProviderStockContractEffectiveDayId] = TPSC.[ProviderStockContractEffectiveDayId] ,
	[ProviderStockContractExpirationDateTime] = TPSC.[ProviderStockContractExpirationDateTime],
	[ProviderStockContractExpirationDayId] = TPSC.[ProviderStockContractExpirationDayId],
	[IAAFeeEffectiveDateTime] = TPSC.[IAAFeeEffectiveDateTime],
	[IAAFeeEffectiveDayId] = TPSC.[IAAFeeEffectiveDayId],
	[IAAFeeExpriationDateTime] = TPSC.[IAAFeeExpriationDateTime],
	[IAAFeeExpriationDayId] = TPSC.[IAAFeeExpriationDayId],
	[ProviderAgreementTermTypeDescription] = TPSC.[ProviderAgreementTermTypeDescription],
	[ProviderAgreementTermMethodTypeDescription] = TPSC.ProviderAgreementTermMethodTypeDescription,
	SettlementApprovalGroupDescription = TPSC.SettlementApprovalGroupDescription
			
			
	WHEN NOT MATCHED THEN
    INSERT( [StockID],
	[ProviderID],
	[SalvageContractID],
	[IAAFeeID],
	[IAAFeeLevel],
	[RateGroupID],
	[RateGroupCriteriaID],
	[IsRateGroup],
	[ModelYearChangeBasisDate],
	[ModelYearChangeDayNumber],
	[ModelYearChangeMonthNumber],
	[IBNRunThreshold],
	[IBNRunChargeThreshold],
	[ETLLoadID],
	[ETLLoadUpdateID],
	[ProviderStockContractEffectiveDateTime],
	[ProviderStockContractEffectiveDayId],
	[ProviderStockContractExpirationDateTime],
	[ProviderStockContractExpirationDayId],
	[IAAFeeEffectiveDateTime],
	[IAAFeeEffectiveDayId],
	[IAAFeeExpriationDateTime],
	[IAAFeeExpriationDayId],
	[ProviderAgreementTermTypeDescription],
	[ProviderAgreementTermMethodTypeDescription],
	SettlementApprovalGroupDescription
	) 	
    VALUES (TPSC.[StockID],
	TPSC.[ProviderID],
	TPSC.[SalvageContractID],
	TPSC.[IAAFeeID],
	TPSC.[IAAFeeLevel],
	TPSC.[RateGroupID],
	TPSC.[RateGroupCriteriaID],
	TPSC.[IsRateGroup],
	TPSC.[ModelYearChangeBasisDate],
	TPSC.[ModelYearChangeDayNumber],
	TPSC.[ModelYearChangeMonthNumber],
	TPSC.[IBNRunThreshold],
	TPSC.[IBNRunChargeThreshold],
	TPSC.[ETLLoadID],
	TPSC.[ETLLoadUpdateID],
	TPSC.[ProviderStockContractEffectiveDateTime],
	TPSC.[ProviderStockContractEffectiveDayId],
	TPSC.[ProviderStockContractExpirationDateTime],
	TPSC.[ProviderStockContractExpirationDayId],
	TPSC.[IAAFeeEffectiveDateTime],
	TPSC.[IAAFeeEffectiveDayId],
	TPSC.[IAAFeeExpriationDateTime],
	TPSC.[IAAFeeExpriationDayId],
	TPSC.[ProviderAgreementTermTypeDescription],
	TPSC.[ProviderAgreementTermMethodTypeDescription],
	SettlementApprovalGroupDescription) 	
OUTPUT $action INTO #DimProviderStockContractRecordCount;
	
	  

/*********************************************************************************************
Get the counts from the MERGE operation
*********************************************************************************************/
SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #DimProviderStockContractRecordCount


--/*********************************************************************************************
--Return the change counts
--*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		COMMIT TRAN
		
		IF OBJECT_ID('tempdb..#TMP_Salvage_Contract_Info') IS NOT NULL DROP TABLE #TMP_Salvage_Contract_Info
		IF OBJECT_ID('tempdb..#TMP_ProviderStockContract') IS NOT NULL DROP TABLE #TMP_ProviderStockContract
		IF OBJECT_ID('tempdb..#DimProviderStockContractRecordCount') IS NOT NULL DROP TABLE #DimProviderStockContractRecordCount
		
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

		IF OBJECT_ID('tempdb..#TMP_Salvage_Contract_Info') IS NOT NULL DROP TABLE #TMP_Salvage_Contract_Info
		IF OBJECT_ID('tempdb..#TMP_ProviderStockContract') IS NOT NULL DROP TABLE #TMP_ProviderStockContract
		IF OBJECT_ID('tempdb..#DimProviderStockContractRecordCount') IS NOT NULL DROP TABLE #DimProviderStockContractRecordCount

		RETURN -1
	END CATCH
	END
