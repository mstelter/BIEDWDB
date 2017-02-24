/*  
===============================================================================================================
Author            : Somesh Velupalli
Create date : 09/02/2011
Description : Populate the EDW FactFinancialTransaction table from CDC   
================================================================================================================
REVISION History
ChangeDate  Developer         Release/Problem Number  
11/14/2011	Juan X Jacome	  --Added Code for Phase 2 Sprint 2
12/16/2001  Juan X Jacome     --Updated logic for SalvageProviderCharge if they have records in History Table
12/28/2011  Preetham Duvva    --Added the logic for it to update the matching records 
								in DimFinancialTransactionSubType
2/2/2012	Juan X. Jacome	  -- Removed the update logic in merge for DimFinancialTransactionSubType	
2/14/2012	Jua X. Jacome	  -- Added ProviderID and BuyerID for Salvage Expense			
3/7/2012	Juan X. Jacome	  -- Added UpdateDateTime field for Charges/Expenses	
8/28/2012	Matt Stelter	  --Fix for  SE.Reference_Text = '.'
8/31/2012   Preetham Duvva      Added BuyerChargeBranchNumber
4/16/2012	Matt Stelter	  --Fix for  SE.Reference_Text		
7/11/2016  Pratyusha Koduru	--Added CreateDateTime for Charge/expense
7/25/2016  Pratyusha Koduru --Added NegativeFuelCharge
11/7/2016	  Matt Stelter	--Removed logic for FinancialTransactionKeyID
01/03/2017 Mohammad Waheed --Added ASAP_Object_Type_ID and Update Logic including for VendorID
================================================================================================================
Usage Example:
Exec usp_ETLFactFinancialTransaction
================================================================================================================
*/

CREATE PROCEDURE [dbo].[usp_ETLFactFinancialTransaction]  
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

           
--Get the Min and Max LSN's

SELECT @MinLSN = MinLSN, @MaxLSN = MaxLSN 
FROM dbo.ETLRun WHERE ETLJobName = 'DailyETLJob'


/*********************************************************************************************
This ETL will have Stock expense, Provider charge & Buyer Charge
*********************************************************************************************/

/*********************************************************************************************
Create temp table to capture all the CDC changes in Salvage Expense
*********************************************************************************************/
--SELECT @MinLSN = BI_ASAP_Rep.sys.fn_cdc_get_min_lsn('dbo_Salvage_Expense')
--SELECT @MaxLSN  = BI_ASAP_Rep.sys.fn_cdc_get_max_lsn()


IF OBJECT_ID('tempdb..#TMP_Salvage_Expense') IS NOT NULL DROP TABLE #TMP_Salvage_Expense
CREATE TABLE #TMP_Salvage_Expense (
  Salvage_Expense_ID int NOT NULL,
  Charge_Type_ID int NOT NULL,
  Stock_ID int  NULL,
  Charge_Day_ID [int] NULL,
  Charge_DateTime [datetime] NULL,
  Status_Code varchar(50) NULL,
  Status_Description varchar(50) NULL,
  Charge_Amount money NULL,
  Tax_Amount [money] NULL,
  Currency varchar(30) NULL,
  TowBill_number int NULL,
  ResponsiblePartyCode varchar(10),
  UpdateDayID int NULL,
  UpdateDateTime [datetime] NULL,
  PaymentStatusCode char(3)NULL,
  ProviderID int NULL,
  BuyerID int NULL,
  Create_DateTime [datetime] NULL,
  [ASAP_Object_Type_ID]   tinyint null
 )

--Create temp table to capture the row count

IF OBJECT_ID('tempdb..#TMP_SalvageExpenseRecordCount') IS NOT NULL DROP TABLE #TMP_SalvageExpenseRecordCount
CREATE TABLE #TMP_SalvageExpenseRecordCount(ChangeType VARCHAR(30))

IF OBJECT_ID('tempdb..#TMP_SalvageBuyerCharge') IS NOT NULL DROP TABLE #TMP_SalvageBuyerCharge
CREATE TABLE #TMP_SalvageBuyerCharge
(
  Salvage_Buyer_Charge_ID int NOT NULL,
  Charge_Type_ID [int] NOT NULL,
  Salvage_Expense_ID [int] NULL,
  Stock_ID int NULL,
  Buyer_ID int null,
  Charge_Day_ID [int] NULL,
  Charge_DateTime [datetime] NULL,
  Status_Code [char](3) NULL,
  Status_Description varchar(50) NULL,
  Charge_Amount money NULL,
  Tax_Amount [money] NULL,
  Override_amount [money] NULL,
  Currency varchar(30) NULL,
  TowBill_number int NULL,
  Update_User_ID int NULL,
  UpdateDayID [int] NULL,
  UpdateDateTime [datetime] NULL,
  OverrideReasonDescription [varchar] (40) NULL,
  Create_DateTime [datetime] NULL,
  ASAP_Object_Type_ID tinyint null
   ) 

IF OBJECT_ID('tempdb..#TMP_BuyerChargeRecordCount') IS NOT NULL DROP TABLE #TMP_BuyerChargeRecordCount
CREATE TABLE #TMP_BuyerChargeRecordCount(ChangeType VARCHAR(30))

IF OBJECT_ID('tempdb..#TMP_BuyerCharge') IS NOT NULL DROP TABLE #TMP_BuyerCharge
CREATE TABLE #TMP_BuyerCharge
(
  Buyer_Charge_ID int NOT NULL,
  Charge_Type_ID [int] NOT NULL,
  Salvage_Expense_ID [int] NULL,
  Stock_ID int NULL,
  Buyer_ID int null,
  Charge_Day_ID [int] NULL,
  Charge_DateTime [datetime] NULL,
  Status_Code [char](3) NULL,
  Status_Description varchar(50) NULL,
  Charge_Amount money NULL,
  Tax_Amount [money] NULL,
  Override_amount [money] NULL,
  Currency varchar(30) NULL,
  TowBill_number int NULL,
  Update_User_ID int NULL,
  UpdateDayID [int] NULL,
  UpdateDateTime [datetime] NULL,
  OverrideReasonDescription [varchar] (40) NULL,
  BranchNumber int NULL,
  Create_DateTime [datetime] NULL
  ) 

IF OBJECT_ID('tempdb..#TMP_SalvageBuyerChargeRecordCount') IS NOT NULL DROP TABLE #TMP_SalvageBuyerChargeRecordCount
CREATE TABLE #TMP_SalvageBuyerChargeRecordCount(ChangeType VARCHAR(30))

 IF OBJECT_ID('tempdb..#TMP_MIN_salvage_provider_charge_history') IS NOT NULL DROP TABLE #TMP_MIN_salvage_provider_charge_history
 CREATE TABLE #TMP_MIN_salvage_provider_charge_history(Salvage_Provider_Charge_ID int NULL, Min_Update_DateTime Datetime NULL)
  
 IF OBJECT_ID('tempdb..#TMP_Provider_Charge') IS NOT NULL DROP TABLE #TMP_Provider_Charge
CREATE TABLE #TMP_Provider_Charge
(
  Provider_Charge_ID int NOT NULL,
  Charge_Type_ID [int] NOT NULL,
  Salvage_Expense_ID int NULL ,
  Stock_ID int NULL,
  ProviderID int null,
  Charge_Day_ID [int] NULL,
  Charge_DateTime [datetime] NULL,
  Status_Code [char](3) NULL,
  Status_Description varchar(50) NULL,
  Charge_Amount money NULL,
  Tax_Amount [money] NULL,
  Override_amount [money] NULL,
  Currency varchar(30) NULL,
  TowBill_number int NULL,
  Update_User_ID int NULL,
  UpdateDayID [int] NULL,
  UpdateDateTime [datetime] NULL,
  OverrideReasonDescription [varchar] (40) NULL,
  Create_User_ID int NULL,
  Create_DateTime [datetime] NULL,
  ASAP_Object_Type_ID tinyint NULL
)

IF OBJECT_ID('tempdb..#tmp_ProviderChargeRecordCount') IS NOT NULL DROP TABLE #TMP_ProviderChargeRecordCount
CREATE TABLE #TMP_ProviderChargeRecordCount(ChangeType VARCHAR(30))

 IF OBJECT_ID('tempdb..#TMP_FactFinancialTransaction') IS NOT NULL DROP TABLE #TMP_FactFinancialTransaction
CREATE TABLE #TMP_FactFinancialTransaction(
	[FinancialTransactionID] [int] NOT NULL,
	[FinancialTransactionTypeID] [int] NOT NULL,
	[SalvageExpenseID] [int] NOT NULL,
	[StockID] [int] NOT NULL,
	[BuyerID] [int] NOT NULL,
	[ProviderID] [int] NOT NULL,
	[TransactionDayID] [int] NULL,
	[TransactionDateTime] [datetime] NULL,
	[Amount] [money] NULL,
	[TaxAmount] [money] NULL,
	[OverrideAmount] [money] NULL,
	[Currency] [varchar](30) NULL,
	[TowBillNumber] [int] NULL,
	[IsVoid] [tinyint] NOT NULL,
	[UpdateUserID] [int] NULL,
	[UpdateDayID] [int] NULL,
	[UpdateDateTime] [datetime] NULL,
	[CreateUserID] [int] NULL,
	[FinancialTransactionSubTypeID] [int] NULL,
	[ChargeStatus] [varchar](40),
	[OverrideReasonDescription] [varchar](40) NULL,
	[CostResponsibleParty] [varchar](40),
	[ETLLoad_TransID] [int],
	[BuyerChargeBranchNumber] [int] NULL,
	[CreateDateTime] [datetime] NULL,
    [VendorID] Varchar(20) Null,
	ASAP_Object_Type_ID tinyint null,
	[DimVendorID] BigInt null
	)
	
IF OBJECT_ID('tempdb..#TMP_FactFinancialTransactionRecordCount') IS NOT NULL DROP TABLE #TMP_FactFinancialTransactionRecordCount
CREATE TABLE #TMP_FactFinancialTransactionRecordCount(ChangeType VARCHAR(30))



IF OBJECT_ID('tempdb..#TMP_FinancialTransactionSubType') IS NOT NULL DROP TABLE #TMP_FinancialTransactionSubType
CREATE TABLE #TMP_FinancialTransactionSubType(
	[FinancialTransactionTypeID] [int] NULL,
	[ChargeStatus] [varchar](40) NULL,
	[OverrideReasonDescription] [varchar](40) NULL,
	[CostResponsibleParty] [varchar](40) NULL
	)
	
IF OBJECT_ID('tempdb..#TMP_FinancialTransactionSubTypeRecordCount') IS NOT NULL DROP TABLE #TMP_FinancialTransactionSubTypeRecordCount
CREATE TABLE #TMP_FinancialTransactionSubTypeRecordCount(ChangeType VARCHAR(30))

IF OBJECT_ID('tempdb..#TMP_FinancialTransactionRecordCount') IS NOT NULL DROP TABLE #TMP_FinancialTransactionRecordCount
CREATE TABLE #TMP_FinancialTransactionRecordCount(ChangeType VARCHAR(30))
	

--/************************************************************************************************************************/

----/*********************************************************************************************
----Insert the values from the CDC Salvage Buyer_Charge table.
----*********************************************************************************************/
  INSERT INTO #TMP_SalvageBuyerCharge
( 
  Salvage_Buyer_Charge_ID,
  Charge_Type_ID,
  Salvage_Expense_ID,
  Stock_ID,
  Buyer_ID,
  Charge_Day_ID,
  Charge_DateTime,
  Status_Code,
  Status_Description,
  Charge_Amount,
  Tax_Amount,
  Override_amount,
  TowBill_number,
  Update_User_ID,
  UpdateDayID,
  UpdateDateTime,
  OverrideReasonDescription,
  Create_DateTime,
   ASAP_Object_Type_ID
   )

SELECT 
         SBC.Salvage_Buyer_Charge_ID,
         SBC.Charge_Type_ID,
         SE.Salvage_Expense_ID,
         ISNULL(MAX(DS.StockID),1000000),
         SBC.Buyer_ID,
         CONVERT(INT, CONVERT(CHAR(8),SBC.Charge_DateTime, 112)) AS Charge_Day_ID,
         SBC.Charge_DateTime,
         SBC.Status_Code,
         S.Status_Description,
         SBC.Charge_Amount,
         SBC.Tax_Amount,
         SBC.Charge_Override_Amount,
         (CASE WHEN ISNUMERIC(SE.Reference_Text) 	= 1 
		 					AND PATINDEX('%[^0-9]%',SE.Reference_Text) = 0
							AND SE.Reference_Text <> '.'
					AND 	LEN(LTRIM(RTRIM(SE.Reference_Text))) 	< 10
					AND CAST(SE.Reference_Text AS float) < 999999999
					THEN 	CAST(round(CAST(SE.Reference_Text AS float),0) AS INT)
					ELSE 	0
					END  ) AS  TowBill_Number,
		SBC.Update_User_ID,
		 CONVERT(INT, CONVERT(CHAR(8),SBC.Update_DateTime, 112)) AS UpdateDayID,		
		 SBC.Update_DateTime,	
		 ISNULL(ORR.Override_Reason_Description, ''),
		 SBC.Charge_DateTime,
		 SE.ASAP_Object_Type_ID
  FROM [BI_ASAP_Rep].cdc.fn_cdc_get_net_changes_dbo_Salvage_Buyer_Charge
  ( @MinLSN,@MaxLSN,'all with merge') AS SBC
  LEFT OUTER JOIN BI_ASAP_Rep.dbo.Salvage_Expense AS SE WITH (NOLOCK)
  ON SBC.Salvage_Expense_ID = SE.Salvage_Expense_ID
  INNER JOIN BI_ASAP_Rep.dbo.Status AS S WITH (NOLOCK)
  ON SBC.Status_Code = S.Status_Code
  LEFT OUTER JOIN dbo.DimStock AS DS WITH (NOLOCK)
  ON SBC.Salvage_ID = DS.ASAPSalvageID
  LEFT OUTER JOIN BI_ASAP_REP.dbo.Override_Reason AS ORR WITH (NOLOCK)
  ON SBC.Override_Reason_Code = ORR.Override_Reason_Code
  WHERE [__$operation] = 5
  GROUP BY 
           SBC.Salvage_Buyer_Charge_ID,
         SBC.Charge_Type_ID,
         SE.Salvage_Expense_ID,
         SBC.Buyer_ID,
         CONVERT(INT, CONVERT(CHAR(8),SBC.Charge_DateTime, 112)),
         SBC.Charge_DateTime,
         SBC.Status_Code,
         S.Status_Description,
         SBC.Charge_Amount,
         SBC.Tax_Amount,
         SBC.Charge_Override_Amount,
         (CASE WHEN ISNUMERIC(SE.Reference_Text) 	= 1 
		 					AND PATINDEX('%[^0-9]%',SE.Reference_Text) = 0
							AND SE.Reference_Text <> '.'
					AND 	LEN(LTRIM(RTRIM(SE.Reference_Text))) 	< 10
					AND CAST(SE.Reference_Text AS float) < 999999999
					THEN 	CAST(round(CAST(SE.Reference_Text AS float),0) AS INT)
					ELSE 	0
					END  ),
		 SBC.Update_User_ID,
		 CONVERT(INT, CONVERT(CHAR(8),SBC.Update_DateTime, 112)),		
		 SBC.Update_DateTime,	
		 ISNULL(ORR.Override_Reason_Description, ''),
		 SBC.Charge_DateTime, 
		 SE.ASAP_Object_Type_ID

----/*********************************************************************************************
----Insert the values from the CDC Buyer_Charge table.
----*********************************************************************************************/
  INSERT INTO #TMP_BuyerCharge
( Buyer_Charge_ID,
  Charge_Type_ID,
  Buyer_ID,
  Charge_Day_ID,
  Charge_DateTime,
  Status_Code,
  Status_Description,
  Charge_Amount,
  Tax_Amount,
  Override_amount,
  Update_User_ID,
  UpdateDayID,
  UpdateDateTime,
  OverrideReasonDescription,
  BranchNumber,
  Create_DateTime 
   )

SELECT 
         BC.Buyer_Charge_ID,
         BC.Charge_Type_ID,
         BC.Buyer_ID,
         CONVERT(INT, CONVERT(CHAR(8),BC.Charge_DateTime, 112)) AS Charge_Day_ID,
         BC.Charge_DateTime,
         BC.Status_Code,
         S.Status_Description,
         BC.Charge_Amount,
         BC.Tax_Amount,
         BC.Charge_Override_Amount,
         BC.Update_User_ID,
		 CONVERT(INT, CONVERT(CHAR(8),BC.Update_DateTime, 112)) AS UpdateDayID,			
		 BC.Update_DateTime,
		 ISNULL(ORR.Override_Reason_Description, ''),
		 BC.Branch_Number,
		 BC.Charge_DateTime 
  FROM [BI_ASAP_Rep].cdc.fn_cdc_get_net_changes_dbo_Buyer_Charge
  ( @MinLSN,@MaxLSN,'all with merge') AS BC
  INNER JOIN BI_ASAP_Rep.dbo.Status AS S WITH (NOLOCK)
  ON BC.Status_Code = S.Status_Code
  LEFT OUTER JOIN BI_ASAP_REP.dbo.Override_Reason AS ORR WITH (NOLOCK)
  ON BC.Override_Reason_Code = ORR.Override_Reason_Code
  WHERE [__$operation] = 5
   
--/*********************************************************************************************
--Capture all the CDC changes in Provider Charge
--*********************************************************************************************/  

INSERT INTO #TMP_MIN_salvage_provider_charge_history
(Salvage_Provider_Charge_ID, Min_Update_DateTime)
 SELECT  SPCH.Salvage_Provider_Charge_ID,
         MIN(SPCH.Update_DateTime) AS Min_Update_DateTime 
 FROM BI_ASAP_Rep.cdc.dbo_Salvage_Provider_Charge_CT AS PCH 
 INNER JOIN BI_ASAP_Rep.dbo.Salvage_Provider_Charge_History AS SPCH
  ON PCH.Salvage_Provider_Charge_ID = SPCH.Salvage_Provider_Charge_ID
 WHERE PCH.[__$start_lsn] BETWEEN @MinLSN AND @MaxLSN 
 GROUP BY SPCH.Salvage_Provider_Charge_ID
 
 
  INSERT INTO #TMP_Provider_Charge
( 
  Provider_Charge_ID ,
  Charge_Type_ID,
  Salvage_Expense_ID,
  Stock_ID,
  ProviderID,
  Charge_Day_ID,
  Charge_DateTime,
  Status_Code,
  Status_Description,
  Charge_Amount,
  Tax_Amount,
  Override_amount,
  TowBill_number,
  Update_User_ID,
  UpdateDayID,
  UpdateDateTime,
  OverrideReasonDescription,
  Create_User_ID,
  Create_DateTime,
  ASAP_Object_Type_ID
 )
SELECT 
         SPC.Salvage_Provider_Charge_ID,
         SPC.Charge_Type_ID,
         SE.Salvage_Expense_ID,
         MAX(DS.StockID),
         SPI.Salvage_Provider_ID,
         CONVERT(INT, CONVERT(CHAR(8),SPC.Create_DateTime, 112)) AS Charge_Day_ID,
         SPC.Create_DateTime,
         SPC.Status_Code,
         SP.Status_Description,
         COALESCE(TMPSPC.Amount,SPC.Amount) AS Charge_Amount,
         SPC.Tax_Amount,
         CASE WHEN TMPSPC.Amount is NULL THEN NULL ELSE  SPC.Amount END  AS Overide_Amount,
        (CASE WHEN ISNUMERIC(SE.Reference_Text) 	= 1 
							AND PATINDEX('%[^0-9]%',SE.Reference_Text) = 0
							AND SE.Reference_Text <> '.'
					AND 	LEN(LTRIM(RTRIM(SE.Reference_Text))) 	< 10
					AND CAST(SE.Reference_Text AS float) < 999999999
					THEN 	CAST(round(CAST(SE.Reference_Text AS float),0) AS INT)
					ELSE 	0
					END  ) AS  TowBill_Number,
		ISNULL(SPC.Update_User_ID,0),
		CONVERT(INT, CONVERT(CHAR(8),SPC.Update_DateTime, 112)) AS UpdateDayID,
		SPC.Update_DateTime,
		ISNULL(ORR.Override_Reason_Description,''),
		SPC.Create_User_ID,
		SPC.Create_DateTime,
		SE.ASAP_Object_Type_ID	
  FROM [BI_ASAP_Rep].cdc.fn_cdc_get_net_changes_dbo_Salvage_Provider_Charge
  ( @MinLSN,@MaxLSN,'all with merge') AS SPC
  LEFT OUTER JOIN BI_ASAP_Rep.dbo.Salvage_Expense AS SE WITH (NOLOCK)
  ON SPC.Salvage_Expense_ID = SE.Salvage_Expense_ID
  INNER JOIN BI_ASAP_Rep.dbo.Salvage_Provider_Info AS SPI WITH (NOLOCK)
  on SPC.Salvage_ID = SPI.Salvage_ID
  INNER JOIN BI_Asap_Rep.dbo.Status AS SP WITH (NOLOCK)
  ON SPC.Status_Code = SP.Status_code
  LEFT OUTER JOIN
  (  SELECT CT.Salvage_Provider_Charge_ID, CT.Amount
      FROM BI_ASAP_Rep.dbo.Salvage_Provider_Charge_History AS CT WITH (NOLOCK)
      INNER JOIN #TMP_MIN_salvage_provider_charge_history AS TMPSPCH WITH (NOLOCK)
        ON CT.Salvage_Provider_Charge_ID = TMPSPCH.Salvage_Provider_Charge_ID
        AND CT.Update_DateTime = TMPSPCH.Min_Update_DateTime   
  ) AS TMPSPC
  ON TMPSPC.Salvage_Provider_Charge_ID = SPC.Salvage_Provider_Charge_ID
  INNER JOIN dbo.DimStock as DS WITH (NOLOCK)
  ON SPC.Salvage_ID = DS.ASAPSalvageID
  LEFT OUTER JOIN BI_ASAP_REP.dbo.Override_Reason AS ORR WITH (NOLOCK)
  ON SPC.Override_Reason_Code = ORR.Override_Reason_Code
  WHERE [__$operation] = 5 
  GROUP BY 
           SPC.Salvage_Provider_Charge_ID,
         SPC.Charge_Type_ID,
         SE.Salvage_Expense_ID,
         SPI.Salvage_Provider_ID,
         CONVERT(INT, CONVERT(CHAR(8),SPC.Create_DateTime, 112)),
         SPC.Create_DateTime,
         SPC.Status_Code,
         SP.Status_Description,
         COALESCE(TMPSPC.Amount,SPC.Amount),
         SPC.Tax_Amount,
         CASE WHEN TMPSPC.Amount is NULL THEN NULL ELSE  SPC.Amount END,
        (CASE WHEN ISNUMERIC(SE.Reference_Text) 	= 1 
							AND PATINDEX('%[^0-9]%',SE.Reference_Text) = 0
							AND SE.Reference_Text <> '.'
					AND 	LEN(LTRIM(RTRIM(SE.Reference_Text))) 	< 10
					AND CAST(SE.Reference_Text AS float) < 999999999
					THEN 	CAST(round(CAST(SE.Reference_Text AS float),0) AS INT)
					ELSE 	0
					END  ),
		ISNULL(SPC.Update_User_ID,0),
		CONVERT(INT, CONVERT(CHAR(8),SPC.Update_DateTime, 112)),
		SPC.Update_DateTime,
		ISNULL(ORR.Override_Reason_Description,''),
		SPC.Create_User_ID,
		SPC.Create_DateTime,
		SE.ASAP_Object_Type_ID	


----/*********************************************************************************************
----Insert the values from the CDC Salvage Expense table.
----*********************************************************************************************/
 INSERT INTO #TMP_Salvage_Expense
( 
  Salvage_Expense_ID ,
  Charge_Type_ID,
  Stock_ID,
  Charge_Day_ID,
  Charge_DateTime,
  Status_Code,
  Status_Description,
  Charge_Amount,
  Tax_Amount,
  TowBill_number,
  ResponsiblePartyCode,
  UpdateDayID,
  UpdateDateTime,
  PaymentStatusCode,
  ProviderID,
  BuyerID,
  Create_DateTime,
   ASAP_Object_Type_ID
  )

SELECT 
         SE.Salvage_Expense_ID,
         SE.Charge_Type_ID,
         ISNULL(MAX(DS.StockID),1000000),
         CONVERT(INT, CONVERT(CHAR(8),SE.Transaction_Date, 112)) AS Charge_Day_ID,
         SE.Transaction_Date,
         SE.Payment_Status_Code,
         S.Status_Description,
         SE.Amount,
         SE.Tax_Amount,
         (CASE WHEN ISNUMERIC(SE.Reference_Text) 	= 1 
					AND PATINDEX('%[^0-9]%',SE.Reference_Text) = 0
					AND SE.Reference_Text <> '.'
					AND 	LEN(LTRIM(RTRIM(SE.Reference_Text))) 	< 10
					AND CAST(SE.Reference_Text AS float) < 999999999
					THEN 	CAST(round(CAST(SE.Reference_Text AS float),0) AS INT)
					ELSE 	0
					END  ) AS  TowBill_Number,
         SE.Responsible_Party_Code,
         CONVERT(INT, CONVERT(CHAR(8),SE.Update_DateTime, 112)) AS UpdateDayID,
         SE.Update_DateTime,
		 SE.Payment_Status_Code,
		 ISNULL(SPI.Salvage_Provider_ID,0),
         ISNULL(SBC.Buyer_ID,0),
		 SE.Create_DateTime,
		 SE.ASAP_Object_Type_ID
  FROM [BI_ASAP_Rep].cdc.fn_cdc_get_net_changes_dbo_Salvage_Expense 
  ( @MinLSN,@MaxLSN,'all with merge') AS SE
   INNER JOIN BI_ASAP_Rep.dbo.Status AS S WITH (NOLOCK)
  ON SE.Payment_Status_Code = S.Status_Code
  INNER JOIN dbo.DimStock AS DS WITH (NOLOCK)
  ON SE.Salvage_ID = DS.ASAPSalvageID
  LEFT OUTER JOIN BI_ASAP_Rep.dbo.Salvage_Provider_Info AS SPI WITH (NOLOCK)
  ON SE.Salvage_ID = SPI.Salvage_ID 
  LEFT OUTER JOIN #TMP_SalvageBuyerCharge AS SBC WITH (NOLOCK)
  ON SE.Salvage_Expense_ID = SBC.Salvage_Expense_ID
  WHERE [__$operation] = 5
  GROUP BY 
         SE.Salvage_Expense_ID,
         SE.Charge_Type_ID,
         CONVERT(INT, CONVERT(CHAR(8),SE.Transaction_Date, 112)),
         SE.Transaction_Date,
         SE.Payment_Status_Code,
         S.Status_Description,
         SE.Amount,
         SE.Tax_Amount,
         (CASE WHEN ISNUMERIC(SE.Reference_Text) 	= 1 
		 					AND PATINDEX('%[^0-9]%',SE.Reference_Text) = 0
							AND SE.Reference_Text <> '.'
					AND 	LEN(LTRIM(RTRIM(SE.Reference_Text))) 	< 10
					AND CAST(SE.Reference_Text AS float) < 999999999
					THEN 	CAST(round(CAST(SE.Reference_Text AS float),0) AS INT)
					ELSE 	0
					END  ),
         SE.Responsible_Party_Code,
         CONVERT(INT, CONVERT(CHAR(8),SE.Update_DateTime, 112)),
		 SE.Update_DateTime,
         SE.Payment_Status_Code,
         ISNULL(SPI.Salvage_Provider_ID,0),
         ISNULL(SBC.Buyer_ID,0),
		 SE.Create_DateTime,
		 SE.ASAP_Object_Type_ID
      
					
--/***********************************************************************************************************************
--* Insert/Update to dbo.DimFinancialTransactionSubType
--*
--*************************************************************************************************************************/

INSERT INTO #TMP_FactFinancialTransaction(
	[FinancialTransactionID],
	[FinancialTransactionTypeID],
	[SalvageExpenseID],
	[StockID],
	[BuyerID],
	[ProviderID],
	[TransactionDayID],
	[TransactionDateTime],
	[Amount],
	[TaxAmount],
	[OverrideAmount],
	[Currency],
	[TowBillNumber],
	[IsVoid],
	[UpdateUserID],
	[UpdateDayID],
	[UpdateDateTime],
	[CreateUserID],
	[FinancialTransactionSubTypeID],
	[ChargeStatus],
	[OverrideReasonDescription],
	[CostResponsibleParty],
	[ETLLoad_TransID],
	[BuyerChargeBranchNumber],
	[CreateDateTime],
	ASAP_Object_Type_ID
  )
  SELECT Provider_Charge_ID ,
  Charge_Type_ID,
  ISNULL(Salvage_Expense_ID,0),
  ISNULL(Stock_ID,1000000),
  0,  
  ProviderID,
  Charge_Day_ID,
  Charge_DateTime,
  Charge_Amount,
  Tax_Amount,
  Override_amount,
  'USD',
  TowBill_number,
  CASE WHEN Status_Code = 'VOD' THEN 1 ELSE 0 END,
  Update_User_ID,
  UpdateDayID,
  UpdateDateTime,
  Create_User_ID,
  NULL,
  CASE  WHEN Status_Code IN ('BKD','BKP','RCG','RCP','RDP','RVD') THEN 'Recognized'
		WHEN Status_Code IN ('DFD','DFP','NEW','UBK')             THEN 'Pending'
		WHEN Status_Code = 'VOD'                                  THEN 'Void'
		ELSE 'N/A' END AS Charge_Status,
  ISNULL(OverrideReasonDescription,''),
  '',  
  2,
  0,
  Create_DateTime,
  ASAP_Object_Type_ID
  FROM #TMP_Provider_Charge
  
  INSERT INTO #TMP_FactFinancialTransaction(
	[FinancialTransactionID],
	[FinancialTransactionTypeID],
	[SalvageExpenseID],
	[StockID],
	[BuyerID],
	[ProviderID],
	[TransactionDayID],
	[TransactionDateTime],
	[Amount],
	[TaxAmount],
	[OverrideAmount],
	[Currency],
	[TowBillNumber],
	[IsVoid],
	[UpdateUserID],
	[UpdateDayID],
	[UpdateDateTime],
	[CreateUserID],
	[FinancialTransactionSubTypeID],
	[ChargeStatus],
	[OverrideReasonDescription],
	[CostResponsibleParty],
	[ETLLoad_TransID],
	[BuyerChargeBranchNumber],
	[CreateDateTime] ,
	ASAP_Object_Type_ID
  )
  SELECT  Salvage_Buyer_Charge_ID,
  Charge_Type_ID,
  ISNULL(Salvage_Expense_ID,0),
  ISNULL(Stock_ID,1000000),
  Buyer_ID,
  0,
  Charge_Day_ID,
  Charge_DateTime,
  Charge_Amount,
  Tax_Amount,
  Override_amount,
  'USD',
  TowBill_number,
  case when status_code = 'VOD' THEN 1 ELSE 0 END,
  Update_User_ID,
  UpdateDayID,
  UpdateDateTime,
  0,
  NULL,
  Status_Description,
  ISNULL(OverrideReasonDescription,''),
  '',
  3,
  0,
  Create_DateTime,
  ASAP_Object_Type_ID
  FROM #TMP_SalvageBuyerCharge
  
  
  INSERT INTO #TMP_FactFinancialTransaction(
	[FinancialTransactionID],
	[FinancialTransactionTypeID],
	[SalvageExpenseID],
	[StockID],
	[BuyerID],
	[ProviderID],
	[TransactionDayID],
	[TransactionDateTime],
	[Amount],
	[TaxAmount],
	[OverrideAmount],
	[Currency],
	[TowBillNumber],
	[IsVoid],
	[UpdateUserID],
	[UpdateDayID],
	[UpdateDateTime],
	[CreateUserID],
	[FinancialTransactionSubTypeID],
	[ChargeStatus],
	[OverrideReasonDescription],
	[CostResponsibleParty],
	[ETLLoad_TransID],
	[BuyerChargeBranchNumber],
	[CreateDateTime],
	[ASAP_Object_Type_ID]
	)
	SELECT   Salvage_Expense_ID,
  Charge_Type_ID,
  Salvage_Expense_ID,
  Stock_ID,
  BuyerID,
  ProviderID,
  Charge_Day_ID,
  Charge_DateTime,
  Charge_Amount,
  Tax_Amount,
  NULL,
  'USD',
  TowBill_number,
  Case when status_code = 'VOD' THEN 1 ELSE 0 END,
  0,
  UpdateDayID,
  UpdateDateTime,
  0,
  NULL,
  CASE WHEN PaymentStatusCode IN ('CGI','CGS')        THEN 'Recognized'
	   WHEN PaymentStatusCode IN ('INV','NEW','UBK')  THEN 'Pending'
	   WHEN PaymentStatusCode IN ('VOD','NA')         THEN 'Void'
       ELSE 'N/A' END AS Charge_Status,
  '',
  ISNULL(ResponsiblePartyCode,''),
  4,
  0,
  Create_DateTime,
  [ASAP_Object_Type_ID]
  FROM #TMP_Salvage_Expense
  
  
    INSERT INTO #TMP_FactFinancialTransaction(
	[FinancialTransactionID],
	[FinancialTransactionTypeID],
	[SalvageExpenseID],
	[StockID],
	[BuyerID],
	[ProviderID],
	[TransactionDayID],
	[TransactionDateTime],
	[Amount],
	[TaxAmount],
	[OverrideAmount],
	[Currency],
	[TowBillNumber],
	[IsVoid],
	[UpdateUserID],
	[UpdateDayID],
	[UpdateDateTime],
	[CreateUserID],
	[FinancialTransactionSubTypeID],
	[ChargeStatus],
	[OverrideReasonDescription],
	[CostResponsibleParty],
	[ETLLoad_TransID],
	[BuyerChargeBranchNumber],
	CreateDateTime,
	[ASAP_Object_Type_ID]
	)
	SELECT Buyer_Charge_ID,
  Charge_Type_ID,
  0,
  1000000,
  Buyer_ID,
  0,
  Charge_Day_ID,
  Charge_DateTime,
  Charge_Amount,
  Tax_Amount,
  Override_amount,
  'USD',
  0,
  Case when status_code = 'VOD' THEN 1 ELSE 0 END,
  Update_User_ID,
  UpdateDayID,
  UpdateDateTime,
  0,
  NULL,
  Status_Description,
  ISNULL(OverrideReasonDescription,''),
  '',
  5,
  BranchNumber,
  Create_DateTime,
  0
  FROM #TMP_BuyerCharge


UPDATE #TMP_FactFinancialTransaction
	SET [VendorID]=    	 GP.[Vendor_ID] 
FROM  #TMP_FactFinancialTransaction AS tfft WITH (NOLOCK)
	LEFT OUTER JOIN BI_ASAP_Rep.dbo.Salvage_Expense AS SE WITH (NOLOCK)   ON tfft.SalvageExpenseID = SE.Salvage_Expense_ID
    LEFT OUTER JOIN  (Select [ASAP_Object_ID],ASAP_Object_Type_ID,Vendor_ID
			              From[BI_ASAP_Rep].[dbo].[ASAP_GP_XRef]
			              Group by [ASAP_Object_ID],ASAP_Object_Type_ID,Vendor_ID)GP  on se.[ASAP_Object_ID]=GP.[ASAP_Object_ID] and GP.ASAP_Object_Type_ID=se.ASAP_Object_Type_ID 			


Update FFT
set [DimVendorID]=dv.[DimVendorID]
From #TMP_FactFinancialTransaction FFT
inner join EDW.[dbo].[DimVendor] dv on FFT.[ASAP_Object_Type_ID]=dv.[ASAP_Object_Type_ID] and FFT.VendorID=dv.VendorID


--Select * from #TMP_FactFinancialTransaction where [VendorID] is not null and SalvageExpenseID=0
/*************************************************************
*  INSERT INTO FinancialTransactionSubType
*
**************************************************************/    
  
  INSERT INTO #TMP_FinancialTransactionSubType(
	[FinancialTransactionTypeID],
	[ChargeStatus],
	[OverrideReasonDescription],
	[CostResponsibleParty]
	)
  
  Select Distinct FinancialTransactionTypeID,
	ChargeStatus,
	OverrideReasonDescription,
	CostResponsibleParty
  FROM #TMP_FactFinancialTransaction
  
  
  
  EXEC @ETLLoadID_sp = [dbo].[usp_ETLLoadAudit] 
       @StartEnd = 'S',
       @PackageName = 'usp_ETLDFTSubType'
            

BEGIN TRAN 

MERGE dbo.DimFinancialTransactionSubType AS DFTSB
USING #TMP_FinancialTransactionSubType  AS TFTST
      ON DFTSB.FinancialTransactionTypeID = TFTST.FinancialTransactionTypeID
	  AND DFTSB.ChargeStatus = TFTST.ChargeStatus
	  AND DFTSB.OverrideReasonDescription = TFTST.OverrideReasonDescription
	  AND DFTSB.CostResponsibleParty = TFTST.CostResponsibleParty

WHEN NOT MATCHED 
THEN INSERT (FinancialTransactionTypeID,
			 ChargeStatus,
			 OverrideReasonDescription,
			 CostResponsibleParty,
             ETLLoadID,
             ETLLoadUpdateID    
             )  
 VALUES (FinancialTransactionTypeID,
		 ChargeStatus,
		 OverrideReasonDescription,
		 CostResponsibleParty,
         @ETLLoadID_sp,
         @ETLLoadID_sp
	    )
OUTPUT $action INTO #TMP_FinancialTransactionSubTypeRecordCount;	    
  
 SELECT @InsertRowCount_sp = COUNT(*)  FROM #TMP_FinancialTransactionSubTypeRecordCount WHERE ChangeType = 'INSERT'
 SELECT @UpdateRowCount_sp = COUNT(*)  FROM #TMP_FinancialTransactionSubTypeRecordCount WHERE ChangeType = 'UPDATE'
 
 EXEC [dbo].[usp_ETLLoadAudit]
	   @StartEnd = 'E',
	   @ETLLoadID = @ETLLoadID_sp,
	   @ExtractRowCount = @ExtractRowCount_sp,
	   @InsertRowCount = @InsertRowCount_sp,
	   @UpdateRowCount = @UpdateRowCount_sp


COMMIT TRAN


UPDATE #TMP_FactFinancialTransaction
SET [FinancialTransactionSubTypeID] = ISNULL(DFTSB.[FinancialTransactionSubTypeID],0)
FROM #TMP_FactFinancialTransaction TFFT
LEFT OUTER JOIN dbo.DimFinancialTransactionSubType DFTSB
  ON TFFT.FinancialTransactionTypeID = DFTSB.FinancialTransactionTypeID
  AND TFFT.ChargeStatus = DFTSB.ChargeStatus
  AND TFFT.OverrideReasonDescription = DFTSB.OverrideReasonDescription
  AND TFFT.CostResponsibleParty = DFTSB.CostResponsibleParty
  


/***********************************************************************************************************************
* Insert/Update to FactFinancialTransaction
*
*************************************************************************************************************************/
/*********************************************************************************************
Update and insert the EDW FactStockTowTransaction table with #TMP_Stock_Expense table
*********************************************************************************************/
--Run the Audit begin
       EXEC @ETLLoadID_sp = [dbo].[usp_ETLLoadAudit] 
            @StartEnd = 'S',
            @PackageName = 'usp_ETLFFT'
            
            
BEGIN TRANSACTION 

MERGE dbo.FactFinancialTransaction AS FFT
USING #TMP_FactFinancialTransaction  AS TFFT
      ON TFFT.FinancialTransactionID = FFT.FinancialTransactionID
        AND TFFT.FinancialTransactionTypeID = FFT.FinancialTransactionTypeID

WHEN MATCHED THEN 
UPDATE SET
	StockID = TFFT.StockID,
	BuyerID = TFFT.BuyerID,
	ProviderID = TFFT.ProviderID,
	TransactionDayID = TFFT.TransactionDayID,
	TransactionDateTime = TFFT.TransactionDateTime,
	Amount = TFFT.Amount,
	TaxAmount = TFFT.TaxAmount,
	OverrideAmount = TFFT.OverrideAmount,
	Currency = TFFT.Currency,
	TowBillNumber = TFFT.TowBillNumber,
	IsVoid = TFFT.IsVoid,
	UpdateUserID = ISNULL(TFFT.UpdateUserID,0),
	UpdateDayID = ISNULL(TFFT.UpdateDayID,29991231),
	UpdateDateTime = ISNULL(TFFT.UpdateDateTime, '2999-12-31'),
	CreateUserID = ISNULL(TFFT.CreateUserID,0),
	FinancialTransactionSubTypeID = ISNULL(TFFT.FinancialTransactionSubTypeID,0),
	BuyerChargeBranchNumber = ISNULL(TFFT.BuyerChargeBranchNumber,0),
	CreateDateTime = ISNULL(TFFT.CreateDateTime, '2999-12-31'),
	ETLLoadUpdateID = @ETLLoadID_sp,
	[VendorID]=TFFT.[VendorID],
	ASAP_Object_Type_ID=TFFT.ASAP_Object_Type_ID,
	[DimVendorID]=TFFT.[DimVendorID]
WHEN NOT MATCHED 
THEN INSERT (   FinancialTransactionID,
				FinancialTransactionTypeID,
				SalvageExpenseID,
				StockID,
				BuyerID,
				ProviderID,
				TransactionDayID,
				TransactionDateTime,
				Amount,
				TaxAmount,
				OverrideAmount,
				Currency,
				TowBillNumber,
				IsVoid,
				UpdateUserID,
				UpdateDayID,
				UpdateDateTime,
				CreateUserID,
				FinancialTransactionSubTypeID,
				BuyerChargeBranchNumber,
				CreateDateTime,
				ETLLoadID,
				ETLLoadUpdateID,
				[VendorID] ,
				ASAP_Object_Type_ID,
				[DimVendorID]
             )  
 VALUES (   FinancialTransactionID,
			FinancialTransactionTypeID,
			SalvageExpenseID,
			StockID,
			BuyerID,
			ProviderID,
			TransactionDayID,
			TransactionDateTime,
			Amount,
			TaxAmount,
			OverrideAmount,
			Currency,
			TowBillNumber,
			IsVoid,
			ISNULL(UpdateUserID,0),
			ISNULL(UpdateDayID,29991231),
			ISNULL(UpdateDateTime,'2999-12-31'),
			ISNULL(CreateUserID,0),
			ISNULL(FinancialTransactionSubTypeID,0),
			ISNULL(TFFT.BuyerChargeBranchNumber,0),
			ISNULL(CreateDateTime, '2999-12-31'),
            @ETLLoadID_sp,
            @ETLLoadID_sp,
			[VendorID],
			ASAP_Object_Type_ID,
			[DimVendorID]
	    )
   OUTPUT $action INTO #TMP_FactFinancialTransactionRecordCount;	
    
CREATE TABLE #TMP_NegativeCharge
          ( [StockID] INT NOT NULL,
		    [SalvageExpenseID] [int] NOT NULL,
		   	[ChargeTypeID] int NOT NULL,
			[NegativeFuelCharge] [money] NULL,
            TaxAmount [money] NULL,
            TowBillnumber int NULL,
            UpdateDayID int NULL,
            UpdateDateTime [datetime] NULL,
            ProviderID int NULL,
            BuyerID int NULL,
            CreateDateTime [datetime] NULL
          )

 INSERT INTO #TMP_NegativeCharge
( 
  StockID,
  SalvageExpenseID ,
  ChargeTypeID,
  NegativeFuelCharge,
  TaxAmount,
  TowBillnumber,
  UpdateDayID,
  UpdateDateTime,
  ProviderID,
  BuyerID,
  CreateDateTime 
  )
		 SELECT 
		 ISNULL(MAX(DS.StockID),1000000),
         SE.Salvage_Expense_ID,
         SE.Charge_Type_ID,
         SE.Amount,
         SE.Tax_Amount,
         (CASE WHEN ISNUMERIC(SE.Reference_Text) 	= 1 
					AND PATINDEX('%[^0-9]%',SE.Reference_Text) = 0
					AND SE.Reference_Text <> '.'
					AND 	LEN(LTRIM(RTRIM(SE.Reference_Text))) 	< 10
					AND CAST(SE.Reference_Text AS float) < 999999999
					THEN 	CAST(round(CAST(SE.Reference_Text AS float),0) AS INT)
					ELSE 	0
					END  ) AS  TowBill_Number,
         CONVERT(INT, CONVERT(CHAR(8),SE.Update_DateTime, 112)) AS UpdateDayID,
         SE.Update_DateTime,
		 ISNULL(SPI.Salvage_Provider_ID,0),
         ISNULL(SBC.Buyer_ID,0),
		 SE.Create_DateTime
  --FROM [BI_ASAP_Rep].cdc.fn_cdc_get_net_changes_dbo_Salvage_Fuel_Surcharge_Discount_Details
  --(@MinLSN,@MaxLSN,'all with merge') AS SE 
  FROM BI_ASAP_REP.[dbo].[Salvage_Fuel_Surcharge_Discount_Details] AS SE
  INNER JOIN EDW.dbo.DimStock AS DS WITH (NOLOCK)
  ON SE.Salvage_ID = DS.ASAPSalvageID
  LEFT OUTER JOIN BI_ASAP_Rep.dbo.Salvage_Provider_Info AS SPI WITH (NOLOCK)
  ON SE.Salvage_ID = SPI.Salvage_ID 
  LEFT OUTER JOIN BI_ASAP_Rep.dbo.Salvage_Buyer_Charge AS SBC WITH (NOLOCK)
  ON SE.Salvage_Expense_ID = SBC.Salvage_Expense_ID
  WHERE 
  --[__$operation] = 5 AND 
  SE.Charge_Type_ID  IN (460)
  GROUP BY 
         SE.Salvage_Expense_ID,
         SE.Charge_Type_ID,
         SE.Amount,
         SE.Tax_Amount,
         (CASE WHEN ISNUMERIC(SE.Reference_Text) 	= 1 
		 					AND PATINDEX('%[^0-9]%',SE.Reference_Text) = 0
							AND SE.Reference_Text <> '.'
					AND 	LEN(LTRIM(RTRIM(SE.Reference_Text))) 	< 10
					AND CAST(SE.Reference_Text AS float) < 999999999
					THEN 	CAST(round(CAST(SE.Reference_Text AS float),0) AS INT)
					ELSE 	0
					END  ),
         CONVERT(INT, CONVERT(CHAR(8),SE.Update_DateTime, 112)),
		 SE.Update_DateTime,
         ISNULL(SPI.Salvage_Provider_ID,0),
         ISNULL(SBC.Buyer_ID,0),
		 SE.Create_DateTime
      

UPDATE FFT
SET FFT.[NegativeFuelCharge] = ISNULL(TN.NegativeFuelCharge,0.0)
--SELECT * 
FROM #TMP_NegativeCharge TN
INNER JOIN EDW.[dbo].[FactFinancialTransaction] FFT
ON FFT.StockID = TN.StockID
AND FFT.FinancialTransactionId= TN.SalvageExpenseID
 
     
--Update FFT
--set [DimVendorID]=dv.[DimVendorID]
--From EDW.[dbo].[FactFinancialTransaction] FFT
--inner join EDW.[dbo].[DimVendor] dv on FFT.[ASAP_Object_Type_ID]=dv.[ASAP_Object_Type_ID] and FFT.VendorID=dv.VendorID
  
 SELECT @InsertRowCount_sp = COUNT(*)  FROM #TMP_FactFinancialTransactionRecordCount WHERE ChangeType = 'INSERT'
 SELECT @UpdateRowCount_sp = COUNT(*)  FROM #TMP_FactFinancialTransactionRecordCount WHERE ChangeType = 'UPDATE'
 
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
	
		
		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

	RETURN -1
	 
 END CATCH

END











GO


