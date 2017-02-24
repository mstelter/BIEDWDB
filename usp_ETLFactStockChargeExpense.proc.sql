
/* 
USE EDW
GO 
==============================================================================
Author		: Juan X. Jacome
Create date	: 02/02/2012
Description	: Populate the EDW FactStockChargeExpense table from FactFinancialTransaction	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number
2/13/2012	Juan X. Jacome	   --Removed join for Provider ID in #TMP_ProviderStockChargeExpense
2/21/2012   Sidd Murao          Added Provider Tow Type to Provider Charge Tax
2/27/2012	Juan X. Jacome		-- Removed ChargeTypeID 68 from ProviderOtherExpense,ProviderTotalExpense and ProviderTaxExpense
4/18/2012	Juan X. Jacome		-- Added logic StockID <> 1000000 for #TMP_StockID
9/11/2012	Juan X. Jacome		-- Added ChargeTypeID 525 (Program Management Fee) for ProviderOtherCharge,ProviderTotalCharge
9/19/2012	Juan X. Jacome		-- Added FinancialTransactionTypeID 527 Notary Fee for ProviderEnhancementCharge or ProviderTotalCharge
10/17/2012  Preetham Duvva      Added FuelSurcharge
10/19/2012	Sidd Murao			-- Added logic for Other Charges Provider and Buyer
10/24/2012  Sidd Murao          -- Modified the logic for Other Charges Provider and Buyer
1/4/2013	Juan X. Jacome		-- Modified the logic to outer join for DimFinancialTransactionType
5/24/2013	Juan X. Jacome		-- Modified logic for provider tax charge - program management fee tax
6/12/2013	Juan X. Jacome		-- Added ChargeTypeID 526 (Notary Fee) from ProviderEnhancementExpense,ProviderTotalExpense,ProviderTaxExpense
9/26/2013	Juan X. Jacome		-- Update logic for BuyerTaxCharge
11/12/2013	Juan X. Jacome	    -- Added ChargeType ID 529 Title Ad Valorem Tax (Provider Charge) and ChargeType ID 530 Title Ad Valorem Tax (Expense)
12/9/2013	Juan X. Jacome      -- Added logic for Advertising Fee Expense 
1/29/2014   Bruce Kutnick		-- Modified ProviderStockChargeExpense logic to match that of CSA Today; logic changed from using transaction types to
                                   using TransactionCategoryID, TransactionClassID and TransactionCustomerID
2/11/2014	Matt Stelter		--Modified Buyer Charge and Expense logic to match that of CSA Today; logic changed from using transaction types to
                                   using TransactionCategoryID, TransactionClassID and TransactionCustomerID
								   Fixed Buyer Late Charge
10/15/2014 Yogitha Alwarsetty   -- Removed Internet fees (TransactionID - 167,228,249) from Buyerothercharge

10/15/2014 Yogitha Alwarsetty   -- Added logic for BuyerInternetCharge
06/22/2016 Pratyusha Koduru  -- Added ProviderAncillaryCharge and backed out ancillary charges from ProviderTitle,ProviderEnhacement,ProviderOther,ProviderTotal Charges and expenses.
10/20/2016 MAtt Stelter	- Fix for stock stocknumber = 17843541, Amount = 17843541.00
==============================================================================
Usage Example:
exec usp_ETLFactStockChargeExpense
==============================================================================
*/

CREATE PROCEDURE [dbo].[usp_ETLFactStockChargeExpense]
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
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLFactStockChargeExpense'

--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WHERE ETLJobName = 'DailyETLJob'


/*********************************************************************************************
Create the temp tables 
*********************************************************************************************/
IF OBJECT_ID('tempdb..#TMP_StockID') IS NOT NULL DROP TABLE #TMP_StockID
CREATE TABLE #TMP_StockID
(	[StockID] [int] NOT NULL
)

IF OBJECT_ID('tempdb..#TMP_FactStockChargeExpense') IS NOT NULL DROP TABLE #TMP_FactStockChargeExpense
CREATE TABLE #TMP_FactStockChargeExpense
(	[StockID] [int] NOT NULL,
	[AdvanceTowCharge] [money] NULL,
	[AdvanceStorageCharge] [money] NULL,
	[AdvanceOtherCharge] [money] NULL,
	[AdvanceTotalCharge] [money] NULL,
	[AdvanceTaxCharge] [money] NULL,
	[AdvanceTowExpense] [money] NULL,
	[AdvanceStorageExpense] [money] NULL,
	[AdvanceOtherExpense] [money] NULL,
	[AdvanceTotalExpense] [money] NULL,
	[AdvanceTaxExpense] [money] NULL,
	[ProviderCommissionCharge] [money] NULL,
	[ProviderEnhancementCharge] [money] NULL,
	[ProviderOtherCharge] [money] NULL,
	[ProviderStorageCharge] [money] NULL,
	[ProviderTaxCharge] [money] NULL,
	[ProviderTitleCharge] [money] NULL,
	[ProviderTotalCharge] [money] NULL,
	[ProviderTowCharge] [money] NULL,
	[ProviderEnhanceExpense] [money] NULL,
	[ProviderOtherExpense] [money] NULL,
	[ProviderTitleExpense] [money] NULL,
	[ProviderTotalExpense] [money] NULL,
	[ProviderTowExpense] [money] NULL,
	[ProviderTaxExpense] [money] NULL,
	[BuyerCharge] [money] NULL,
	[BuyerPulloutCharge] [money] NULL,
	[BuyerDMVCharge] [money] NULL,
	[BuyerLateCharge] [money] NULL,
	[BuyerLoadingCharge] [money] NULL,
	[BuyerOtherCharge] [money] NULL,
	[BuyerStorageCharge] [money] NULL,
	[BuyerTotalCharge] [money] NULL,
	[BuyerTaxCharge] [money] NULL,
	[BuyerTowCharge] [money] NULL,
	[BuyerDMVExpense] [money] NULL,
	[BuyerOtherExpense] [money] NULL,
	[BuyerTotalExpense] [money] NULL,
	[BuyerTaxExpense] [money] NULL,
	[BuyerTowExpense] [money] NULL,
	[FuelSurcharge]   [money] NULL,
	[BuyerInternetCharge]  [money] NULL,
	[ProviderAncillaryCharge] [money] NULL
)

IF OBJECT_ID('tempdb..#temp_FactStockChargeExpenseRecordCount') IS NOT NULL DROP TABLE #temp_FactStockChargeExpenseRecordCount
CREATE TABLE #temp_FactStockChargeExpenseRecordCount(ChangeType VARCHAR(30))

IF OBJECT_ID('tempdb..#TMP_ProviderStockChargeExpense') IS NOT NULL DROP TABLE #TMP_ProviderStockChargeExpense
CREATE TABLE #TMP_ProviderStockChargeExpense
(	[StockID] [int] NOT NULL,
	[AdvanceTowCharge] [money] NULL,
	[AdvanceStorageCharge] [money] NULL,
	[AdvanceOtherCharge] [money] NULL,
	[AdvanceTotalCharge] [money] NULL,
	[AdvanceTaxCharge] [money] NULL,
	[AdvanceTowExpense] [money] NULL,
	[AdvanceStorageExpense] [money] NULL,
	[AdvanceOtherExpense] [money] NULL,
	[AdvanceTotalExpense] [money] NULL,
	[AdvanceTaxExpense] [money] NULL,
	[ProviderCommissionCharge] [money] NULL,
	[ProviderEnhancementCharge] [money] NULL,
	[ProviderOtherCharge] [money] NULL,
	[ProviderStorageCharge] [money] NULL,
	[ProviderTaxCharge] [money] NULL,
	[ProviderTitleCharge] [money] NULL,
	[ProviderTotalCharge] [money] NULL,
	[ProviderTowCharge] [money] NULL,
	[ProviderEnhanceExpense] [money] NULL,
	[ProviderOtherExpense] [money] NULL,
	[ProviderTitleExpense] [money] NULL,
	[ProviderTotalExpense] [money] NULL,
	[ProviderTowExpense] [money] NULL,
	[ProviderTaxExpense] [money] NULL,
	[FuelSurcharge]   [money] NULL,
	[ProviderAncillaryCharge]  [money] NULL	
)

IF OBJECT_ID('tempdb..#TMP_BuyerStockChargeExpense') IS NOT NULL DROP TABLE #TMP_BuyerStockChargeExpense
CREATE TABLE #TMP_BuyerStockChargeExpense
(	[StockID] [int] NOT NULL,
	[BuyerCharge] [money] NULL,
	[BuyerPulloutCharge] [money] NULL,
	[BuyerDMVCharge] [money] NULL,
	[BuyerLateCharge] [money] NULL,
	[BuyerLoadingCharge] [money] NULL,
	[BuyerOtherCharge] [money] NULL,
	[BuyerStorageCharge] [money] NULL,
	[BuyerTotalCharge] [money] NULL,
	[BuyerTaxCharge] [money] NULL,
	[BuyerTowCharge] [money] NULL,
	[BuyerDMVExpense] [money] NULL,
	[BuyerOtherExpense] [money] NULL,
	[BuyerTotalExpense] [money] NULL,
	[BuyerTaxExpense] [money] NULL,
	[BuyerTowExpense] [money] NULL,
	[BuyerInternetCharge]  [money] NULL
)

/*********************************************************************************************
Create a temp table and Insert the values from the CDC Current Tow_Type table.
*********************************************************************************************/
INSERT INTO #TMP_StockID
(	StockID
)
SELECT DISTINCT StockID
FROM dbo.FactFinancialTransaction ft WITH (NOLOCK)	
WHERE ETLLoadUpdateID IN (SELECT TOP 2 ETLLoadID   -- SET to TOP 2 in PEDWDB
						  FROM ETLLoadAudit WITH (NOLOCK)
						  WHERE PackageName = 'usp_ETLFFT'
						  ORDER BY ETLLoadID DESC
						  )
and StockID <> 1000000	
					  
INSERT INTO #TMP_ProviderStockChargeExpense
(	StockID,
	AdvanceTowCharge,
	AdvanceStorageCharge,
	AdvanceOtherCharge, 
	AdvanceTotalCharge,
	AdvanceTaxCharge,
	AdvanceTowExpense,
	AdvanceStorageExpense,
	AdvanceOtherExpense,
	AdvanceTotalExpense,
	AdvanceTaxExpense,
	ProviderCommissionCharge,
	ProviderEnhancementCharge,
	ProviderOtherCharge,
	ProviderStorageCharge,
	ProviderTaxCharge,
	ProviderTitleCharge,
	ProviderTotalCharge,
	ProviderTowCharge,
	ProviderEnhanceExpense,
	ProviderOtherExpense,
	ProviderTitleExpense,
	ProviderTotalExpense,
	ProviderTowExpense,
	ProviderTaxExpense,
	FuelSurcharge,
    ProviderAncillaryCharge
)
	
SELECT	#TMP_StockID.StockID,
--sum(case when ft.FinancialTransactionTypeID IN (290,291,292,468,294,295,466,297,298,299) 
sum(case when dftt.[TransactionCategoryID] IN (20) AND dftt.TransactionClassID IN (4) AND dftt.TransactionCustomerID IN (3)
THEN ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) AdvanceTowCharge ,

-- sum(case when ft.FinancialTransactionTypeID IN (289) 
sum(case when dftt.[TransactionCategoryID] IN (17) AND dftt.TransactionClassID IN (4) AND dftt.TransactionCustomerID IN (3)
THEN ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) AdvanceStorageCharge ,

--sum(case when ft.FinancialTransactionTypeID IN (275,276,277,278,516,279,280,281,282,283,284,285,370,515,376,287,372,288) 	
sum(case when dftt.[TransactionCategoryID] IN (22) AND dftt.TransactionClassID IN (4) AND dftt.TransactionCustomerID IN (3) 										 
THEN ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) AdvanceOtherCharge ,

--sum(case when ft.FinancialTransactionTypeID IN (275,276,277,278,516,279,280,281,282,283,284,285,370,515,376,287,372,288
--,290,291,292,468,294,295,466,297,298,299,289) 
sum(case when dftt.[TransactionCategoryID] IN (22, 17, 20) AND dftt.TransactionClassID IN (4) AND dftt.TransactionCustomerID IN (3) 
THEN ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) AdvanceTotalCharge ,

--sum(case when ft.FinancialTransactionTypeID IN (275,276,277,278,516,279,280,281,282,283,284,285,370,515,376,287,372,288
--,290,291,292,468,294,295,466,297,298,299,289) 
sum(case when dftt.[TransactionCategoryID] IN (22, 17, 20) AND dftt.TransactionClassID IN (4) AND dftt.TransactionCustomerID IN (3) 
THEN ISNULL(ft.[TaxAmount],0)
ELSE 0
END	) AdvanceTaxCharge ,

sum(case when dftt.[TransactionCategoryID] IN (20) AND dftt.TransactionClassID IN (4) AND dftt.TransactionCustomerID IN (2) 
--sum(case when ft.FinancialTransactionTypeID IN (147,148,149,467,151,152,465,154,155,156) 
THEN ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) AdvanceTowExpense ,



sum(case when dftt.[TransactionCategoryID] IN (17) AND dftt.TransactionClassID IN (4) AND dftt.TransactionCustomerID IN (2) 
--sum(case when ft.FinancialTransactionTypeID IN (146) 
THEN ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0) 
ELSE 0
END	) AdvanceStorageExpense ,


sum(case when dftt.[TransactionCategoryID] IN (22) AND dftt.TransactionClassID IN (4) AND dftt.TransactionCustomerID IN (2) 
--sum(case when ft.FinancialTransactionTypeID IN (132,133,134,135,514,136,137,138,139,140,141,142,369,513,377,144,371,145) 
THEN ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0) 
ELSE 0
END	) AdvanceOtherExpense,


sum(case when dftt.[TransactionCategoryID] IN (22, 17, 20) AND dftt.TransactionClassID IN (4) AND dftt.TransactionCustomerID IN (2) 
--sum(case when ft.FinancialTransactionTypeID IN (147,148,149,467,151,152,465,154,155,156,146,132,133,134,135,514,136,137,138,139,140,141,142,369,513,377,144,371,145) 
THEN ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0) 
ELSE 0
END	) AdvanceTotalExpense,



sum(case when dftt.[TransactionCategoryID] IN (22, 17, 20) AND dftt.TransactionClassID IN (4) AND dftt.TransactionCustomerID IN (2) 
--sum(case when ft.FinancialTransactionTypeID IN (147,148,149,467,151,152,465,154,155,156,146,132,133,134,135,514,136,137,138,139,140,141,142,369,513,377,144,371,145) 
THEN  ISNULL(ft.[TaxAmount],0)
ELSE 0
END	) AdvanceTaxExpense,



sum(case when dftt.[TransactionCategoryID] IN (5) AND dftt.TransactionClassID IN (1) AND dftt.TransactionCustomerID IN (3) 
-- sum(case when ft.FinancialTransactionTypeID IN (164,165,166) 
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) ProviderCommissionCharge,


sum(case when dftt.[TransactionCategoryID] IN (21) AND dftt.TransactionClassID IN (3) AND dftt.TransactionCustomerID IN (3) AND TransactionId NOT in (310,281,280)
--sum(case when ft.FinancialTransactionTypeID IN (270,242,414,273,244,491,493,495,497,243,247,293,251,252,253,455,499,501,503,303,272,256,249,263,257,454,259,271,261,262,305,264,265,266,267,269,274,301,527) 
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) ProviderEnhancementCharge,


sum(case when dftt.[TransactionCategoryID] IN (10) AND dftt.TransactionClassID IN (3) AND dftt.TransactionCustomerID IN (3) AND TransactionId NOT IN (232) 
--sum(case when dftt.transactioncustomerid = 3 and dftt.transactioncategoryid = 10 
THEN ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) ProviderOtherCharge,


sum(case when dftt.[TransactionCategoryID] IN (18) AND dftt.TransactionClassID IN (2) AND dftt.TransactionCustomerID IN (3) 
--sum(case when ft.FinancialTransactionTypeID IN (168) 
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) ProviderStorageCharge,


--sum(case when ft.FinancialTransactionTypeID IN (164,165,166,270,242,414,273,244,491,493,495,497,243,247,293,251,252,253,455,499,501,503,303,272,256,249,263,257,454,259,271,261,262,305,264,265,266,267,269,274,301,
--378,310,187,188,438,379,440,189,191,192,193,194,195,447,196,197,198,476,475,474,200,307,201,309,312,202,203,204,412,205,206,207,210,441,442,211,212,213,214,215,216,167,316,317,217,218,423,219,450,221,222
--,168,471,225,314,522,435,429,431,433,419,411,409,226,416,227,228,229,381,230,231,232,320,235,237,233,234,353,421,236,331,332,333,334,335,336,337,338,339,238,449,453,452,239,354,240,241,358,505,427,170,171,172,459,173,174,351,176,511,178,179,509,180,181,182,185,186
--,525,527,529,533) 
sum(case when (dftt.[TransactionCategoryID] IN (5) AND dftt.TransactionClassID IN (1) AND dftt.TransactionCustomerID IN (3) )
	OR  (dftt.[TransactionCategoryID] IN (18) AND dftt.TransactionClassID IN (2) AND dftt.TransactionCustomerID IN (3) )
	OR  (dftt.[TransactionCategoryID] IN (7,10,19,21,23) AND dftt.TransactionClassID IN (3) AND dftt.TransactionCustomerID IN (3) )
THEN  ISNULL(ft.[TaxAmount],0)
ELSE 0
END	) ProviderTaxCharge,



sum(case when dftt.[TransactionCategoryID] IN (19) AND dftt.TransactionClassID IN (3) AND dftt.TransactionCustomerID IN (3) AND TransactionId NOT IN (235,231,234,308)
--sum(case when ft.FinancialTransactionTypeID IN (471,225,314,522,435,429,431,433,419,411,409,226,416,227,228,229,381,230,231,232,320,235,237,233,234,353,421,236,331,332,333,334,335,336,337,338,339,238,449,453,452,239,354,240,241,358,529) 
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) ProviderTitleCharge,


--------sum(case when ft.FinancialTransactionTypeID IN (
--------164,165,166,-- ProviderCommissionCharge
--------505,427,170,171,172,459,173,174,351,176,511,178,179,509,180,181,182,185,186, -- ProviderTowCharge
--------168, -- ProviderStorageCharge
--------471,225,314,522,435,429,431,433,419,411,409,226,416,227,228,229,381,230,231,232,320,235,237,233,234,353,421,236,331,332,333,334,335,336,337,338,339,238,449,453,452,239,354,240,241,358,529,--ProviderTitleCharge
--------270,242,414,273,244,491,493,495,497,243,247,293,251,252,253,455,499,501,503,303,272,256,249,263,257,454,259,271,261,262,305,264,265,266,267,269,274,301,527 --ProviderEnhancementCharge
----------378,310,187,188,438,379,440,189,191,192,193,194,195,447,196,197,198,476,475,474,200,307,201,309,312,202,203,204,412,205,206,207,210,441,442,211,212,213,214,215,216,167,316,317,217,218,423,219,450,221,222,525 --ProviderOtherCharge
--------) OR dftt.transactioncustomerid = 3 and dftt.transactioncategoryid = 10
--------THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
--------ELSE 0
--------END	) ProviderTotalCharge,
sum(case when (dftt.[TransactionCategoryID] IN (5) AND dftt.TransactionClassID IN (1) AND dftt.TransactionCustomerID IN (3) )
	OR  (dftt.[TransactionCategoryID] IN (18) AND dftt.TransactionClassID IN (2) AND dftt.TransactionCustomerID IN (3) )
	OR  (dftt.[TransactionCategoryID] IN (7,10,19,21) AND dftt.TransactionClassID IN (3) AND dftt.TransactionCustomerID IN (3) AND TransactionId NOT in (308,232,235,231,234,310,281,280))
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) ProviderTotalCharge,


--sum(case when ft.FinancialTransactionTypeID IN (505,427,170,171,172,459,173,174,351,176,511,178,179,509,180,181,182,185,186) 
sum(case when (dftt.[TransactionCategoryID] IN (7) AND dftt.TransactionClassID IN (3) AND dftt.TransactionCustomerID IN (3) )
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) ProviderTowCharge,


--sum(case when ft.FinancialTransactionTypeID IN (127,99,413,130,101,490,492,494,496,100,104,150,108,109,110,498,
--500,502,302,129,113,106,120,114,116,128,118,119,304,121,122,123,124,126,131,300,526) 
sum(case when (dftt.[TransactionCategoryID] IN (21) AND dftt.TransactionClassID IN (3) AND dftt.TransactionCustomerID IN (2) AND TransactionId NOT IN (310,281,280))
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) ProviderEnhanceExpense,


--sum(case when ft.FinancialTransactionTypeID IN (424,425,52,53,437,436,439,54,56,57,58,59,60,62,306,63,
--308,311,64,65,66,67,69,72,73,74,75,76,77,78,79,80,81,451,83,84,340,534) 

sum(case when (dftt.[TransactionCategoryID] IN (10) AND dftt.TransactionClassID IN (3) AND dftt.TransactionCustomerID IN (2) AND TransactionId NOT in (232))
	OR  (dftt.[TransactionCategoryID] IN (11) AND dftt.TransactionClassID IN (3) AND dftt.TransactionCustomerID IN (2) )
	OR  (dftt.[TransactionCategoryID] IN (24) AND dftt.TransactionClassID IN (1,3) AND dftt.TransactionCustomerID IN (2) )	
THEN  ISNULL(ft.[Amount],0)
ELSE 0
END	) ProviderOtherExpense,



--sum(case when ft.FinancialTransactionTypeID IN (472,85,313,521,434,428,430,432,418,410,408,86,415,87,88,89,
--380,90,91,92,319,94,359,93,352,420,95,96,97,417,98,357,530) 
sum(case when (dftt.[TransactionCategoryID] IN (19) AND dftt.TransactionClassID IN (3) AND dftt.TransactionCustomerID IN (2) AND TransactionId NOT IN (235,231,234,308))
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) ProviderTitleExpense,

--sum(case when ft.FinancialTransactionTypeID IN (504,426,35,36,37,460,38,39,350,41,510,43,44,508,45,46,47,50,51,
--472,85,313,521,434,428,430,432,418,410,408,86,415,87,88,89,
--380,90,91,92,319,94,359,93,352,420,95,96,97,417,98,357,127,99,413,130,101,490,492,494,496,100,104,150,108,109,110,498,
--500,502,302,129,113,106,120,114,116,128,118,119,304,121,122,123,124,126,131,300,472,85,313,521,434,428,430,432,418,410,408,86,415,87,88,89,
--380,90,91,92,319,94,359,93,352,420,95,96,97,417,98,357,424,425,52,53,437,436,439,54,56,57,58,59,60,62,306,63,
--308,311,64,65,66,67,69,72,73,74,75,76,77,78,79,80,81,451,83,84,340,526,530,534) 
sum(case when (dftt.[TransactionCategoryID] IN (7,10,19,21,24) AND dftt.TransactionClassID IN (3) AND dftt.TransactionCustomerID IN (2) AND TransactionId NOT in (308,232,235,231,234,310,281,280) )
	OR  (dftt.[TransactionCategoryID] IN (24) AND dftt.TransactionClassID IN (1) AND dftt.TransactionCustomerID IN (2) )
	THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) ProviderTotalExpense,


--sum(case when ft.FinancialTransactionTypeID IN (504,426,35,36,37,460,38,39,350,41,510,43,44,508,45,46,47,50,51)
sum(case when (dftt.[TransactionCategoryID] IN (7) AND dftt.TransactionClassID IN (3) AND dftt.TransactionCustomerID IN (2)) 
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) ProviderTowExpense,

--sum(case when ft.FinancialTransactionTypeID IN (504,426,35,36,37,460,38,39,350,41,510,43,44,508,45,46,47,50,51,
--472,85,313,521,434,428,430,432,418,410,408,86,415,87,88,89,
--380,90,91,92,319,94,359,93,352,420,95,96,97,417,98,357,127,99,413,130,101,490,492,494,496,100,104,150,108,109,110,498,
--500,502,302,129,113,106,120,114,116,128,118,119,304,121,122,123,124,126,131,300,472,85,313,521,434,428,430,432,418,410,408,86,415,87,88,89,
--380,90,91,92,319,94,359,93,352,420,95,96,97,417,98,357,424,425,52,53,437,436,439,54,56,57,58,59,60,62,306,63,
--308,311,64,65,66,67,69,72,73,74,75,76,77,78,79,80,81,451,83,84,340,526,530,534) 


sum(case when (dftt.[TransactionCategoryID] IN (7,10,19,21,24) AND dftt.TransactionClassID IN (3) AND dftt.TransactionCustomerID IN (2) )
	OR  (dftt.[TransactionCategoryID] IN (24) AND dftt.TransactionClassID IN (1) AND dftt.TransactionCustomerID IN (2) )
THEN  ISNULL(ft.[TaxAmount],0)
ELSE 0
END	) ProviderTaxExpense,


--SUM(CASE WHEN ft.FinancialTransactionTypeID IN (460) THEN ISNULL(ft.Amount,0) ELSE 0 END) FuelSurcharge
sum(case when (dftt.[TransactionCategoryID] IN (7) AND dftt.TransactionClassID IN (3) AND dftt.TransactionCustomerID IN (2) ) 
and dftt.TransactionDescription = 'Fuel Surcharge'
THEN ISNULL(ft.Amount,0) ELSE 0 END) FuelSurcharge,

SUM(CASE WHEN (dftt.[TransactionId] IN (308,232,235,231,234,310,281,280) AND dftt.TransactionCustomerID IN (3) )
         THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
     ELSE 0
     END ) ProviderAncillaryCharge

FROM #TMP_StockID
INNER JOIN DimStock DS WITH (NOLOCK)
ON #TMP_StockID.StockID = DS.StockID
LEFT OUTER JOIN dbo.FactFinancialTransaction ft	WITH (NOLOCK)	
ON DS.StockID = ft.StockID
AND ft.IsVoid = 0
LEFT OUTER JOIN dbo.DimFinancialTransactionType dftt WITH (NOLOCK)
ON FT.FinancialTransactionTypeID = dftt.FinancialTransactionTypeID
GROUP BY  #TMP_StockID.stockid	




INSERT INTO #TMP_BuyerStockChargeExpense
(	StockID,
	BuyerCharge,
	BuyerPulloutCharge,
	BuyerDMVCharge,
	BuyerLateCharge,
	BuyerLoadingCharge,
	BuyerOtherCharge,
	BuyerStorageCharge,
	BuyerTotalCharge,
	BuyerTaxCharge,
	BuyerTowCharge,
	BuyerDMVExpense,
	BuyerOtherExpense,
	BuyerTotalExpense,
	BuyerTaxExpense,
	BuyerTowExpense,
    BuyerInternetCharge  
)
		
SELECT	#TMP_StockID.StockID,
--sum(case when ft.FinancialTransactionTypeID IN (2,4,6)
sum(case when (dftt.[TransactionCategoryID] IN (3) AND dftt.TransactionClassID IN (5) AND dftt.TransactionCustomerID IN (1) )  
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) BuyerCharge,
		 --sum(case when ft.FinancialTransactionTypeID IN (22) 
sum(case when (dftt.[TransactionCategoryID] IN (13) AND dftt.TransactionClassID IN (5) AND dftt.TransactionCustomerID IN (1) )  
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) BuyerPulloutCharge,
		 --sum(case when ft.FinancialTransactionTypeID IN (400,398,456,443,8,392,458,396,394,
		 --360,402,373,330,326,322,406,328,445,362,324,404,9) 
sum(case when (dftt.[TransactionCategoryID] IN (4) AND dftt.TransactionClassID IN (5) AND dftt.TransactionCustomerID IN (1) )  
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) BuyerDMVCharge,
		 --sum(case when ft.FinancialTransactionTypeID IN (20,293) --This was wrong.  Should have been 393 not 293
sum(case when dftt.[TransactionCategoryID] IN (12) AND dftt.TransactionClassID IN (5) AND dftt.TransactionCustomerID IN (1)		  
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) BuyerLateCharge,
		 --sum(case when ft.FinancialTransactionTypeID IN (12) 
sum(case when dftt.[TransactionCategoryID] IN (9) AND dftt.TransactionClassID IN (5) AND dftt.TransactionCustomerID IN (1)		
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) BuyerLoadingCharge,
		 sum(case when dftt.transactioncustomerid = 1 and dftt.transactioncategoryid =11
		 and (dftt.TransactionID Not IN (167,228,249))
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) BuyerOtherCharge,
		 --sum(case when ft.FinancialTransactionTypeID IN (26,391)
sum(case when dftt.[TransactionCategoryID] IN (18) AND dftt.TransactionClassID IN (5) AND dftt.TransactionCustomerID IN (1)			  
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0) 
ELSE 0
END	) BuyerStorageCharge,
		--sum(case when ft.FinancialTransactionTypeID IN (400,398,456,443,8,392,458,396,394,
		-- 360,402,373,330,326,322,406,328,445,362,324,404,9,2,4,6,22,20,293,12,26,391,506,463,10,390,464,366)
sum(case when dftt.[TransactionCategoryID] IN (3,4,8,9,12,13,18) AND dftt.TransactionClassID IN (5) AND dftt.TransactionCustomerID IN (1)			  
	 OR (dftt.transactioncustomerid = 1 and dftt.transactioncategoryid = 11)	
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0) 
ELSE 0
END	) BuyerTotalCharge,
		 --sum(case when ft.FinancialTransactionTypeID IN (400,398,456,443,8,392,458,396,394,360,402,373,330
		 --,326,322,406,328,445,362,324,404,9,2,4,6,22,20,293,12,343,448,469,470,14,512,473,318,345,349,347,446,17,364,19,26,391,506,463,10,390,464,366) 
sum(case when dftt.[TransactionCategoryID] IN (3,4,8,9,12,13,18) AND dftt.TransactionClassID IN (5) AND dftt.TransactionCustomerID IN (1)			  
	 OR (dftt.transactioncustomerid = 1 and dftt.transactioncategoryid = 11)      
THEN  ISNULL(ft.[TaxAmount],0)
ELSE 0
END	) BuyerTaxCharge,
		 --sum(case when ft.FinancialTransactionTypeID IN (506,463,10,390,464,366) 
sum(case when dftt.[TransactionCategoryID] IN (8) AND dftt.TransactionClassID IN (5) AND dftt.TransactionCustomerID IN (1)		
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) BuyerTowCharge,
		 --sum(case when ft.FinancialTransactionTypeID IN (401,399,457,444,157,375,397,395,361,403,374,329,325,321,407,327,363,323,405) 
sum(case when dftt.[TransactionCategoryID] IN (4) AND dftt.TransactionClassID IN (5) AND dftt.TransactionCustomerID IN (2)		
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) BuyerDMVExpense,
		 --sum(case when ft.FinancialTransactionTypeID IN (342,160,344,348,346,365) 
sum(case when dftt.[TransactionCategoryID] IN (11) AND dftt.TransactionClassID IN (5) AND dftt.TransactionCustomerID IN (2)		
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0) 
ELSE 0
END	) BuyerOtherExpense,
		 --sum(case when ft.FinancialTransactionTypeID IN (401,399,457,444,157,375,397,395,361,403,374,329,325,321,407,327,363,323,405,
		 --507,461,158,462,367,342,160,344,348,346,365,507,461,158,462,367) 
sum(case when dftt.TransactionClassID IN (5) AND dftt.TransactionCustomerID IN (2)		
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0) 
ELSE 0
END	) BuyerTotalExpense,
 --sum(case when ft.FinancialTransactionTypeID IN (401,399,457,444,157,375,397,395,361,403,374,329,325,321,407,327,363,323,405,
	--	 507,461,158,462,367,342,160,344,348,346,365,507,461,158,462,367) 
sum(case when dftt.TransactionClassID IN (5) AND dftt.TransactionCustomerID IN (2)	
THEN  ISNULL(ft.[TaxAmount],0) 
ELSE 0
END	) BuyerTaxExpense,
		 --sum(case when ft.FinancialTransactionTypeID IN (507,461,158,462,367) 
sum(case when dftt.[TransactionCategoryID] IN (8) AND dftt.TransactionClassID IN (5) AND dftt.TransactionCustomerID IN (2)		
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0) 
ELSE 0
END	) BuyerTowExpense,

 sum(case when dftt.transactioncustomerid = 1 and dftt.transactioncategoryid =11
		 and (dftt.TransactionID IN (167,228,249))
THEN  ISNULL(COALESCE(ft.[OverrideAmount], ft.[Amount]),0)
ELSE 0
END	) BuyerInternetCharge

FROM #TMP_StockID
INNER JOIN DimStock DS WITH (NOLOCK)
ON #TMP_StockID.StockID = DS.StockID
LEFT OUTER JOIN dbo.FactFinancialTransaction ft	WITH (NOLOCK)	
ON DS.StockID = ft.StockID
AND DS.CurrentBuyerID = ft.BuyerID
AND ft.IsVoid = 0
LEFT OUTER JOIN DimFinancialTransactionType dftt with (nolock)
on (dftt.FinancialTransactionTypeID = ft.FinancialTransactionTypeID)
GROUP BY  #TMP_StockID.stockid	
					  

INSERT INTO #TMP_FactStockChargeExpense
(	StockID,
	AdvanceTowCharge,
	AdvanceStorageCharge,
	AdvanceOtherCharge, 
	AdvanceTotalCharge,
	AdvanceTaxCharge,
	AdvanceTowExpense,
	AdvanceStorageExpense,
	AdvanceOtherExpense,
	AdvanceTotalExpense,
	AdvanceTaxExpense,
	ProviderCommissionCharge,
	ProviderEnhancementCharge,
	ProviderOtherCharge,
	ProviderStorageCharge,
	ProviderTaxCharge,
	ProviderTitleCharge,
	ProviderTotalCharge,
	ProviderTowCharge,
	ProviderEnhanceExpense,
	ProviderOtherExpense,
	ProviderTitleExpense,
	ProviderTotalExpense,
	ProviderTowExpense,
	ProviderTaxExpense,
	BuyerCharge,
	BuyerPulloutCharge,
	BuyerDMVCharge,
	BuyerLateCharge,
	BuyerLoadingCharge,
	BuyerOtherCharge,
	BuyerStorageCharge,
	BuyerTotalCharge,
	BuyerTaxCharge,
	BuyerTowCharge,
	BuyerDMVExpense,
	BuyerOtherExpense,
	BuyerTotalExpense,
	BuyerTaxExpense,
	BuyerTowExpense,
	FuelSurcharge,
	BuyerInternetCharge,
    ProviderAncillaryCharge
)
		
SELECT	PSCE.StockID,
	PSCE.AdvanceTowCharge,
	PSCE.AdvanceStorageCharge,
	PSCE.AdvanceOtherCharge, 
	PSCE.AdvanceTotalCharge,
	PSCE.AdvanceTaxCharge,
	PSCE.AdvanceTowExpense,
	PSCE.AdvanceStorageExpense,
	PSCE.AdvanceOtherExpense,
	PSCE.AdvanceTotalExpense,
	PSCE.AdvanceTaxExpense,
	PSCE.ProviderCommissionCharge,
	PSCE.ProviderEnhancementCharge,
	PSCE.ProviderOtherCharge,
	PSCE.ProviderStorageCharge,
	PSCE.ProviderTaxCharge,
	PSCE.ProviderTitleCharge,
	PSCE.ProviderTotalCharge,
	PSCE.ProviderTowCharge,
	PSCE.ProviderEnhanceExpense,
	PSCE.ProviderOtherExpense,
	PSCE.ProviderTitleExpense,
	PSCE.ProviderTotalExpense,
	PSCE.ProviderTowExpense,
	PSCE.ProviderTaxExpense,
	BSCE.BuyerCharge,
	BSCE.BuyerPulloutCharge,
	BSCE.BuyerDMVCharge,
	BSCE.BuyerLateCharge,
	BSCE.BuyerLoadingCharge,
	BSCE.BuyerOtherCharge,
	BSCE.BuyerStorageCharge,
	BSCE.BuyerTotalCharge,
	BSCE.BuyerTaxCharge,
	BSCE.BuyerTowCharge,
	BSCE.BuyerDMVExpense,
	BSCE.BuyerOtherExpense,
	BSCE.BuyerTotalExpense,
	BSCE.BuyerTaxExpense,
	BSCE.BuyerTowExpense,
	PSCE.FuelSurcharge,
	BSCE.BuyerInternetCharge,
    PSCE.ProviderAncillaryCharge
FROM #TMP_ProviderStockChargeExpense PSCE WITH (NOLOCK)
INNER JOIN #TMP_BuyerStockChargeExpense BSCE WITH (NOLOCK)
ON PSCE.StockID = BSCE.StockID 

/*************Fix for stock stocknumber = 17843541, Amount = 17843541.00*****************/
UPDATE #TMP_FactStockChargeExpense SET
ProviderEnhancementCharge = 0,
ProviderTotalCharge = 99.00 where stockID = 18941989

/*********************************************************************************************
MERGE the data from the temp table into the EDW FactStockChargeExpense table
This does Update and Insert only.  No Deletes
*********************************************************************************************/

BEGIN TRANSACTION 
  
    MERGE [dbo].FactStockChargeExpense AS FSTCE
    USING #TMP_FactStockChargeExpense AS Tmp_FSTCE  
          ON Tmp_FSTCE.StockID = FSTCE.StockID
    
    WHEN MATCHED THEN 
    UPDATE SET 
        FSTCE.AdvanceTowCharge = Tmp_FSTCE.AdvanceTowCharge,
		FSTCE.AdvanceStorageCharge = Tmp_FSTCE.AdvanceStorageCharge,
		FSTCE.AdvanceOtherCharge = Tmp_FSTCE.AdvanceOtherCharge, 
		FSTCE.AdvanceTotalCharge = Tmp_FSTCE.AdvanceTotalCharge,
		FSTCE.AdvanceTaxCharge = Tmp_FSTCE.AdvanceTaxCharge,
		FSTCE.AdvanceTowExpense = Tmp_FSTCE.AdvanceTowExpense,
		FSTCE.AdvanceStorageExpense = Tmp_FSTCE.AdvanceStorageExpense,
		FSTCE.AdvanceOtherExpense = Tmp_FSTCE.AdvanceOtherExpense,
		FSTCE.AdvanceTotalExpense = Tmp_FSTCE.AdvanceTotalExpense,
		FSTCE.AdvanceTaxExpense = Tmp_FSTCE.AdvanceTaxExpense,
		FSTCE.ProviderCommissionCharge = Tmp_FSTCE.ProviderCommissionCharge,
		FSTCE.ProviderEnhancementCharge = Tmp_FSTCE.ProviderEnhancementCharge,
		FSTCE.ProviderOtherCharge = Tmp_FSTCE.ProviderOtherCharge,
		FSTCE.ProviderStorageCharge = Tmp_FSTCE.ProviderStorageCharge,
		FSTCE.ProviderTaxCharge = Tmp_FSTCE.ProviderTaxCharge,
		FSTCE.ProviderTitleCharge = Tmp_FSTCE.ProviderTitleCharge,
		FSTCE.ProviderTotalCharge = Tmp_FSTCE.ProviderTotalCharge,
		FSTCE.ProviderTowCharge = Tmp_FSTCE.ProviderTowCharge,
		FSTCE.ProviderEnhanceExpense = Tmp_FSTCE.ProviderEnhanceExpense,
		FSTCE.ProviderOtherExpense = Tmp_FSTCE.ProviderOtherExpense,
		FSTCE.ProviderTitleExpense = Tmp_FSTCE.ProviderTitleExpense,
		FSTCE.ProviderTotalExpense = Tmp_FSTCE.ProviderTotalExpense,
		FSTCE.ProviderTowExpense = Tmp_FSTCE.ProviderTowExpense,
		FSTCE.ProviderTaxExpense = Tmp_FSTCE.ProviderTaxExpense,
		FSTCE.BuyerCharge = Tmp_FSTCE.BuyerCharge,
		FSTCE.BuyerPulloutCharge = Tmp_FSTCE.BuyerPulloutCharge,
		FSTCE.BuyerDMVCharge = Tmp_FSTCE.BuyerDMVCharge,
		FSTCE.BuyerLateCharge = Tmp_FSTCE.BuyerLateCharge,
		FSTCE.BuyerLoadingCharge = Tmp_FSTCE.BuyerLoadingCharge,
		FSTCE.BuyerOtherCharge = Tmp_FSTCE.BuyerOtherCharge,
		FSTCE.BuyerStorageCharge = Tmp_FSTCE.BuyerStorageCharge,
		FSTCE.BuyerTotalCharge = Tmp_FSTCE.BuyerTotalCharge,
		FSTCE.BuyerTaxCharge = Tmp_FSTCE.BuyerTaxCharge,
		FSTCE.BuyerTowCharge = Tmp_FSTCE.BuyerTowCharge,
		FSTCE.BuyerDMVExpense = Tmp_FSTCE.BuyerDMVExpense,
		FSTCE.BuyerOtherExpense = Tmp_FSTCE.BuyerOtherExpense,
		FSTCE.BuyerTotalExpense = Tmp_FSTCE.BuyerTotalExpense,
		FSTCE.BuyerTaxExpense = Tmp_FSTCE.BuyerTaxExpense,
		FSTCE.BuyerTowExpense = Tmp_FSTCE.BuyerTowExpense,
		FSTCE.FuelSurcharge = Tmp_FSTCE.FuelSurcharge,
		FSTCE.BuyerInternetCharge= Tmp_FSTCE.BuyerInternetCharge,
        FSTCE.ProviderAncillaryCharge = Tmp_FSTCE.ProviderAncillaryCharge,
		FSTCE.ETLLoadUpdateID = @ETLLoadID	
   WHEN NOT MATCHED THEN
   INSERT(  StockID,
			AdvanceTowCharge,
			AdvanceStorageCharge,
			AdvanceOtherCharge,
			AdvanceTotalCharge,
			AdvanceTaxCharge,
			AdvanceTowExpense,
			AdvanceStorageExpense,
			AdvanceOtherExpense,
			AdvanceTotalExpense,
			AdvanceTaxExpense,
			ProviderCommissionCharge,
			ProviderEnhancementCharge,
			ProviderOtherCharge,
			ProviderStorageCharge,
			ProviderTaxCharge,
			ProviderTitleCharge,
			ProviderTotalCharge,
			ProviderTowCharge,
			ProviderEnhanceExpense,
			ProviderOtherExpense,
			ProviderTitleExpense,
			ProviderTotalExpense,
			ProviderTowExpense,
			ProviderTaxExpense,
			BuyerCharge,
			BuyerPulloutCharge,
			BuyerDMVCharge,
			BuyerLateCharge,
			BuyerLoadingCharge,
			BuyerOtherCharge,
			BuyerStorageCharge,
			BuyerTotalCharge,
			BuyerTaxCharge,
			BuyerTowCharge,
			BuyerDMVExpense,
			BuyerOtherExpense,
			BuyerTotalExpense,
			BuyerTaxExpense,
			BuyerTowExpense,
			FuelSurcharge,
			BuyerInternetCharge,
            ProviderAncillaryCharge,
			ETLLoadID,
			ETLLoadUpdateID
		)	 
	VALUES (StockID,
			AdvanceTowCharge,
			AdvanceStorageCharge,
			AdvanceOtherCharge,
			AdvanceTotalCharge,
			AdvanceTaxCharge,
			AdvanceTowExpense,
			AdvanceStorageExpense,
			AdvanceOtherExpense,
			AdvanceTotalExpense,
			AdvanceTaxExpense,
			ProviderCommissionCharge,
			ProviderEnhancementCharge,
			ProviderOtherCharge,
			ProviderStorageCharge,
			ProviderTaxCharge,
			ProviderTitleCharge,
			ProviderTotalCharge,
			ProviderTowCharge,
			ProviderEnhanceExpense,
			ProviderOtherExpense,
			ProviderTitleExpense,
			ProviderTotalExpense,
			ProviderTowExpense,
			ProviderTaxExpense,
			BuyerCharge,
			BuyerPulloutCharge,
			BuyerDMVCharge,
			BuyerLateCharge,
			BuyerLoadingCharge,
			BuyerOtherCharge,
			BuyerStorageCharge,
			BuyerTotalCharge,
			BuyerTaxCharge,
			BuyerTowCharge,
			BuyerDMVExpense,
			BuyerOtherExpense,
			BuyerTotalExpense,
			BuyerTaxExpense,
			BuyerTowExpense,	
			FuelSurcharge,
			BuyerInternetCharge,
            ProviderAncillaryCharge,
			@ETLLoadID,
			@ETLLoadID
			)
	     
	OUTPUT $action INTO #temp_FactStockChargeExpenseRecordCount;
	  
  /*********************************************************************************************
Get the counts from the MERGE operation
*********************************************************************************************/
SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #temp_FactStockChargeExpenseRecordCount

/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		COMMIT TRAN
		IF OBJECT_ID('tempdb..#TMP_StockID') IS NOT NULL DROP TABLE #TMP_StockID
		IF OBJECT_ID('tempdb..#TMP_FactStockChargeExpense') IS NOT NULL DROP TABLE #TMP_FactStockChargeExpense
		IF OBJECT_ID('tempdb..#TMP_ProviderStockChargeExpense') IS NOT NULL DROP TABLE #TMP_ProviderStockChargeExpense
		IF OBJECT_ID('tempdb..#TMP_BuyerStockChargeExpense') IS NOT NULL DROP TABLE #TMP_BuyerStockChargeExpense
		
		RETURN 0
	END TRY     

	BEGIN CATCH
		SET NOCOUNT OFF
		IF @@TRANCOUNT > 0 
			ROLLBACK TRAN
		
		IF OBJECT_ID('tempdb..#TMP_StockID') IS NOT NULL DROP TABLE #TMP_StockID
		IF OBJECT_ID('tempdb..#TMP_FactStockChargeExpense') IS NOT NULL DROP TABLE #TMP_FactStockChargeExpense	
		IF OBJECT_ID('tempdb..#TMP_ProviderStockChargeExpense') IS NOT NULL DROP TABLE #TMP_ProviderStockChargeExpense
		IF OBJECT_ID('tempdb..#TMP_BuyerStockChargeExpense') IS NOT NULL DROP TABLE #TMP_BuyerStockChargeExpense

		EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ErrorCode = @@ERROR, @UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

		RETURN -1
	END CATCH
	END



GO


