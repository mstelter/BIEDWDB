/* 
==============================================================================
Author		: Matt Stelter
Create date	: 5/8/2012
Description	: Populate Temp tables to be used for the long running dimensions	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number
==============================================================================
Usage Example:
Exec usp_ETLMaterializeViewsForSSAS 
--SELECT * FROM TMP_DimStock
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLMaterializeViewsForSSAS]
AS
  BEGIN
      DECLARE @ETLLoadID          INT,
              @ExtractRowCount_sp INT,
              @InsertRowCount     INT,
              @UpdateRowCount     INT,
              @DeleteRowCount     INT,
              @StartLSN           BINARY(10),
              @EndLSN             BINARY(10),
              @MinAllowableDate   DATETIME,
              @MaxAllowableDate   DATETIME

      SET NOCOUNT ON;

      BEGIN TRY
          BEGIN TRANSACTION

          EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'MaterializeViews'

      /*********************************************************************************************
      Insert the data for DimStock and DimStockCycletime
      *********************************************************************************************/
          --IF OBJECT_ID('edw.dbo.TMP_DimStock') IS NOT NULL DROP TABLE edw.dbo.TMP_DimStock
          --CREATE NONCLUSTERED INDEX IX_v_DimStock ON EDW.dbo.TMP_DimStock (StockID) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
          TRUNCATE TABLE dbo.TMP_DimStock

          INSERT INTO dbo.TMP_DimStock
                      (StockID,IsACVOutlier,AdjustorName,AgreementTermTypeDescription,ASAPSalvageID,AssignmentTypeDescription,AssignorName,
                       AuctionTypeDescription,ISBuyerTow,
                       IsChargeTitleProcessFee,IsCancel,CountryOfManufacture,CarStartsAtCheckin,ClaimHandler,ClaimNumber,ColorDescription,
                       ISDeliveredtoBranch,
                       ISDryRun,IsExpressPickupTow,IsFlippedtoPA,FullBodyStyle,GarageName,GarageAddress,GarageCityName,GarageCountyName,
                       GarageStateAbbreviation,
                       GarageZipCode,InsuredName,LossTypeDescription,MakeName,Mileage,MinimumBidSource,ModelName,ModelYear,MotorCycleSegment,
                       OdometerReadingTypeDescription,OwnerName,PolicyNumber,DamageDescriptionPrimary,ISReturnTow,SaleDocBrand,SaleDocNumber,
                       DamageDescriptionSecondary,SeriesName,SpecialtyCategoryDescription,FullStockNumber,SaleDocumentTypeDescription,StockSegment,
                       StockTypeDescription,TitleType,ISVIC,VIN,KeysRetainedCodeatCheckIn,YearMakeModel,ActualCashValue,BlackBookACV,ModelYearMeasure,
                       ISOffSiteSale,SellFromLocationName,SellFromLocationCity,SellFromLocationState,SellFromLocationZip,ISReleaseProblem,
                       MinimumBidCallerName,
                       IsPriorityTow,StockHoldReasonDescription,IsStockProviderProcessTitle,StockSubTypeDescription,TitleProcessingBranchNumber,
                       TitleReceivedBrand,IsTowable,IsAgreementChanged,IsReportedToNICB,IsTitleServiceUsed,KeysRetainedCodeAtSale,
                       ProcessStatusDescription,
                       ISStockSold,CurrentStockStatusDescription,BulkSaleInd,IBuyFastSoldInd,HadReleaseProblemInd,IsHadHold,IsOnHold,
                       IsBuyerRenegedAfterSettlement,IsBuyerReneged,BuyerRenegeReason,PreviousAgreementTermTypeDescription,TitleProblemInd,
                       HadTitleProblemInd,
                       AssignmentMethod,IsManualAssignment,IsElectronicAssignment,IsCreditSale,AdvanceRateType,AdvanceStorageDailyRate,
                       IAAAdvanceRateType,
                       OriginalBranch,OriginalProviderGroup,OriginalProviderOffice,BranchTransferInd,ProviderGroupTransferInd,
                       ProviderOfficeTransferInd,
                       ReRuns,
                       ProviderReRuns,ProviderReRunsNoBid,MinimumBidOutlierInd,BuyerFinanceCompanyCode,AdvanceStorageEndDate,
                       AdvanceStorageEndDate_Filter,
                       AdvanceStorageStartDate,AdvanceStorageStartDate_Filter,AdvanceStorageVerifiedDate,AdvanceStorageVerifiedDate_Filter,
                       AnticipatedSaleDate,
                       AnticipatedSaleDate_Filter,AssignmentDateTime,AssignmentDateTime_Filter,AuctionDateTime,AuctionDateTime_Filter,
                       OriginalAuctionDateTime,
                       OriginalAuctionDateTime_Filter,BuyerPaymentDateTime,BuyerPaymentDateTime_Filter,BuyerPickupDateTime,BuyerPickupDateTime_Filter,
                       BuyerRenegeCancelSaleDate,BuyerRenegeCancelSaleDate_Filter,CancelDateTime,CancelDateTime_Filter,CertINDateTime,
                       CertINDateTime_Filter,
                       CheckInDateTime,CheckInDateTime_Filter,CheckInImagesTakenDate,CheckInImagesTakenDate_Filter,CheckIssuedDate,
                       CheckIssuedDate_Filter,
                       CurrentProcessStatusStartDate,CurrentProcessStatusStartDate_Filter,CurrentStockStatusStartDate,
                       CurrentStockStatusStartDate_Filter,
                       DateOfLossReportedDateTime,DateOfLossReportedDateTime_Filter,DispatchDateTime,DispatchDateTime_Filter,
                       DispatchPermanentlyCancelledDateTime,DispatchPermanentlyCancelledDateTime_Filter,EarliestSaleDate,EarliestSaleDate_Filter,
                       FirstSoldDateTime,FirstSoldDateTime_Filter,HoldReleaseDateTime,HoldReleaseDateTime_Filter,IBuyFastDisplayEndDate,
                       IBuyFastDisplayEndDate_Filter,IBuyFastDisplayStartDate,IBuyFastDisplayStartDate_Filter,InspectionDateTime,
                       InspectionDateTime_Filter,
                       RefundDateTime,RefundDateTime_Filter,LogInDateTime,LogInDateTime_Filter,LogOutDateTime,LogOutDateTime_Filter,LossDateTime,
                       LossDateTime_Filter,PickupDateTime,PickupDateTime_Filter,StockDropOffDateTime,StockDropOffDateTime_Filter,PlacedOnHoldDate,
                       PlacedOnHoldDate_Filter,PreBillPaidDateTime,PreBillPaidDateTime_Filter,ProviderQuotedReleaseDateTime,
                       ProviderQuotedReleaseDateTime_Filter
                       ,ReceivedAllDocumentsDateTime,ReceivedAllDocumentsDateTime_Filter,RecoveryDatetime,RecoveryDatetime_Filter,RefundPaymentDate,
                       RefundPaymentDate_Filter,ReleaseDateTime,ReleaseDateTime_Filter,ReleaseProblemIdentifiedDate,
                       ReleaseProblemIdentifiedDate_Filter,
                       ReleaseProblemResolvedDate,ReleaseProblemResolvedDate_Filter,SaleDocumentReceivedDateTime,SaleDocumentReceivedDateTime_Filter,
                       SalesDocumentAppliedDatetime,SalesDocumentAppliedDatetime_Filter,SettlementDateTime,SettlementDateTime_Filter,
                       SettlementPaidDateTime,
                       SettlementPaidDateTime_Filter,SMVDDatetime,SMVDDatetime_Filter,SoldDateTime,SoldDateTime_Filter,TitleApprovedByProviderDateTime,
                       TitleApprovedByProviderDateTime_Filter,TitleInDateTime,TitleInDateTime_Filter,TitleProblemIdentifiedDate,
                       TitleProblemIdentifiedDate_Filter,TitleProblemResolvedDate,TitleProblemResolvedDate_Filter,VehicleImagesLoadedDate,
                       VehicleImagesLoadedDate_Filter,VICInspectionDateTime,VICInspectionDateTime_Filter,OriginalBranchChangeDatetime,
                       OriginalBranchChangeDate_Filter,OriginalProviderTransferDateTime,OriginalProviderTransferDate_Filter)
          SELECT DS.StockID,
                 DS.IsACVOutlier,--5.2 S10
                 DS.AdjustorFirstName+' '+DS.AdjustorLastName AS AdjustorName,
                 DS.AgreementTermTypeDescription,
                 DS.ASAPSalvageID,
                 DS.AssignmentTypeDescription,
                 DS.AssignorFirstName+' '+DS.AssignorLastName AS AssignorName,--NEW
                 DS.AuctionTypeDescription,
                 DS.ISBuyerTow,
                 DS.IsChargeTitleProcessFee,
                 DS.IsCancel,
                 DS.CountryOfManufacture,
                 CASE
                   WHEN DS.EngineStatusCode IN ('SWJ', 'CST') THEN 1
                   WHEN DS.EngineStatusCode IN ('WST', 'EDG') THEN 0
                   ELSE 2
                 END AS CarStartsAtCheckin,
                 DS.ClaimHandler,
                 DS.ClaimNumber,
                 DS.ColorDescription,
                 DS.ISDeliveredtoBranch,
                 DS.ISDryRun,
                 CASE
                   WHEN DS.ISExpressPickupTow=1 THEN 1
                   ELSE 0
                 END AS IsExpressPickupTow,
                 DS.IsFlippedtoPA,
                 DS.FullBodyStyle,
                 Isnull(DSL_PU.LocationName, '') AS GarageName,
                 Isnull(DSL_PU.AddressLine1, '') AS GarageAddress,
                 Isnull(DSL_PU.CityName, '') AS GarageCityName,
                 Isnull(DSL_PU.CountyName, '') AS GarageCountyName,
                 Isnull(DSL_PU.StateAbbreviation, '') AS GarageStateAbbreviation,
                 Isnull(DSL_PU.ZipCode, '') AS GarageZipCode,
                 Isnull(DS.InsuredFirstName, '')+' '+Isnull(DS.InsuredLastName, '') AS InsuredName,
                 DS.LossTypeDescription,
                 DS.MakeName,
                 DS.Mileage,
                 DS.MinimumBidSource,
                 DS.ModelName,
                 DS.ModelYear,
                 DS.MotorCycleSegment,
                 DS.OdometerReadingTypeDescription,
                 Isnull(DS.OwnerFirstName, '')+' '+Isnull(DS.OwnerLastName, '') AS OwnerName,
                 DS.PolicyNumber,
                 DS.DamageDescriptionPrimary,
                 DS.ISReturnTow,
                 DS.SaleDocBrand,
                 DS.SaleDocNumber,
                 DS.DamageDescriptionSecondary,
                 DS.SeriesName,
                 DS.SpecialtyCategoryDescription,
                 DS.FullStockNumber,
                 DS.SaleDocumentTypeDescription,
                 DS.StockSegment,
                 DS.StockTypeDescription,
                 DS.TitleType,
                 DS.ISVIC,
                 DS.VIN,
                 DS.KeysRetainedCodeatCheckIn,
                 Isnull(Cast(DS.ModelYear AS VARCHAR), 'NONE')+' '+Isnull(Rtrim(DS.MakeName), '')+' '+Isnull(Rtrim(DS.ModelName), '') AS YearMakeModel
                 ,
                 Isnull(DS.ActualCashValue, 0) AS ActualCashValue,--TimeLine-P2S7
                 Isnull(DS.BlackbookACV, 0) AS BlackBookACV,--TimeLine-P2S7
                 CASE
                   WHEN (DS.ModelYear)<1960
                         OR (DS.ModelYear) IS NULL THEN 1960
                   WHEN (DS.ModelYear)>Year(Getdate())+1 THEN Year(Getdate())
                   WHEN (DS.ModelYear)>3000 THEN 0
                   ELSE Isnull((DS.ModelYear), 0)
                 END AS ModelYearMeasure,--TimeLine-P2S7
                 DS.ISOffSiteSale,
                 Isnull(DSL_SF.LocationName, '') AS SellFromLocationName,
                 Isnull(DSL_SF.CityName, '') AS SellFromLocationCity,
                 Isnull(DSL_SF.StateAbbreviation, '') AS SellFromLocationState,
                 Isnull(DSL_SF.ZipCode, '') AS SellFromLocationZip,
                 IsReleaseProblem AS ISReleaseProblem,
                 DS.MinimumBidCallerName,
                 DS.IsPriorityTow,
                 DS.StockHoldReasonDescription,
                 DS.IsStockProviderProcessTitle,
                 DS.StockSubTypeDescription,
                 DS.TitleProcessingBranchNumber,
                 DS.TitleReceivedBrand,
                 DS.IsTowable,
                 DS.IsAgreementChanged,
                 DS.IsReportedToNICB,
                 DS.IsTitleServiceUsed,
                 DS.KeysRetainedCodeAtSale,
                 DS.ProcessStatusDescription,
                 DS.ISStockSold,
                 DS.CurrentStockStatusDescription
                 --TimeLine-P2S7
                 ,
                 DS.IsBulkSale AS BulkSaleInd
                 --,DS.IsCreditCardSale
                 ,
                 DS.IsIBFSold AS IBuyFastSoldInd,
                 DS.IsHadReleaseProblem AS HadReleaseProblemInd,
                 DS.IsHadHold,
                 DS.IsOnHold,
                 CASE
                   WHEN DS.IsBuyerRenegedAfterSettlement=1
                         OR DS.IsBuyerCancelAfterSettlement=1 THEN 1
                   ELSE 0
                 END AS IsBuyerRenegedAfterSettlement,
                 CASE
                   WHEN DS.IsBuyerReneged=1
                         OR DS.IsBuyerCancel=1 THEN 1
                   ELSE 0
                 END AS IsBuyerReneged,
                 CASE
                   WHEN DS.BuyerRenegedType='BYRE' THEN 'Buyer Reneged'
                   WHEN DS.BuyerRenegedType='BYCN' THEN 'Buyer Cancel Sale'
                   ELSE ''
                 END AS BuyerRenegeReason,
                 Isnull(DS.PreviousAgreementTermTypeDescription, '') AS PreviousAgreementTermTypeDescription,
                 DS.IsTitleProblem AS TitleProblemInd,
                 Isnull(DS.IsHadTitleProblem, 0) AS HadTitleProblemInd,
                 DS.AssignmentMethod,
                 CASE
                   WHEN AssignmentMethod IN ('ASAP') THEN 1
                   ELSE 0
                 END AS IsManualAssignment,
                 CASE
                   WHEN AssignmentMethod IN ('CSA', 'EBIZ') THEN 1
                   ELSE 0
                 END AS IsElectronicAssignment,
                 DS.IsCreditSale,
                 DS.AdvanceRateType AS AdvanceRateType,
                 CASE
                   WHEN DS.DaysInAdvanceStorageProvider<>0 THEN DS.AdvanceStorageAmount/DaysInAdvanceStorageProvider
                   WHEN DS.DaysInAdvanceStorageIAA<>0 THEN 0
                 END AS AdvanceStorageDailyRate,
                 DS.IAAAdvanceRateType AS IAAAdvanceRateType,
                 DB.BranchName OriginalBranch,
                 DP.ProviderGroupName OriginalProviderGroup,
                 DP.ProviderName OriginalProviderOffice,
                 Isnull(DS.IsBranchTransfer, 0) AS BranchTransferInd,
                 Isnull(DS.IsProviderGroupTransfer, 0) AS ProviderGroupTransferInd,
                 Isnull(DS.IsProviderOfficeTransfer, 0) AS ProviderOfficeTransferInd,
                 CASE
                   WHEN DS.SaleRunCount>0 THEN DS.SaleRunCount-1
                   ELSE 0
                 END AS ReRuns,
                 CASE
                   WHEN DS.SaleRunCountExcludingCancel>DS.SaleRunCount THEN
                     CASE
                       WHEN (DS.SaleRunCount-DS.BuyerRenegedCancelCount)>0 THEN (DS.SaleRunCount-DS.BuyerRenegedCancelCount)-1
                       ELSE 0
                     END
                   WHEN DS.SaleRunCount>=DS.SaleRunCountExcludingCancel
                        AND DS.SaleRunCountExcludingCancel>0 THEN DS.SaleRunCountExcludingCancel-1
                   ELSE 0
                 END ProviderReRuns,
                 CASE
                   WHEN DS.SaleRunCountExCancelExNOBid>DS.SaleRunCount THEN
                     CASE
                       WHEN (DS.SaleRunCount-DS.BuyerRenegedCancelCount)>0 THEN (DS.SaleRunCount-DS.BuyerRenegedCancelCount)-1
                       ELSE 0
                     END
                   WHEN DS.SaleRunCount>=DS.SaleRunCountExCancelExNOBid
                        AND DS.SaleRunCountExCancelExNOBid>0 THEN DS.SaleRunCountExCancelExNOBid-1
                   ELSE 0
                 END ProviderReRunsNoBid,
                 Isnull(DS.IsMinimumBidOutlier, 0) MinimumBidOutlierInd,
                 DS.BuyerFinanceCompanyCode,
                 --START FactStockCycleTimeObjects
                 FSCT.AdvanceStorageEndDate,
                 CONVERT(DATE, FSCT.AdvanceStorageEndDate) AS AdvanceStorageEndDate_Filter,
                 FSCT.AdvanceStorageStartDate,
                 CONVERT(DATE, FSCT.AdvanceStorageStartDate) AS AdvanceStorageStartDate_Filter,
                 FSCT.AdvanceStorageVerifiedDate,
                 CONVERT(DATE, FSCT.AdvanceStorageVerifiedDate) AS AdvanceStorageVerifiedDate_Filter,
                 FSCT.AnticipatedSaleDate,
                 CONVERT(DATE, FSCT.AnticipatedSaleDate) AS AnticipatedSaleDate_Filter,
                 FSCT.AssignmentDateTime,
                 CONVERT(DATE, FSCT.AssignmentDateTime) AS AssignmentDateTime_Filter,
                 FSCT.AuctionDateTime,
                 CONVERT(DATE, FSCT.AuctionDateTime) AS AuctionDateTime_Filter,
                 FSCT.OriginalAuctionDateTime,
                 CONVERT(DATE, FSCT.OriginalAuctionDateTime) AS OriginalAuctionDateTime_Filter,
                 FSCT.BuyerPaymentDateTime,
                 CONVERT(DATE, FSCT.BuyerPaymentDateTime) AS BuyerPaymentDateTime_Filter,
                 FSCT.BuyerPickupDateTime,
                 CONVERT(DATE, FSCT.BuyerPickupDateTime) AS BuyerPickupDateTime_Filter,
                 FSCT.BuyerRenegeCancelSaleDate,
                 CONVERT(DATE, FSCT.BuyerRenegeCancelSaleDate) AS BuyerRenegeCancelSaleDate_Filter,
                 FSCT.CancelDateTime,
                 CONVERT(DATE, FSCT.CancelDateTime) AS CancelDateTime_Filter,
                 FSCT.CertINDateTime,
                 CONVERT(DATE, FSCT.CertINDateTime) AS CertINDateTime_Filter,
                 FSCT.CheckInDateTime,
                 CONVERT(DATE, FSCT.CheckInDateTime) AS CheckInDateTime_Filter,
                 FSCT.CheckInImagesTakenDate,
                 CONVERT(DATE, FSCT.CheckInImagesTakenDate) AS CheckInImagesTakenDate_Filter,
                 FSCT.CheckIssuedDate,
                 CONVERT(DATE, FSCT.CheckIssuedDate) AS CheckIssuedDate_Filter,
                 FSCT.CurrentProcessStatusStartDate,
                 CONVERT(DATE, FSCT.CurrentProcessStatusStartDate) AS CurrentProcessStatusStartDate_Filter,
                 FSCT.CurrentStockStatusStartDate,
                 CONVERT(DATE, FSCT.CurrentStockStatusStartDate) AS CurrentStockStatusStartDate_Filter,
                 FSCT.DateOfLossReportedDateTime,
                 CONVERT(DATE, FSCT.DateOfLossReportedDateTime) AS DateOfLossReportedDateTime_Filter,
                 FSCT.DispatchDateTime,
                 CONVERT(DATE, FSCT.DispatchDateTime) AS DispatchDateTime_Filter,
                 FSCT.DispatchPermanentlyCancelledDateTime,
                 CONVERT(DATE, FSCT.DispatchPermanentlyCancelledDateTime) AS DispatchPermanentlyCancelledDateTime_Filter,
                 FSCT.EarliestSaleDate,
                 CONVERT(DATE, FSCT.EarliestSaleDate) AS EarliestSaleDate_Filter,
                 FSCT.FirstSoldDateTime,
                 CONVERT(DATE, FSCT.FirstSoldDateTime) AS FirstSoldDateTime_Filter,
                 FSCT.HoldReleaseDateTime,
                 CONVERT(DATE, FSCT.HoldReleaseDateTime) AS HoldReleaseDateTime_Filter,
                 FSCT.IBuyFastDisplayEndDate,
                 CONVERT(DATE, FSCT.IBuyFastDisplayEndDate) AS IBuyFastDisplayEndDate_Filter,
                 FSCT.IBuyFastDisplayStartDate,
                 CONVERT(DATE, FSCT.IBuyFastDisplayStartDate) AS IBuyFastDisplayStartDate_Filter,
                 FSCT.InspectionDateTime,
                 CONVERT(DATE, FSCT.InspectionDateTime) AS InspectionDateTime_Filter,
                 FSCT.LastProviderRefundDate AS RefundDateTime,
                 CONVERT(DATE, FSCT.LastProviderRefundDate) AS RefundDateTime_Filter,
                 FSCT.LogInDateTime,
                 CONVERT(DATE, FSCT.LogInDateTime) AS LogInDateTime_Filter,
                 FSCT.LogOutDateTime,
                 CONVERT(DATE, FSCT.LogOutDateTime) AS LogOutDateTime_Filter,
                 FSCT.LossDateTime,
                 CONVERT(DATE, FSCT.LossDateTime) AS LossDateTime_Filter,
                 FSCT.PickupDateTime,
                 CONVERT(DATE, FSCT.PickupDateTime) AS PickupDateTime_Filter,
                 FSCT.PickupDateTime AS StockDropOffDateTime,
                 CONVERT(DATE, FSCT.PickupDateTime) AS StockDropOffDateTime_Filter,
                 FSCT.PlacedOnHoldDate,
                 CONVERT(DATE, FSCT.PlacedOnHoldDate) AS PlacedOnHoldDate_Filter,
                 FSCT.PreBillPaidDateTime,
                 CONVERT(DATE, FSCT.PreBillPaidDateTime) AS PreBillPaidDateTime_Filter,
                 FSCT.ProviderQuotedReleaseDateTime,
                 CONVERT(DATE, FSCT.ProviderQuotedReleaseDateTime) AS ProviderQuotedReleaseDateTime_Filter,
                 FSCT.ReceivedAllDocumentsDateTime,
                 CONVERT(DATE, FSCT.ReceivedAllDocumentsDateTime) AS ReceivedAllDocumentsDateTime_Filter,
                 FSCT.RecoveryDatetime,
                 CONVERT(DATE, FSCT.RecoveryDatetime) AS RecoveryDatetime_Filter,
                 FSCT.RefundPaymentDate,
                 CONVERT(DATE, FSCT.RefundPaymentDate) AS RefundPaymentDate_Filter,
                 FSCT.ReleaseDateTime,
                 CONVERT(DATE, FSCT.ReleaseDateTime) AS ReleaseDateTime_Filter,
                 FSCT.ReleaseProblemIdentifiedDate,
                 CONVERT(DATE, FSCT.ReleaseProblemIdentifiedDate) AS ReleaseProblemIdentifiedDate_Filter,
                 FSCT.ReleaseProblemResolvedDate,
                 CONVERT(DATE, FSCT.ReleaseProblemResolvedDate) AS ReleaseProblemResolvedDate_Filter,
                 FSCT.SaleDocumentReceivedDateTime,
                 CONVERT(DATE, FSCT.SaleDocumentReceivedDateTime) AS SaleDocumentReceivedDateTime_Filter,
                 FSCT.SalesDocumentAppliedDatetime,
                 CONVERT(DATE, FSCT.SalesDocumentAppliedDatetime) AS SalesDocumentAppliedDatetime_Filter,
                 FSCT.SettlementDateTime,
                 CONVERT(DATE, FSCT.SettlementDateTime) AS SettlementDateTime_Filter,
                 FSCT.SettlementPaidDateTime,
                 CONVERT(DATE, FSCT.SettlementPaidDateTime) AS SettlementPaidDateTime_Filter,
                 FSCT.SMVDDatetime,
                 CONVERT(DATE, FSCT.SMVDDatetime) AS SMVDDatetime_Filter,
                 FSCT.SoldDateTime,
                 CONVERT(DATE, FSCT.SoldDateTime) AS SoldDateTime_Filter,
                 FSCT.TitleApprovedByProviderDateTime,
                 CONVERT(DATE, FSCT.TitleApprovedByProviderDateTime) AS TitleApprovedByProviderDateTime_Filter,
                 FSCT.TitleInDateTime,
                 CONVERT(DATE, FSCT.TitleInDateTime) AS TitleInDateTime_Filter,
                 FSCT.TitleProblemIdentifiedDate,
                 CONVERT(DATE, FSCT.TitleProblemIdentifiedDate) AS TitleProblemIdentifiedDate_Filter,
                 FSCT.TitleProblemResolvedDate,
                 CONVERT(DATE, FSCT.TitleProblemResolvedDate) AS TitleProblemResolvedDate_Filter,
                 FSCT.VehicleImagesLoadedDate,
                 CONVERT(DATE, FSCT.VehicleImagesLoadedDate) AS VehicleImagesLoadedDate_Filter,
                 FSCT.VICInspectionDateTime,
                 CONVERT(DATE, FSCT.VICInspectionDateTime) AS VICInspectionDateTime_Filter,
                 FSCT.OriginalBranchChangeDatetime,
                 CONVERT(DATE, FSCT.OriginalBranchChangeDatetime) AS OriginalBranchChangeDate_Filter,
                 FSCT.OriginalProviderTransferDateTime,
                 CONVERT(DATE, FSCT.OriginalProviderTransferDateTime) AS OriginalProviderTransferDate_Filter
          --INTO dbo.TMP_DimStock
          FROM   dbo.LKStockID LK WITH (NOLOCK)
                 INNER JOIN dbo.DimStock DS WITH (NOLOCK)
                   ON LK.StockID=DS.StockID
                 INNER JOIN dbo.FactStockCycleTime FSCT WITH (NOLOCK)
                   ON LK.StockID=FSCT.StockID
                 LEFT OUTER JOIN dbo.DimStorageLocation DSL_SF WITH (NOLOCK)
                   ON DS.SellFromStorageLocationID=DSL_SF.StorageLocationID
                 LEFT OUTER JOIN dbo.DimStorageLocation DSL_PU WITH (NOLOCK)
                   ON DS.PickupLocationID=DSL_PU.StorageLocationID
                 LEFT OUTER JOIN DimBranch DB WITH (nolock)
                   ON DS.OriginalBranchNUmber=DB.BranchNUmber
                 LEFT OUTER JOIN DimProvider DP WITH (nolock)
                   ON DS.OriginalProviderID=DP.ProviderID

          SET @InsertRowCount = @@ROWCOUNT

          --IF OBJECT_ID('edw.dbo.TMP_DimTow') IS NOT NULL DROP TABLE edw.dbo.TMP_DimTow
          --SELECT * INTO edw.dbo.TMP_DimTow FROM [EDW].[dbo].[v_DimTow]
          TRUNCATE TABLE dbo.TMP_DimTow

          INSERT INTO dbo.TMP_DimTow
                      (StockTowID,StockID,TowBillNumber,BranchNumber,BuyerID,IsInterBranchTransfer,IsPickupWithReleaseProblem,
                       IsPickupWithoutReleaseProblem,
                       IsReturnTow,
                       GlobalRate,CustomerType,Customer_Type,DispatchProcessCompleteIndicator,FirstAssignedTowerID,MethodOfPayment,PaidTowerID,
                       IsReleaseProblem,
                       ProviderID,IsTowBillOutlier,IsTowCancelled,TowDays,IsTowDispatchReversed,IsTowDryRun,TowerBranchZoneRateID,IsTowerChanged,
                       TowerID,TowCategory,TowTypeCode,TowTypeDescription,TowerPaymentMethod,TowerPickupPerformance,TowerRate,TowinStorageLocationID,
                       TowMileBand,TowMilesOutlierIndicator,TowoutStorageLocationID,IsTowPriority,IsTowSuccessful,TravelTimeMinutes,AdditionalFeePerMileAtTow,
                       IsBuyerOverrideAtTow,DryRunAmtAtTow,DryRunCalculationMethodAtTow,FuelSurchargeAmtAtTow,FuelSurchargeCalculationMethodAtTow,
                       PickupTimeLimitHoursAtTow,RateAmountAtTow,SalvageDutyTypeCodeAtTow,StockTypeAtTow,TowCustomerTypeCodeAtTow,IsTowerOverrideAtTow,
                       TowRadiusMilesAtTow,IsZipCodeOverrideAtTow,PickupDatime,PickupDatime_Filter,PickupDayId,ActualReleaseDayID,
                       ActualReleaseDateTime,ActualReleaseDateTime_Filter,AssignmentDayID,AssignmentDateTime,AssignmentDateTime_Filter,AssignToPickupBusinessHours,
                       BuyerTowDispatchCompletedDayID,BuyerTowDispatchCompletedDateTime,BuyerTowDispatchCompletedDateTime_Filter,BuyerTowRequestDayID,
                       BuyerTowRequestDateTime,BuyerTowRequestDateTime_Filter,DispatchCancelledDayID,DispatchCancelledDateTime,
                       DispatchCancelledDateTime_Filter,DispatchPermanentlyCancelledDayID,DispatchPermanentlyCancelledDateTime,DispatchPermanentlyCancelledDateTime_Filter,
                       DispatchToLoginBusiness,DispatchToLoginCalendar,DispatchToPickupBusiness,DispatchToPickupCalendar,DispatchTowerAssignedDayID,
                       DispatchTowerAssignedDateTime,DispatchTowerAssignedDateTime_Filter,DispatchTowerDayID,DispatchTowerDateTime,
                       DispatchTowerDateTime_Filter,DispatchTowerLoggedInDayID,DispatchTowerLoggedInDateTime,DispatchTowerLoggedInDateTime_Filter,DispatchTowerLoggedOutDayID,
                       DispatchTowerLoggedOutDateTime,DispatchTowerLoggedOutDateTime_Filter,DryRunDayID,DryRunDateTime,DryRunDateTime_Filter,
                       DryRunEventDayID,DryRunEventDateTime,DryRunEventDateTime_Filter,InterbranchtransferBusinessHours,InterbranchTransferDayID,
                       InterbranchTransferDateTime,InterbranchTransferDateTime_Filter,OffsitePickupDateTime,OffsitePickupDateTime_Filter,OffsitePickupDayID,OffsiteTowDayID,
                       OffsiteTowDateTime,OffsiteTowDateTime_Filter,PickupEnrouteToPickupBusiness,PickupEnrouteToPickupCalendar,PickupToLoginBusiness,
                       PickupToLoginCalendar,ReassignAdministrativeBranchDayID,ReassignAdministrativeBranchDateTime,
                       ReassignAdministrativeBranchDateTime_Filter,ReleaseToPickupBusiness,ReleaseToPickupBusinessHours,ReleaseToPickupCalendar,ReleaseToPickupEffectiveDaysBusiness,
                       ReleaseToPickupEffectiveDaysCalendar,ReturnTowDayID,ReturnTowDateTime,ReturnTowDateTime_Filter,SalvageDroppedOffDayID,
                       SalvageDroppedOffDateTime,SalvageDroppedOffDateTime_Filter,SoldDayID,SoldDateTime,SoldDateTime_Filter,TowCompletedDayID,
                       TowCompletedDateTime,TowCompletedDateTime_Filter,TowInitiatedDayID,TowInitiatedDateTime,TowInitiatedDateTime_Filter,
                       WaitDispatchToWaitDriverBusiness,WaitDispatchToWaitDriverCalendar,WaitDriverToPickupEnrouteBusiness,
                       WaitDriverToPickupEnrouteCalendar,WaitReleaseToWaitDispatchBusiness,WaitReleaseToWaitDispatchCalendar,ReturnTowBusinessHours,To_StorageLocationID,To_LocationName,
                       To_AddressLine1,To_AddressLine2,To_CityName,To_StateAbbreviation,To_ZipCode,To_CountryAbbreviation,To_CountyName,To_CountryName,
                       To_PhoneNumber1,To_LocationPhone,To_PhoneExtension1,To_StorageLocationTypeCode,To_Location_Type,To_UpdateUserID,
                       To_UpdateDateTime,To_PhoneNumber2,To_PhoneExtension2,To_FaxNumber,To_EmailAddress,To_IsAcceptsIAACheck,To_IsAcceptsTowerCheck,To_IsBillandRelease,
                       To_IsCallBeforeArrival,To_IsCashAccepted,To_IsCashOnly,To_IsCreditCardAccepted,To_CheckPayableTo,To_ContactUserID,
                       To_CreateDateTime,To_FederalTaxID,To_FinancialParentStorageLocationID,To_Lattitude,To_Longitude,To_MailAddressLine1,To_MailAddressLine2,
                       To_MailCityName,To_MailStateAbbreviation,To_MailZipCode,To_MailCountryAbbreviation,To_MailCountyName,To_MailCountryName,To_IsOwnersResidence,
                       To_IsPaymentAcceptedAtLocation,To_ParentStorageLocationID,To_Probability,To_IsResidence,To_StorageLocationStatusCode,
                       To_StorageLocationAreaCode,To_IsCurrentValidated,To_CurrentValidatedUserID,To_CurrentValidatedDateTime,To_IsAddressValidated,
                       To_IsAddressValidatedByUser,To_AddressValidatedUserID,To_AddressValidatedDatetime,To_AddressUpdateUserID,
                       To_AddressUpdateDateTime,To_IsUpdateable,To_IsComputedValidated,To_StorageLocationSourceCode,To_IsOffSiteSale,To_ETLLoadID,To_ETLLoadUpdateID,
                       To_StorageLocationAddressID,From_StorageLocationID,From_LocationName,From_AddressLine1,From_AddressLine2,From_CityName,
                       From_StateAbbreviation,From_ZipCode,From_CountryAbbreviation,From_CountyName,From_CountryName,From_PhoneNumber1,
                       From_LocationPhone,From_PhoneExtension1,From_StorageLocationTypeCode,From_Location_Type,From_UpdateUserID,From_UpdateDateTime,From_PhoneNumber2,
                       From_PhoneExtension2,From_FaxNumber,From_EmailAddress,From_IsAcceptsIAACheck,From_IsAcceptsTowerCheck,From_IsBillandRelease,
                       From_IsCallBeforeArrival,From_IsCashAccepted,From_IsCashOnly,From_IsCreditCardAccepted,From_CheckPayableTo,From_ContactUserID,
                       From_CreateDateTime,From_FederalTaxID,From_FinancialParentStorageLocationID,From_Lattitude,From_Longitude,From_MailAddressLine1,
                       From_MailAddressLine2,From_MailCityName,From_MailStateAbbreviation,From_MailZipCode,From_MailCountryAbbreviation,
                       From_MailCountyName,From_MailCountryName,From_IsOwnersResidence,From_IsPaymentAcceptedAtLocation,From_ParentStorageLocationID,From_Probability,
                       From_IsResidence,From_StorageLocationStatusCode,From_StorageLocationAreaCode,From_IsCurrentValidated,
                       From_CurrentValidatedUserID,From_CurrentValidatedDateTime,From_IsAddressValidated,From_IsAddressValidatedByUser,From_AddressValidatedUserID,
                       From_AddressValidatedDatetime,From_AddressUpdateUserID,From_AddressUpdateDateTime,From_IsUpdateable,From_IsComputedValidated,
                       From_StorageLocationSourceCode,From_IsOffSiteSale,From_ETLLoadID,From_ETLLoadUpdateID,From_StorageLocationAddressID,TowZoneID,
                       TowZoneNumber,TowMileageBand,TowMiles)
          SELECT FST.StockTowID,
                 FST.StockID,
                 FST.TowBillNumber,
                 FST.BranchNumber,
                 FST.BuyerID,
                 CASE
                   WHEN (FST.TowTypeCode='IB') THEN 1
                   ELSE 0
                 END AS IsInterBranchTransfer,
                 CASE
                   WHEN (FST.IsReleaseProblem=1
                         AND FST.TowTypeCode='PK'
                         AND FSTCT.DryRunDayID=29991231) THEN 1
                   ELSE 0
                 END AS IsPickupWithReleaseProblem,
                 CASE
                   WHEN (FST.IsReleaseProblem=0
                         AND FST.TowTypeCode='PK'
                         AND FSTCT.DryRunDayID=29991231) THEN 1
                   ELSE 0
                 END AS IsPickupWithoutReleaseProblem,
                 CASE
                   WHEN (FST.TowTypeCode='RT') THEN 1
                   ELSE 0
                 END AS IsReturnTow,
                 FST.GlobalRate,
                 FST.CustomerType,
                 CASE
                   WHEN FST.TowTypeCode='BY' THEN 'Buyer'
                   ELSE 'Salvage Provider'
                 END AS Customer_Type,
                 CASE
                   WHEN Isnull(FST.IsTowDryRun, 0)=1 THEN 1
                   WHEN Isnull(FST.IsTowSuccessful, 0)=1 THEN 1
                   ELSE 0
                 END AS DispatchProcessCompleteIndicator,
                 FST.FirstAssignedTowerID,
                 CASE
                   WHEN FST.TowerPaymentMethod='CSH' THEN 'Cash'
                   WHEN FST.TowerPaymentMethod='IAA' THEN 'IAA Check'
                   WHEN FST.TowerPaymentMethod='EXT' THEN 'Tower Check'
                   WHEN FST.TowerPaymentMethod='MAN' THEN 'IAA Manual Check'
                 END AS MethodOfPayment,
                 FST.PaidTowerID,
                 FST.IsReleaseProblem,
                 FST.ProviderID,
                 FST.IsTowBillOutlier,
                 FST.IsTowCancelled,
                 FST.TowDays,
                 FST.IsTowDispatchReversed,
                 FST.IsTowDryRun,
                 FST.TowerBranchZoneRateID,
                 FST.IsTowerChanged,
                 FST.TowerID,
                 FST.TowCategory,
                 --    FST.TowMileageBand,
                 --dbo.DimTowZone.TowMileageBand,
                 FST.TowTypeCode,
                 TowType.TowTypeDescription,
                 FST.TowerPaymentMethod,
                 FST.TowerPickupPerformance,
                 FST.TowerRate,
                 FST.TowinStorageLocationID,
                 --   FST.TowMiles,
                 CASE
                   WHEN (FST.TowMiles<=25) THEN '0-25'
                   WHEN (FST.TowMiles>25
                         AND FST.TowMiles<=50) THEN '26-50'
                   WHEN (FST.TowMiles>50
                         AND FST.TowMiles<=75) THEN '51-75'
                   WHEN (FST.TowMiles>75
                         AND FST.TowMiles<=100) THEN '76-100'
                   WHEN (FST.TowMiles>100
                         AND FST.TowMiles<=125) THEN '101-125'
                   WHEN (FST.TowMiles>125
                         AND FST.TowMiles<=150) THEN '126-150'
                   WHEN (FST.TowMiles>150
                         AND FST.TowMiles<=175) THEN '151-175'
                   WHEN (FST.TowMiles>175
                         AND FST.TowMiles<=200) THEN '176-200'
                   WHEN (FST.TowMiles>200
                         AND FST.TowMiles<=225) THEN '201-225'
                   WHEN (FST.TowMiles>225
                         AND FST.TowMiles<=250) THEN '226-250'
                   WHEN (FST.TowMiles>250
                         AND FST.TowMiles<=275) THEN '251-275'
                   WHEN (FST.TowMiles>275
                         AND FST.TowMiles<=300) THEN '276-300'
                   WHEN (FST.TowMiles>300) THEN '300+'
                 END AS TowMileBand,
                 CASE
                   WHEN FST.TowMiles>500 THEN 1
                   ELSE 0
                 END AS TowMilesOutlierIndicator,
                 FST.TowoutStorageLocationID,
                 FST.IsTowPriority,
                 FST.IsTowSuccessful,
                 --     FST.TowZoneID,
                 FST.TravelTimeMinutes,
                 FST.AdditionalFeePerMileAtTow,
                 FST.IsBuyerOverrideAtTow,
                 FST.DryRunAmtAtTow,
                 FST.DryRunCalculationMethodAtTow,
                 FST.FuelSurchargeAmtAtTow,
                 FST.FuelSurchargeCalculationMethodAtTow,
                 FST.PickupTimeLimitHoursAtTow,
                 FST.RateAmountAtTow,
                 FST.SalvageDutyTypeCodeAtTow,
                 FST.StockTypeAtTow,
                 FST.TowCustomerTypeCodeAtTow,
                 FST.IsTowerOverrideAtTow,
                 0 AS TowRadiusMilesAtTow,
                 FST.IsZipCodeOverrideAtTow,
                 CASE
                   WHEN FST.TowTypeCode IN ('PK', 'OP') THEN Isnull(FSTCT.SalvageDroppedOffDateTime, '12/31/2999')
                   WHEN FST.TowTypeCode NOT IN ('PK', 'OP') THEN Isnull(FSTCT.DispatchTowerLoggedInDateTime, '12/31/2999')
                   ELSE 0
                 END AS PickupDatime,
                 CONVERT(DATE, CASE WHEN FST.TowTypeCode IN ('PK', 'OP') THEN Isnull(FSTCT.SalvageDroppedOffDateTime, '12/31/2999') WHEN
                 FST.TowTypeCode
                 NOT
                 IN
                 ('PK',
                 'OP') THEN Isnull(FSTCT.DispatchTowerLoggedInDateTime, '12/31/2999') ELSE 0 END) AS PickupDatime_Filter,
                 CASE
                   WHEN FST.TowTypeCode IN ('PK', 'OP') THEN Isnull(FSTCT.SalvageDroppedOffDayID, 29991231)
                   WHEN FST.TowTypeCode NOT IN ('PK', 'OP') THEN Isnull(FSTCT.DispatchTowerLoggedInDayID, 29991231)
                   ELSE 0
                 END PickupDayId,
                 Isnull(FSTCT.ActualReleaseDayID, 29991231) AS ActualReleaseDayID,
                 Isnull(FSTCT.ActualReleaseDateTime, '12/31/2999') AS ActualReleaseDateTime,
                 CONVERT(DATE, Isnull(FSTCT.ActualReleaseDateTime, '12/31/2999')) AS ActualReleaseDateTime_Filter,
                 Isnull(FSTCT.AssignmentDayID, 29991231) AS AssignmentDayID,
                 Isnull(FSTCT.AssignmentDateTime, '12/31/2999') AS AssignmentDateTime,
                 CONVERT(DATE, Isnull(FSTCT.AssignmentDateTime, '12/31/2999')) AS AssignmentDateTime_Filter,
                 FSTCT.AssignToPickupBusinessHours,
                 Isnull(FSTCT.BuyerTowDispatchCompletedDayID, 29991231) AS BuyerTowDispatchCompletedDayID,
                 Isnull(FSTCT.BuyerTowDispatchCompletedDateTime, '12/31/2999') AS BuyerTowDispatchCompletedDateTime,
                 CONVERT(DATE, Isnull(FSTCT.BuyerTowDispatchCompletedDateTime, '12/31/2999')) AS BuyerTowDispatchCompletedDateTime_Filter,
                 Isnull(FSTCT.BuyerTowRequestDayID, 29991231) AS BuyerTowRequestDayID,
                 Isnull(FSTCT.BuyerTowRequestDateTime, '12/31/2999') AS BuyerTowRequestDateTime,
                 CONVERT(DATE, Isnull(FSTCT.BuyerTowRequestDateTime, '12/31/2999')) AS BuyerTowRequestDateTime_Filter,
                 Isnull(FSTCT.DispatchCancelledDayID, 29991231) AS DispatchCancelledDayID,
                 Isnull(FSTCT.DispatchCancelledDateTime, '12/31/2999') AS DispatchCancelledDateTime,
                 CONVERT(DATE, Isnull(FSTCT.DispatchCancelledDateTime, '12/31/2999')) AS DispatchCancelledDateTime_Filter,
                 Isnull(FSTCT.DispatchPermanentlyCancelledDayID, 29991231) AS DispatchPermanentlyCancelledDayID,
                 Isnull(FSTCT.DispatchPermanentlyCancelledDateTime, '12/31/2999') AS DispatchPermanentlyCancelledDateTime,
                 CONVERT(DATE, Isnull(FSTCT.DispatchPermanentlyCancelledDateTime, '12/31/2999')) AS DispatchPermanentlyCancelledDateTime_Filter,
                 FSTCT.DispatchToLoginBusiness,
                 FSTCT.DispatchToLoginCalendar,
                 FSTCT.DispatchToPickupBusiness,
                 FSTCT.DispatchToPickupCalendar,
                 Isnull(FSTCT.DispatchTowerAssignedDayID, 29991231) AS DispatchTowerAssignedDayID,
                 Isnull(FSTCT.DispatchTowerAssignedDateTime, '12/31/2999') AS DispatchTowerAssignedDateTime,
                 CONVERT(DATE, Isnull(FSTCT.DispatchTowerAssignedDateTime, '12/31/2999')) AS DispatchTowerAssignedDateTime_Filter,
                 Isnull(FSTCT.DispatchTowerDayID, 29991231) AS DispatchTowerDayID,
                 Isnull(FSTCT.DispatchTowerDateTime, '12/31/2999') AS DispatchTowerDateTime,
                 CONVERT(DATE, Isnull(FSTCT.DispatchTowerDateTime, '12/31/2999')) AS DispatchTowerDateTime_Filter,
                 Isnull(FSTCT.DispatchTowerLoggedInDayID, 29991231) AS DispatchTowerLoggedInDayID,
                 Isnull(FSTCT.DispatchTowerLoggedInDateTime, '12/31/2999') AS DispatchTowerLoggedInDateTime,
                 CONVERT(DATE, Isnull(FSTCT.DispatchTowerLoggedInDateTime, '12/31/2999')) AS DispatchTowerLoggedInDateTime_Filter,
                 Isnull(FSTCT.DispatchTowerLoggedOutDayID, 29991231) AS DispatchTowerLoggedOutDayID,
                 Isnull(FSTCT.DispatchTowerLoggedOutDateTime, '12/31/2999') AS DispatchTowerLoggedOutDateTime,
                 CONVERT(DATE, Isnull(FSTCT.DispatchTowerLoggedOutDateTime, '12/31/2999')) AS DispatchTowerLoggedOutDateTime_Filter,
                 Isnull(FSTCT.DryRunDayID, 29991231) AS DryRunDayID,
                 Isnull(FSTCT.DryRunDateTime, '12/31/2999') AS DryRunDateTime,
                 CONVERT(DATE, Isnull(FSTCT.DryRunDateTime, '12/31/2999')) AS DryRunDateTime_Filter,
                 Isnull(FSTCT.DryRunEventDayID, 29991231) AS DryRunEventDayID,
                 Isnull(FSTCT.DryRunEventDateTime, '12/31/2999') AS DryRunEventDateTime,
                 CONVERT(DATE, Isnull(FSTCT.DryRunEventDateTime, '12/31/2999')) AS DryRunEventDateTime_Filter,
                 FSTCT.InterbranchtransferBusinessHours,
                 Isnull(FSTCT.InterbranchTransferDayID, 29991231) AS InterbranchTransferDayID,
                 Isnull(FSTCT.InterbranchTransferDateTime, '12/31/2999') AS InterbranchTransferDateTime,
                 CONVERT(DATE, Isnull(FSTCT.InterbranchTransferDateTime, '12/31/2999')) AS InterbranchTransferDateTime_Filter,
                 Isnull(FSTCT.OffsitePickupDateTime, '12/31/2999') AS OffsitePickupDateTime,
                 CONVERT(DATE, Isnull(FSTCT.OffsitePickupDateTime, '12/31/2999')) AS OffsitePickupDateTime_Filter,
                 Isnull(FSTCT.OffsitePickupDayID, 29991231) AS OffsitePickupDayID,
                 Isnull(FSTCT.OffsiteTowDayID, 29991231) AS OffsiteTowDayID,
                 Isnull(FSTCT.OffsiteTowDateTime, '12/31/2999') AS OffsiteTowDateTime,
                 CONVERT(DATE, Isnull(FSTCT.OffsiteTowDateTime, '12/31/2999')) AS OffsiteTowDateTime_Filter,
                 FSTCT.PickupEnrouteToPickupBusiness,
                 FSTCT.PickupEnrouteToPickupCalendar,
                 FSTCT.PickupToLoginBusiness,
                 FSTCT.PickupToLoginCalendar,
                 Isnull(FSTCT.ReassignAdministrativeBranchDayID, 29991231) AS ReassignAdministrativeBranchDayID,
                 Isnull(FSTCT.ReassignAdministrativeBranchDateTime, '12/31/2999') AS ReassignAdministrativeBranchDateTime,
                 CONVERT(DATE, Isnull(FSTCT.ReassignAdministrativeBranchDateTime, '12/31/2999')) AS ReassignAdministrativeBranchDateTime_Filter,
                 FSTCT.ReleaseToPickupBusiness,
                 FSTCT.ReleaseToPickupBusinessHours,
                 FSTCT.ReleaseToPickupCalendar,
                 FSTCT.ReleaseToPickupEffectiveDaysBusiness,
                 FSTCT.ReleaseToPickupEffectiveDaysCalendar,
                 Isnull(FSTCT.ReturnTowDayID, 29991231) AS ReturnTowDayID,
                 Isnull(FSTCT.ReturnTowDateTime, '12/31/2999') AS ReturnTowDateTime,
                 CONVERT(DATE, Isnull(FSTCT.ReturnTowDateTime, '12/31/2999')) AS ReturnTowDateTime_Filter,
                 Isnull(FSTCT.SalvageDroppedOffDayID, 29991231) AS SalvageDroppedOffDayID,
                 Isnull(FSTCT.SalvageDroppedOffDateTime, '12/31/2999') AS SalvageDroppedOffDateTime,
                 CONVERT(DATE, Isnull(FSTCT.SalvageDroppedOffDateTime, '12/31/2999')) AS SalvageDroppedOffDateTime_Filter,
                 Isnull(FSTCT.SoldDayID, 29991231) AS SoldDayID,
                 Isnull(FSTCT.SoldDateTime, '12/31/2999') AS SoldDateTime,
                 CONVERT(DATE, Isnull(FSTCT.SoldDateTime, '12/31/2999')) AS SoldDateTime_Filter,
                 Isnull(FSTCT.TowCompletedDayID, 29991231) AS TowCompletedDayID,
                 Isnull(FSTCT.TowCompletedDateTime, '12/31/2999') AS TowCompletedDateTime,
                 CONVERT(DATE, Isnull(FSTCT.TowCompletedDateTime, '12/31/2999')) AS TowCompletedDateTime_Filter,
                 Isnull(FSTCT.TowInitiatedDayID, 29991231) AS TowInitiatedDayID,
                 Isnull(FSTCT.TowInitiatedDateTime, '12/31/2999') AS TowInitiatedDateTime,
                 CONVERT(DATE, Isnull(FSTCT.TowInitiatedDateTime, '12/31/2999')) AS TowInitiatedDateTime_Filter,
                 FSTCT.WaitDispatchToWaitDriverBusiness,
                 FSTCT.WaitDispatchToWaitDriverCalendar,
                 FSTCT.WaitDriverToPickupEnrouteBusiness,
                 FSTCT.WaitDriverToPickupEnrouteCalendar,
                 FSTCT.WaitReleaseToWaitDispatchBusiness,
                 FSTCT.WaitReleaseToWaitDispatchCalendar,
                 FSTCT.ReturnTowBusinessHours,
                 DSL_IN.StorageLocationID To_StorageLocationID,
                 DSL_IN.LocationName To_LocationName,
                 DSL_IN.AddressLine1 To_AddressLine1,
                 DSL_IN.AddressLine2 To_AddressLine2,
                 DSL_IN.CityName To_CityName,
                 DSL_IN.StateAbbreviation To_StateAbbreviation,
                 DSL_IN.ZipCode To_ZipCode,
                 DSL_IN.CountryAbbreviation To_CountryAbbreviation,
                 DSL_IN.CountyName To_CountyName,
                 DSL_IN.CountryName To_CountryName,
                 DSL_IN.PhoneNumber1 To_PhoneNumber1,
                 CASE
                   WHEN DSL_IN.PhoneNumber1 IS NOT NULL THEN '('+Substring (Cast(DSL_IN.PhoneNumber1 AS VARCHAR), 1, 3)+')'+Substring (
                                                                                                                            Cast(DSL_IN.PhoneNumber1
                                                                                                                                 AS
                                                                                                                                 VARCHAR), 4, 3)+'-'+
                                                             Substring (
                                                             Cast(
                                                             DSL_IN.PhoneNumber1 AS VARCHAR), 7, 4)
                   ELSE 'No Phone Number'
                 END AS To_LocationPhone,
                 DSL_IN.PhoneExtension1 To_PhoneExtension1,
                 DSL_IN.StorageLocationTypeCode To_StorageLocationTypeCode,
                 CASE
                   WHEN DSL_IN.StorageLocationTypeCode='AFFIL' THEN 'Affiliate'
                   WHEN DSL_IN.StorageLocationTypeCode='IAABR' THEN 'IAA Branch'
                   WHEN DSL_IN.StorageLocationTypeCode='Other' THEN 'Other'
                   WHEN DSL_IN.StorageLocationTypeCode='RESID' THEN 'Residential'
                   WHEN DSL_IN.StorageLocationTypeCode='STOLO' THEN 'Storage Location'
                 END AS To_Location_Type,
                 DSL_IN.UpdateUserID To_UpdateUserID,
                 DSL_IN.UpdateDateTime To_UpdateDateTime,
                 DSL_IN.PhoneNumber2 To_PhoneNumber2,
                 DSL_IN.PhoneExtension2 To_PhoneExtension2,
                 DSL_IN.FaxNumber To_FaxNumber,
                 DSL_IN.EmailAddress To_EmailAddress,
                 DSL_IN.IsAcceptsIAACheck To_IsAcceptsIAACheck,
                 DSL_IN.IsAcceptsTowerCheck To_IsAcceptsTowerCheck,
                 DSL_IN.IsBillandRelease To_IsBillandRelease,
                 DSL_IN.IsCallBeforeArrival To_IsCallBeforeArrival,
                 DSL_IN.IsCashAccepted To_IsCashAccepted,
                 DSL_IN.IsCashOnly To_IsCashOnly,
                 DSL_IN.IsCreditCardAccepted To_IsCreditCardAccepted,
                 DSL_IN.CheckPayableTo To_CheckPayableTo,
                 DSL_IN.ContactUserID To_ContactUserID,
                 DSL_IN.CreateDateTime To_CreateDateTime,
                 DSL_IN.FederalTaxID To_FederalTaxID,
                 DSL_IN.FinancialParentStorageLocationID To_FinancialParentStorageLocationID,
                 DSL_IN.Lattitude To_Lattitude,
                 DSL_IN.Longitude To_Longitude,
                 DSL_IN.MailAddressLine1 To_MailAddressLine1,
                 DSL_IN.MailAddressLine2 To_MailAddressLine2,
                 DSL_IN.MailCityName To_MailCityName,
                 DSL_IN.MailStateAbbreviation To_MailStateAbbreviation,
                 DSL_IN.MailZipCode To_MailZipCode,
                 DSL_IN.MailCountryAbbreviation To_MailCountryAbbreviation,
                 DSL_IN.MailCountyName To_MailCountyName,
                 DSL_IN.MailCountryName To_MailCountryName,
                 DSL_IN.IsOwnersResidence To_IsOwnersResidence,
                 DSL_IN.IsPaymentAcceptedAtLocation To_IsPaymentAcceptedAtLocation,
                 DSL_IN.ParentStorageLocationID To_ParentStorageLocationID,
                 DSL_IN.Probability To_Probability,
                 DSL_IN.IsResidence To_IsResidence,
                 DSL_IN.StorageLocationStatusCode To_StorageLocationStatusCode,
                 DSL_IN.StorageLocationAreaCode To_StorageLocationAreaCode,
                 DSL_IN.IsCurrentValidated To_IsCurrentValidated,
                 DSL_IN.CurrentValidatedUserID To_CurrentValidatedUserID,
                 DSL_IN.CurrentValidatedDateTime To_CurrentValidatedDateTime,
                 DSL_IN.IsAddressValidated To_IsAddressValidated,
                 DSL_IN.IsAddressValidatedByUser To_IsAddressValidatedByUser,
                 DSL_IN.AddressValidatedUserID To_AddressValidatedUserID,
                 DSL_IN.AddressValidatedDatetime To_AddressValidatedDatetime,
                 DSL_IN.AddressUpdateUserID To_AddressUpdateUserID,
                 DSL_IN.AddressUpdateDateTime To_AddressUpdateDateTime,
                 DSL_IN.IsUpdateable To_IsUpdateable,
                 DSL_IN.IsComputedValidated To_IsComputedValidated,
                 DSL_IN.StorageLocationSourceCode To_StorageLocationSourceCode,
                 DSL_IN.IsOffSiteSale To_IsOffSiteSale,
                 DSL_IN.ETLLoadID To_ETLLoadID,
                 DSL_IN.ETLLoadUpdateID To_ETLLoadUpdateID,
                 DSL_IN.StorageLocationAddressID To_StorageLocationAddressID,
                 DSL_OUT.StorageLocationID From_StorageLocationID,
                 DSL_OUT.LocationName From_LocationName,
                 DSL_OUT.AddressLine1 From_AddressLine1,
                 DSL_OUT.AddressLine2 From_AddressLine2,
                 DSL_OUT.CityName From_CityName,
                 DSL_OUT.StateAbbreviation From_StateAbbreviation,
                 DSL_OUT.ZipCode From_ZipCode,
                 DSL_OUT.CountryAbbreviation From_CountryAbbreviation,
                 DSL_OUT.CountyName From_CountyName,
                 DSL_OUT.CountryName From_CountryName,
                 DSL_OUT.PhoneNumber1 From_PhoneNumber1,
                 CASE
                   WHEN DSL_OUT.PhoneNumber1 IS NOT NULL THEN '('+Substring (Cast(DSL_OUT.PhoneNumber1 AS VARCHAR), 1, 3)+')'+Substring (
                                                                                                                              Cast(
                                                              DSL_OUT.PhoneNumber1 AS
                                                              VARCHAR), 4, 3)+'-'+
                                                              Substring (
                                                              Cast(
                                                              DSL_OUT.PhoneNumber1 AS VARCHAR), 7, 4)
                   ELSE 'No Phone Number'
                 END AS From_LocationPhone,
                 DSL_OUT.PhoneExtension1 From_PhoneExtension1,
                 DSL_OUT.StorageLocationTypeCode From_StorageLocationTypeCode,
                 CASE
                   WHEN DSL_OUT.StorageLocationTypeCode='AFFIL' THEN 'Affiliate'
                   WHEN DSL_OUT.StorageLocationTypeCode='IAABR' THEN 'IAA Branch'
                   WHEN DSL_OUT.StorageLocationTypeCode='Other' THEN 'Other'
                   WHEN DSL_OUT.StorageLocationTypeCode='RESID' THEN 'Residential'
                   WHEN DSL_OUT.StorageLocationTypeCode='STOLO' THEN 'Storage Location'
                 END AS From_Location_Type,
                 DSL_OUT.UpdateUserID From_UpdateUserID,
                 DSL_OUT.UpdateDateTime From_UpdateDateTime,
                 DSL_OUT.PhoneNumber2 From_PhoneNumber2,
                 DSL_OUT.PhoneExtension2 From_PhoneExtension2,
                 DSL_OUT.FaxNumber From_FaxNumber,
                 DSL_OUT.EmailAddress From_EmailAddress,
                 DSL_OUT.IsAcceptsIAACheck From_IsAcceptsIAACheck,
                 DSL_OUT.IsAcceptsTowerCheck From_IsAcceptsTowerCheck,
                 DSL_OUT.IsBillandRelease From_IsBillandRelease,
                 DSL_OUT.IsCallBeforeArrival From_IsCallBeforeArrival,
                 DSL_OUT.IsCashAccepted From_IsCashAccepted,
                 DSL_OUT.IsCashOnly From_IsCashOnly,
                 DSL_OUT.IsCreditCardAccepted From_IsCreditCardAccepted,
                 DSL_OUT.CheckPayableTo From_CheckPayableTo,
                 DSL_OUT.ContactUserID From_ContactUserID,
                 DSL_OUT.CreateDateTime From_CreateDateTime,
                 DSL_OUT.FederalTaxID From_FederalTaxID,
                 DSL_OUT.FinancialParentStorageLocationID From_FinancialParentStorageLocationID,
                 DSL_OUT.Lattitude From_Lattitude,
                 DSL_OUT.Longitude From_Longitude,
                 DSL_OUT.MailAddressLine1 From_MailAddressLine1,
                 DSL_OUT.MailAddressLine2 From_MailAddressLine2,
                 DSL_OUT.MailCityName From_MailCityName,
                 DSL_OUT.MailStateAbbreviation From_MailStateAbbreviation,
                 DSL_OUT.MailZipCode From_MailZipCode,
                 DSL_OUT.MailCountryAbbreviation From_MailCountryAbbreviation,
                 DSL_OUT.MailCountyName From_MailCountyName,
                 DSL_OUT.MailCountryName From_MailCountryName,
                 DSL_OUT.IsOwnersResidence From_IsOwnersResidence,
                 DSL_OUT.IsPaymentAcceptedAtLocation From_IsPaymentAcceptedAtLocation,
                 DSL_OUT.ParentStorageLocationID From_ParentStorageLocationID,
                 DSL_OUT.Probability From_Probability,
                 DSL_OUT.IsResidence From_IsResidence,
                 DSL_OUT.StorageLocationStatusCode From_StorageLocationStatusCode,
                 DSL_OUT.StorageLocationAreaCode From_StorageLocationAreaCode,
                 DSL_OUT.IsCurrentValidated From_IsCurrentValidated,
                 DSL_OUT.CurrentValidatedUserID From_CurrentValidatedUserID,
                 DSL_OUT.CurrentValidatedDateTime From_CurrentValidatedDateTime,
                 DSL_OUT.IsAddressValidated From_IsAddressValidated,
                 DSL_OUT.IsAddressValidatedByUser From_IsAddressValidatedByUser,
                 DSL_OUT.AddressValidatedUserID From_AddressValidatedUserID,
                 DSL_OUT.AddressValidatedDatetime From_AddressValidatedDatetime,
                 DSL_OUT.AddressUpdateUserID From_AddressUpdateUserID,
                 DSL_OUT.AddressUpdateDateTime From_AddressUpdateDateTime,
                 DSL_OUT.IsUpdateable From_IsUpdateable,
                 DSL_OUT.IsComputedValidated From_IsComputedValidated,
                 DSL_OUT.StorageLocationSourceCode From_StorageLocationSourceCode,
                 DSL_OUT.IsOffSiteSale From_IsOffSiteSale,
                 DSL_OUT.ETLLoadID From_ETLLoadID,
                 DSL_OUT.ETLLoadUpdateID From_ETLLoadUpdateID,
                 DSL_OUT.StorageLocationAddressID From_StorageLocationAddressID,
                 --,ISNULL(dbo.DimTowZone.TowZoneNumber, 0) TowZoneNumber
                 --,ISNULL(T.TowZoneNumber, 0) As TowZoneNumber
                 --,ISNULL(T.TowMileageBand, '') AS TowMileageBand
                 --,ISNULL(T.towzoneid, 0) As towzoneid
                 Isnull(T.towzoneid, 0) TowZoneID,
                 Isnull(T.TowZoneNumber, 0) TowZoneNumber,
                 Isnull(T.TowMileageBand, '') TowMileageBand,
                 Isnull(FST.TowMiles, 0) TowMiles
          FROM   dbo.LKStockID LK WITH (NOLOCK)
                 INNER JOIN dbo.FactStockTow FST WITH(Nolock)
                   ON LK.StockID=FST.StockID
                 LEFT OUTER JOIN dbo.FactStockTowCycleTime FSTCT WITH(Nolock)
                   ON FST.StockID=FSTCT.StockID
                      AND FST.TowBillNumber=FSTCT.TowBillNumber
                 LEFT OUTER JOIN dbo.DimStorageLocation DSL_IN WITH(Nolock)
                   ON FST.TowinStorageLocationID=DSL_IN.StorageLocationID
                 LEFT OUTER JOIN dbo.DimStorageLocation DSL_OUT WITH(Nolock)
                   ON FST.TowoutStorageLocationID=DSL_OUT.StorageLocationID
                 LEFT OUTER JOIN dbo.DimTowType TowType WITH(Nolock)
                   ON FST.TowTypeCode=TowType.TowTypeCode
                 LEFT OUTER JOIN (SELECT towzoneid,
                                         BranchNumber,
                                         TowZoneNumber,
                                         TowCustomerTypeCode,
                                         RadiusMiles,
                                         TravelTimeMinutes,
                                         TowMileageBand
                                  FROM   dbo.DimTowZone WITH (NOLOCK)
                                  WHERE  towzoneid IN (SELECT Max(towzoneid)
                                                       FROM   dbo.DimTowZone WITH (NOLOCK)
                                                       GROUP  BY BranchNumber,
                                                                 TowZoneNumber,
                                                                 TowCustomerTypeCode)) T
                   ON FST.BranchNumber=T.BranchNumber
                      AND FST.TowZoneNumber=T.TowZoneNumber
                      AND FST.CustomerType=T.TowCustomerTypeCode

          SET @InsertRowCount = @InsertRowCount+@@ROWCOUNT

          /*********************************************************************************************
          Return the change counts
          *********************************************************************************************/
          EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

          COMMIT TRAN

          RETURN 0
      END TRY

      BEGIN CATCH
          SET NOCOUNT OFF

          IF @@TRANCOUNT>0
            ROLLBACK TRAN

          EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ErrorCode = @@ERROR,@UpdateRowCount = @UpdateRowCount,@InsertRowCount
          =
          @InsertRowCount

          DECLARE @ErrorMessage  NVARCHAR(4000),
                  @ErrorSeverity INT,
                  @ErrorState    INT;

          SELECT @ErrorMessage=Error_message(),
                 @ErrorSeverity=Error_severity(),
                 @ErrorState=Error_state();

          RAISERROR (@ErrorMessage,
                     @ErrorSeverity,
                     @ErrorState);

          RETURN -1
      END CATCH
  END 
