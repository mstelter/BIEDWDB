/* 
USE EDW
GO 
==============================================================================
Author		: Matt Stelter
Create date	: 01/23/2013
Description	: Populate the EDW PolkPrefix table from POLK database	
==============================================================================
REVISION History
ChangeDate	Developer	Sprint		

==============================================================================
Usage Example:
Exec usp_ETLPolkPrefix
==============================================================================
*/
CREATE PROCEDURE [dbo].[Usp_etlpolkprefix]
AS
  BEGIN
      DECLARE @ExtractRowCount INT

      SET NOCOUNT ON;

      BEGIN TRY
          BEGIN TRAN

      MERGE [EDW].dbo.PolkPrefix AS EDW_Polk USING [POLK].dbo.Polk_Prefix AS ASAP_Polk WITH (NOLOCK) ON ASAP_Polk.MatchKey = EDW_Polk.MatchKey AND
      ASAP_Polk.MatchKey_1_8 = EDW_Polk.MatchKey_1_8 AND ASAP_Polk.MatchKey_10 = EDW_Polk.MatchKey_10 WHEN MATCHED THEN UPDATE SET
      EDW_Polk.MatchKey=ASAP_Polk.MatchKey, EDW_Polk.MakeAbbreviation=ASAP_Polk.MakeAbbreviation, EDW_Polk.YearModel=ASAP_Polk.YearModel,
      EDW_Polk.VehicleType=ASAP_Polk.VehicleType, EDW_Polk.MakeName=ASAP_Polk.MakeName, EDW_Polk.SeriesName=ASAP_Polk.SeriesName,
      EDW_Polk.BodyType=ASAP_Polk.BodyType,
      EDW_Polk.WheelsDrivingWheels=ASAP_Polk.WheelsDrivingWheels, EDW_Polk.CubicInchDisplacement=ASAP_Polk.CubicInchDisplacement,
      EDW_Polk.Cylinders=ASAP_Polk.Cylinders,
      EDW_Polk.Fuel=ASAP_Polk.Fuel, EDW_Polk.Carburetion=ASAP_Polk.Carburetion, EDW_Polk.GVWCycles=ASAP_Polk.GVWCycles,
      EDW_Polk.WheelBase=ASAP_Polk.WheelBase, EDW_Polk.TireSize=ASAP_Polk.TireSize, EDW_Polk.TonRating=ASAP_Polk.TonRating,
      EDW_Polk.BaseShippingWeight=ASAP_Polk.BaseShippingWeight,
      EDW_Polk.WeightVariance=ASAP_Polk.WeightVariance, EDW_Polk.BaseListPrice=ASAP_Polk.BaseListPrice,
      EDW_Polk.PriceVariance=ASAP_Polk.PriceVariance,
      EDW_Polk.HighPerformanceCode=ASAP_Polk.HighPerformanceCode, EDW_Polk.DrivingWheels=ASAP_Polk.DrivingWheels, EDW_Polk.Symbol=ASAP_Polk.Symbol,
      EDW_Polk.LocationIndicator=ASAP_Polk.LocationIndicator, EDW_Polk.AirConditioning=ASAP_Polk.AirConditioning,
      EDW_Polk.PowerSteering=ASAP_Polk.PowerSteering, EDW_Polk.PowerBrakes=ASAP_Polk.PowerBrakes, EDW_Polk.PowerWindows=ASAP_Polk.PowerWindows,
      EDW_Polk.TiltWheel=ASAP_Polk.TiltWheel,
      EDW_Polk.Roof=ASAP_Polk.Roof, EDW_Polk.OptionalRoof1=ASAP_Polk.OptionalRoof1, EDW_Polk.OptionalRoof2=ASAP_Polk.OptionalRoof2,
      EDW_Polk.Radio=ASAP_Polk.Radio, EDW_Polk.OptionalRadio1=ASAP_Polk.OptionalRadio1, EDW_Polk.OptionalRadio2=ASAP_Polk.OptionalRadio2,
      EDW_Polk.Transmission=ASAP_Polk.Transmission, EDW_Polk.OptionalTransmission1=ASAP_Polk.OptionalTransmission1,
      EDW_Polk.OptionalTransmission2=ASAP_Polk.OptionalTransmission2, EDW_Polk.AntiLockBrakes=ASAP_Polk.AntiLockBrakes,
      EDW_Polk.SecuritSystem=ASAP_Polk.SecuritSystem,
      EDW_Polk.DaytimeRunningLights=ASAP_Polk.DaytimeRunningLights, EDW_Polk.VISRAP=ASAP_Polk.VISRAP,
      EDW_Polk.CabConfiguration=ASAP_Polk.CabConfiguration,
      EDW_Polk.FrontAxleCode=ASAP_Polk.FrontAxleCode, EDW_Polk.RearAxleCode=ASAP_Polk.RearAxleCode, EDW_Polk.BrakesCode=ASAP_Polk.BrakesCode,
      EDW_Polk.EngineManufacturer=ASAP_Polk.EngineManufacturer, EDW_Polk.EngineModel=ASAP_Polk.EngineModel,
      EDW_Polk.EngineTypeCode=ASAP_Polk.EngineTypeCode,
      EDW_Polk.CommercialTrailer=ASAP_Polk.CommercialTrailer, EDW_Polk.CommercialTrailerNumberofAxles=ASAP_Polk.CommercialTrailerNumberofAxles,
      EDW_Polk.CommercialTrailerLength=ASAP_Polk.CommercialTrailerLength, EDW_Polk.ProactiveVINIndicator=ASAP_Polk.ProactiveVINIndicator,
      EDW_Polk.MAStateExceptions=ASAP_Polk.MAStateExceptions, EDW_Polk.TXStateExceptions=ASAP_Polk.TXStateExceptions,
      EDW_Polk.SeriesAbbrev=ASAP_Polk.SeriesAbbrev,
      EDW_Polk.VINPattern=ASAP_Polk.VINPattern, EDW_Polk.NCIC=ASAP_Polk.NCIC, EDW_Polk.FullBodyStyleName=ASAP_Polk.FullBodyStyleName,
      EDW_Polk.NVPP=ASAP_Polk.NVPP,
      EDW_Polk.SegmentationCode=ASAP_Polk.SegmentationCode, EDW_Polk.CountryofOrigin=ASAP_Polk.CountryofOrigin,
      EDW_Polk.EngineInformation=ASAP_Polk.EngineInformation,
      EDW_Polk.Transmission_2=ASAP_Polk.Transmission_2, EDW_Polk.BaseModel=ASAP_Polk.BaseModel, EDW_Polk.Filler=ASAP_Polk.Filler,
      EDW_Polk.MatchKey_1_8=ASAP_Polk.MatchKey_1_8, EDW_Polk.MatchKey_10=ASAP_Polk.MatchKey_10 
      WHEN NOT MATCHED THEN INSERT (MatchKey,
      MakeAbbreviation, YearModel, VehicleType
      , MakeName, SeriesName, BodyType, WheelsDrivingWheels, CubicInchDisplacement, Cylinders, Fuel, Carburetion, GVWCycles, WheelBase, TireSize,
      TonRating, BaseShippingWeight, WeightVariance, BaseListPrice, PriceVariance, HighPerformanceCode, DrivingWheels, Symbol, LocationIndicator,
      AirConditioning, PowerSteering, PowerBrakes, PowerWindows, TiltWheel, Roof, OptionalRoof1, OptionalRoof2, Radio, OptionalRadio1, OptionalRadio2,
      Transmission, OptionalTransmission1, OptionalTransmission2, AntiLockBrakes, SecuritSystem, DaytimeRunningLights, VISRAP, CabConfiguration,
      FrontAxleCode, RearAxleCode, BrakesCode, EngineManufacturer, EngineModel, EngineTypeCode, CommercialTrailer, CommercialTrailerNumberofAxles,
      CommercialTrailerLength, ProactiveVINIndicator, MAStateExceptions, TXStateExceptions, SeriesAbbrev, VINPattern, NCIC, FullBodyStyleName, NVPP,
      SegmentationCode, CountryofOrigin, EngineInformation, Transmission_2, BaseModel, Filler, MatchKey_1_8, MatchKey_10) VALUES (ASAP_POLK.MatchKey,
      ASAP_POLK.MakeAbbreviation, ASAP_POLK.YearModel, ASAP_POLK.VehicleType, ASAP_POLK.MakeName, ASAP_POLK.SeriesName, ASAP_POLK.BodyType,
      ASAP_POLK.WheelsDrivingWheels, ASAP_POLK.CubicInchDisplacement, ASAP_POLK.Cylinders, ASAP_POLK.Fuel, ASAP_POLK.Carburetion, ASAP_POLK.GVWCycles,
      ASAP_POLK.WheelBase,
      ASAP_POLK.TireSize, ASAP_POLK.TonRating, ASAP_POLK.BaseShippingWeight, ASAP_POLK.WeightVariance, ASAP_POLK.BaseListPrice,
      ASAP_POLK.PriceVariance,
      ASAP_POLK.HighPerformanceCode, ASAP_POLK.DrivingWheels, ASAP_POLK.Symbol, ASAP_POLK.LocationIndicator, ASAP_POLK.AirConditioning,
      ASAP_POLK.PowerSteering, ASAP_POLK.PowerBrakes, ASAP_POLK.PowerWindows, ASAP_POLK.TiltWheel, ASAP_POLK.Roof, ASAP_POLK.OptionalRoof1,
      ASAP_POLK.OptionalRoof2,
      ASAP_POLK.Radio, ASAP_POLK.OptionalRadio1, ASAP_POLK.OptionalRadio2, ASAP_POLK.Transmission, ASAP_POLK.OptionalTransmission1,
      ASAP_POLK.OptionalTransmission2, ASAP_POLK.AntiLockBrakes, ASAP_POLK.SecuritSystem, ASAP_POLK.DaytimeRunningLights, ASAP_POLK.VISRAP,
      ASAP_POLK.CabConfiguration,
      ASAP_POLK.FrontAxleCode, ASAP_POLK.RearAxleCode, ASAP_POLK.BrakesCode, ASAP_POLK.EngineManufacturer, ASAP_POLK.EngineModel,
      ASAP_POLK.EngineTypeCode, ASAP_POLK.CommercialTrailer, ASAP_POLK.CommercialTrailerNumberofAxles, ASAP_POLK.CommercialTrailerLength,
      ASAP_POLK.ProactiveVINIndicator, ASAP_POLK.MAStateExceptions, ASAP_POLK.TXStateExceptions, ASAP_POLK.SeriesAbbrev, ASAP_POLK.VINPattern,
      ASAP_POLK.NCIC,
      ASAP_POLK.FullBodyStyleName, ASAP_POLK.NVPP, ASAP_POLK.SegmentationCode, ASAP_POLK.CountryofOrigin, ASAP_POLK.EngineInformation,
      ASAP_POLK.Transmission_2,
      ASAP_POLK.BaseModel, ASAP_POLK.Filler, ASAP_POLK.MatchKey_1_8, ASAP_POLK.MatchKey_10);

          --FROM  [POLK].dbo.Polk_Prefix AS ASAP_Polk WITH (NOLOCK)
          --	LEFT OUTER JOIN [EDW].dbo.Polk_Prefix AS EDW_Polk WITH (NOLOCK)
          --		ON ASAP_Polk.MatchKey = EDW_Polk.MatchKey
          --			AND ASAP_Polk.MatchKey_1_8 = EDW_Polk.MatchKey_1_8
          --			AND ASAP_Polk.MatchKey_10 = EDW_Polk.MatchKey_10
          --WHERE EDW_Polk.MatchKey IS NULL order by 1
          --AND ASAP_Polk.YEarModel >= YEAR(GETDATE())
          COMMIT TRAN

          RETURN 0
      END TRY

      BEGIN CATCH
          SET NOCOUNT OFF

          ROLLBACK TRAN

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


