--USE [EDW]
--GO
--/****** Object:  StoredProcedure [dbo].[usp_dqsUpdateEVMAttributeValues]    Script Date: 5/1/2014 10:25:28 AM ******/
--SET ANSI_NULLS ON
--GO
--SET QUOTED_IDENTIFIER ON
--GO
/*
USE [EDW]
GO
==============================================================================
Author		: Bruce Kutnick
Create date	: 4/30/2014
Description	: Updates EVMAttributeValue Table With Cleansed Data  
==============================================================================
REVISION History
ChangeDate	Developer	    Release/Problem Number
4/30/2014	B Kutnick		Initial Release - upon initial release we are only includin
							REPORTING and MERCHANDISING. We NEVER change the Polk/EVM attributes
							and this time we are not updating data from the INTERCHANGE group.

7/2/2014	B Kutnick		Added calls to execute procs 'usp_LoadMerchandisingRecordsToCleanse' 
							and 'usp_LoadReportingRecordsToCleanse' on STAGING.  This will insure
							that updated EVMAttributeValues are available for the next subsequent 
							cleansing.

7/2/2014	B Kutnick		Added AttributeID and BusinessGroupID to EVMAttributeChangeAudit table
==============================================================================
Usage Example:
 
==============================================================================
*/
-- exec [dbo].[usp_dqsUpdateEVMAttributeValues]
CREATE PROCEDURE [dbo].[usp_dqsUpdateEVMAttributeValues]
AS
BEGIN

SET NOCOUNT ON

DECLARE @RecordCounter int
CREATE  TABLE #UpdateSet
		(
		AttributeIDToUpdate int,
		UpdatedValue varchar(35),
		UpdateReason nvarchar(100)
		)
INSERT	INTO  #UpdateSet
		(
		AttributeIDToUpdate,
		UpdatedValue,
		UpdateReason
		)
 

-- Insert Merchandising Data
SELECT	Make_AttributeID_Output,
		Make_Value_Output,
		LEFT([CompositeMerchandisingDomain_Reason], 100)
FROM	STAGING.[dbo].[dqsEVMMerchandisingCleansingOutput] WITH (NOLOCK)
WHERE	[Make_Value_Source] <> [Make_Value_Output]
AND		Record_status = 'Corrected'
UNION ALL
SELECT	BaseModel_AttributeID_Output,
		BaseModel_value_Output,
		LEFT([CompositeMerchandisingDomain_Reason], 100)
FROM	STAGING.[dbo].[dqsEVMMerchandisingCleansingOutput] WITH (NOLOCK)
WHERE	[BaseModel_Value_Source] <> [BaseModel_Value_Output]
AND		Record_status = 'Corrected'
UNION ALL
SELECT	SeriesName_AttributeID_Output,
		SeriesName_value_Output,
		LEFT([CompositeMerchandisingDomain_Reason], 100)
FROM	STAGING.[dbo].[dqsEVMMerchandisingCleansingOutput] WITH (NOLOCK)
WHERE	[SeriesName_Value_Source] <> [SeriesName_Value_Output]
AND		Record_status = 'Corrected'
UNION ALL
SELECT	FullBodyStyle_AttributeID_Output,
		FullBodyStyle_value_Output,
		LEFT([CompositeMerchandisingDomain_Reason], 100)
FROM	STAGING.[dbo].[dqsEVMMerchandisingCleansingOutput] WITH (NOLOCK)
WHERE	[FullBodyStyle_Value_Source] <> [FullBodyStyle_Value_Output]
AND		Record_status = 'Corrected'
UNION ALL
SELECT	FuelType_AttributeID_Output,
		FuelType_value_Output,
		LEFT([CompositeMerchandisingDomain_Reason], 100)
FROM	STAGING.[dbo].[dqsEVMMerchandisingCleansingOutput] WITH (NOLOCK)
WHERE	[FuelType_Value_Source] <> [FuelType_Value_Output]
AND		Record_status = 'Corrected'

--- Insert Reporting Data
UNION ALL
SELECT	Make_AttributeID_Output,
		Make_Value_Output,
		LEFT([CompositeReportingDomain_Reason], 100)
FROM	STAGING.[dbo].[dqsEVMReportingCleansingOutput] WITH (NOLOCK)
WHERE	[Make_Value_Source] <> [Make_Value_Output]
AND		Record_status = 'Corrected'
UNION ALL
SELECT	BaseModel_AttributeID_Output,
		BaseModel_value_Output,
		LEFT([CompositeReportingDomain_Reason], 100)
FROM	STAGING.[dbo].[dqsEVMReportingCleansingOutput] WITH (NOLOCK)
WHERE	[BaseModel_Value_Source] <> [BaseModel_Value_Output]
AND		Record_status = 'Corrected'
UNION ALL
SELECT	SeriesName_AttributeID_Output,
		SeriesName_value_Output,
		LEFT([CompositeReportingDomain_Reason], 100)
FROM	STAGING.[dbo].[dqsEVMReportingCleansingOutput] WITH (NOLOCK)
WHERE	[SeriesName_Value_Source] <> [SeriesName_Value_Output]
AND		Record_status = 'Corrected'
UNION ALL
SELECT	FullBodyStyle_AttributeID_Output,
		FullBodyStyle_value_Output,
		LEFT([CompositeReportingDomain_Reason], 100)
FROM STAGING.[dbo].[dqsEVMReportingCleansingOutput] WITH (NOLOCK)
WHERE	[FullBodyStyle_Value_Source] <> [FullBodyStyle_Value_Output]
AND		Record_status = 'Corrected'
UNION ALL
SELECT	FuelType_AttributeID_Output,
		FuelType_value_Output,
		LEFT([CompositeReportingDomain_Reason], 100)
FROM STAGING.[dbo].[dqsEVMReportingCleansingOutput] WITH (NOLOCK)
WHERE	[FuelType_Value_Source] <> [FuelType_Value_Output]
AND		Record_status = 'Corrected'

SELECT	@RecordCounter = COUNT(*) from #UpdateSet
IF		@RecordCounter > 0
BEGIN	TRY
	BEGIN TRANSACTION
		-- insert into the audit table prior to updating main table
		INSERT INTO  EDW.[dbo].[EVMAttributeChangeAudit]  
				(
				AttributeValueID,
				MatchKey,
				YearModel,
				BusinessGroup,
				AttributeChanged,
				PriorValue,
				UpdatedValue,
				UpdateReason,
				UpdateDateBy,
				UpdatedDateTime,
				AttributeID,
				BusinessGroupID
				) 
		SELECT	atv.ID AttributeValueID,
				atv.matchKey,
				atv.YearModel,
				bg.Description AS [BusinessGroup],
				a.AttributeName AS [AttributeChanged],
				atv.Value [PriorValue],
				ups.UpdatedValue,
				ups.UpdateReason  AS [UpdateReason],
				'DataCleansingProject' AS [UpdatedBy],
				GETDATE() AS [UpdatedDateTime],
				bga.AttributeID,
				bga.BusinessGroupID
		FROM	#UpdateSet ups WITH (NOLOCK)
				INNER JOIN EDW.[dbo].[EVMAttributeValue] atv WITH (NOLOCK) 
					ON atv.ID = ups.AttributeIDToUpdate 
				INNER JOIN [dbo].[EVMBusinessGroupAttribute] bga WITH (NOLOCK)
		 			ON bga.[BusinessGroupAttributeID]  = atv.[BusinessGroupAttributeID]
				INNER JOIN [dbo].[EVMBusinessGroup] bg WITH (NOLOCK)
					ON bg.BusinessGroupID = bga.BusinessGroupID
				INNER JOIN [dbo].[EVMAttribute] a WITH (NOLOCK)
					ON a.AttributeID = bga.AttributeID
 

		UPDATE	EDW.dbo.EVMAttributeValue
		SET		VALUE = s.UpdatedValue,
				UpdatedDateTime = GETDATE()
		FROM	#UpdateSet s WITH (NOLOCK)
				INNER JOIN EDW.dbo.EVMAttributeValue av WITH (NOLOCK)
					ON av.ID = s.AttributeIDToUpdate

		-- If successful, truncate the staging tables
		TRUNCATE TABLE STAGING.[dbo].[dqsEVMReportingCleansingOutput] 
		TRUNCATE TABLE STAGING.[dbo].[dqsEVMMerchandisingCleansingOutput] 


		-- now that EVMAttributeValue has been updated we need to rebuild a data set
		-- for the next DQS Cleansing
		EXEC STAGING.[dbo].[usp_LoadMerchandisingRecordsToCleanse]
		EXEC STAGING.[dbo].[usp_LoadReportingRecordsToCleanse]

	COMMIT TRANSACTION
END TRY

BEGIN CATCH        
	IF @@TRANCOUNT > 0
	BEGIN
		ROLLBACK TRANSACTION 
	END
		
	-- Raise an error with the details of the exception
	DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
	SELECT @ErrMsg = ERROR_MESSAGE(),
			@ErrSeverity = ERROR_SEVERITY()

	RAISERROR(@ErrMsg, @ErrSeverity, 1)
END CATCH


END
