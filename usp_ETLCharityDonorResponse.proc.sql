/* 
USE EDW
GO 
==============================================================================
Author		: Matt Stelter
Create date	: 5/6/2013
Description	: Populate the EDW Charity DonorResponse tables from TEMP Charity tables	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number
5/13/2013	MStelter			Added update for IsMostRecent
==============================================================================
Usage Example:
Exec EDW.dbo.usp_ETLCharityDonorResponse
==============================================================================

TRUNCATE TABLE EDW.dbo.CharityDonorResponse

*/

CREATE PROCEDURE [dbo].[usp_ETLCharityDonorResponse]
AS     
BEGIN
DECLARE @ETLLoadID INT,
		@InsertRowCount INT,
		@UpdateRowCount INT

----Run the Audit begin
EXEC @ETLLoadID = EDW.[dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLCharityDonorResponse'

DECLARE @TableName    VARCHAR(50),
        @Fields       VARCHAR(max),
        @InsertValues VARCHAR(max),
        @FieldName    VARCHAR(50),
        @Datatype     VARCHAR(50),
        @MaxLength    INT,
        @SQL          VARCHAR(8000),
        @RecordCount  INT,
        @FileName     VARCHAR(255),
        @BIUpdateDatetime DATETIME	
        
SET NOCOUNT ON;
BEGIN TRY

SET @BIUpdateDatetime = GETDATE()

/********************************************************************/
/************Remove trailing "Z" on date values**********************/
/********************************************************************/
IF OBJECT_ID('tempdb..#tmp_RecordCount') IS NOT NULL DROP TABLE #tmp_RecordCount
CREATE TABLE #tmp_RecordCount(ChangeType varchar(20)) 

SET @TableName = 'Staging.dbo.Charity_SF_DonorResponse'

      SET @Fields = ''
      SET @InsertValues = ''
      SET @FieldName = ''
      SET @MaxLength = 0

      DECLARE TableFields CURSOR FOR
        SELECT c.name 'ColumnName'
        FROM   Staging.sys.columns c
        WHERE  c.object_id=Object_id(@TableName)

      OPEN TableFields;

      FETCH NEXT FROM TableFields INTO @FieldName

      WHILE @@FETCH_STATUS=0 BEGIN
            IF @FieldName LIKE '%date%'
                OR @FieldName LIKE '%modstamp%' BEGIN
                  SET @SQL = 'UPDATE '+@TableName+' SET '+@FieldName+' = 
				CASE WHEN RIGHT(['+@FieldName+'],1) = ''z'' 
					THEN LEFT(['+@FieldName+
                  '],LEN(['+
                             @FieldName
                             +'])-1) 
					ELSE ['+
                             @FieldName+'] 
					END'

                  EXEC (@SQL)
              END

            FETCH NEXT FROM TableFields INTO @FieldName
        END

      CLOSE TableFields;

      DEALLOCATE TableFields;

BEGIN TRAN
/********************************************************************/
/************Merge Campaign****************************************/
/********************************************************************/
    MERGE EDW.[dbo].[CharityDonorResponse] AS CC
    USING Staging.[dbo].[Charity_SF_DonorResponse] AS SC
          ON CC.[ID] = SC.[Id]
    WHEN MATCHED THEN 
        UPDATE SET 
			[SYSTEMMODSTAMP]=SC.[SYSTEMMODSTAMP],
			[QUESTION__C]=SC.[QUESTION__C],
			[QUESTIONTEXT__C]=SC.[QUESTIONTEXT__C],
			[OWNERID]=SC.[OWNERID],
			[NAME]=SC.[NAME],
			[LASTMODIFIEDDATE]=SC.[LASTMODIFIEDDATE],
			[LASTMODIFIEDBYID]=SC.[LASTMODIFIEDBYID],
			[ISDELETED]=SC.[ISDELETED],
			[ID]=SC.[ID],
			[DONATION__C]=SC.[DONATION__C],
			[CREATEDDATE]=SC.[CREATEDDATE],
			[CREATEDBYID]=SC.[CREATEDBYID],
			[ANSWER_TEXT__C]=SC.[ANSWER_TEXT__C]
	WHEN NOT MATCHED THEN
    INSERT([SYSTEMMODSTAMP]
      ,[QUESTION__C]
      ,[QUESTIONTEXT__C]
      ,[OWNERID]
      ,[NAME]
      ,[LASTMODIFIEDDATE]
      ,[LASTMODIFIEDBYID]
      ,[ISDELETED]
      ,[ID]
      ,[DONATION__C]
      ,[CREATEDDATE]
      ,[CREATEDBYID]
      ,[ANSWER_TEXT__C]) 
	  VALUES ([SYSTEMMODSTAMP]
      ,[QUESTION__C]
      ,[QUESTIONTEXT__C]
      ,[OWNERID]
      ,[NAME]
      ,[LASTMODIFIEDDATE]
      ,[LASTMODIFIEDBYID]
      ,[ISDELETED]
      ,[ID]
      ,[DONATION__C]
      ,[CREATEDDATE]
      ,[CREATEDBYID]
      ,[ANSWER_TEXT__C])
	     
OUTPUT $action INTO #tmp_RecordCount;

SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #tmp_RecordCount

SELECT DONATION__C INTO #ModifiedDonationQuestions FROM EDW.dbo.CharityDonorResponse 
WHERE SYSTEMMODSTAMP BETWEEN GETDATE()-5 and GETDATE()
							
UPDATE CDR Set IsMostRecent = 0
FROM EDW.dbo.CharityDonorResponse CDR
INNER JOIN #ModifiedDonationQuestions MDQ WITH (NOLOCK)
ON CDR.DONATION__C = MDQ.DONATION__C

UPDATE CDR Set IsMostRecent = 1
FROM EDW.dbo.CharityDonorResponse CDR
INNER JOIN #ModifiedDonationQuestions MDQ WITH (NOLOCK)
ON CDR.DONATION__C = MDQ.DONATION__C
INNER JOIN (SELECT Question__C,Donation__C, MAX(SYSTEMMODSTAMP) as LastModified 
			FROM EDW.dbo.CharityDonorResponse 
			GROUP BY Question__C,Donation__C) MAXDQ
ON CDR.DONATION__C = MAXDQ.DONATION__C AND CDR.SYSTEMMODSTAMP = MAXDQ.LastModified AND CDR.QUESTION__C = MAXDQ.QUESTION__C	

/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC EDW.[dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		COMMIT TRAN

		DROP TABLE #ModifiedDonationQuestions		
		
		RETURN 0
	END TRY     

	BEGIN CATCH
		SET NOCOUNT OFF
		IF @@TRANCOUNT > 0 
			ROLLBACK TRAN
		EXEC edw.[dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ErrorCode = @@ERROR, @UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

		DROP TABLE #ModifiedDonationQuestions		

		RETURN -1
	END CATCH
	END


