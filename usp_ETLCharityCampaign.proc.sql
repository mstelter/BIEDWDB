/* 
USE EDW
GO 
==============================================================================
Author		: Matt Stelter
Create date	: 2/28/2013
Description	: Populate the EDW Charity Campaign tables from TEMP Charity tables	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number
5/21/2013	Mstelter			Added Campaign_800_Number__c
11/2/2016	Mstelter			Added code to populate CharityQuestionsAnswers - Did it here because this is the last SP to run in Charity load
==============================================================================
Usage Example:
Exec EDW.dbo.usp_ETLCharityCampaign
==============================================================================

TRUNCATE TABLE EDW.dbo.CharityCampaign

*/

CREATE PROCEDURE [dbo].[usp_ETLCharityCampaign]
AS     
BEGIN
DECLARE @ETLLoadID INT,
		@InsertRowCount INT,
		@UpdateRowCount INT

----Run the Audit begin
EXEC @ETLLoadID = EDW.[dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLCharityCampaign'

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

SET @TableName = 'Staging.dbo.Charity_SF_Campaign'

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
    MERGE EDW.[dbo].[CharityCampaign] AS CC
    USING Staging.[dbo].[Charity_SF_Campaign] AS SC
          ON CC.[CampaignId] = SC.[Id]
    WHEN MATCHED THEN 
        UPDATE SET 
			[ActualCost]=SC.[ActualCost],
			[AmountAllOpportunities]=SC.[AmountAllOpportunities],
			[AmountWonOpportunities]=SC.[AmountWonOpportunities],
			[BudgetedCost]=SC.[BudgetedCost],
			[CampaignMemberRecordTypeId]=SC.[CampaignMemberRecordTypeId],
			[Charity__c]=SC.[Charity__c],
			[CreatedById]=SC.[CreatedById],
			[CreatedDate]=SC.[CreatedDate],
			[Description]=SC.[Description],
			[EndDate]=SC.[EndDate],
			[ExpectedResponse]=SC.[ExpectedResponse],
			[ExpectedRevenue]=SC.[ExpectedRevenue],
			[Five9__Five9CallNow__c]=SC.[Five9__Five9CallNow__c],
			[Five9__Five9list__c]=SC.[Five9__Five9list__c],
			[IsActive]=SC.[IsActive],
			[IsDeleted]=SC.[IsDeleted],
			[LastActivityDate]=SC.[LastActivityDate],
			[LastModifiedById]=SC.[LastModifiedById],
			[LastModifiedDate]=SC.[LastModifiedDate],
			[Lead__c]=SC.[Lead__c],
			[Name]=SC.[Name],
			[NumberOfContacts]=SC.[NumberOfContacts],
			[NumberOfConvertedLeads]=SC.[NumberOfConvertedLeads],
			[NumberOfLeads]=SC.[NumberOfLeads],
			[NumberOfOpportunities]=SC.[NumberOfOpportunities],
			[NumberOfResponses]=SC.[NumberOfResponses],
			[NumberOfWonOpportunities]=SC.[NumberOfWonOpportunities],
			[NumberSent]=SC.[NumberSent],
			[OwnerId]=SC.[OwnerId],
			[ParentId]=SC.[ParentId],
			[StartDate]=SC.[StartDate],
			[Status]=SC.[Status],
			[SubLead__c]=SC.[SubLead__c],
			[SystemModstamp]=SC.[SystemModstamp],
			[Type]=SC.[Type],
			[X800_Number__c]=SC.[X800_Number__c],
			BIUpdateDatetime = @BIUpdateDatetime,
			Campaign_800_Number__c = SC.Campaign_800_Number__c
	WHEN NOT MATCHED THEN
    INSERT([ActualCost]
,[AmountAllOpportunities]
,[AmountWonOpportunities]
,[BudgetedCost]
,[CampaignMemberRecordTypeId]
,[Charity__c]
,[CreatedById]
,[CreatedDate]
,[Description]
,[EndDate]
,[ExpectedResponse]
,[ExpectedRevenue]
,[Five9__Five9CallNow__c]
,[Five9__Five9list__c]
,[CampaignId]
,[IsActive]
,[IsDeleted]
,[LastActivityDate]
,[LastModifiedById]
,[LastModifiedDate]
,[Lead__c]
,[Name]
,[NumberOfContacts]
,[NumberOfConvertedLeads]
,[NumberOfLeads]
,[NumberOfOpportunities]
,[NumberOfResponses]
,[NumberOfWonOpportunities]
,[NumberSent]
,[OwnerId]
,[ParentId]
,[StartDate]
,[Status]
,[SubLead__c]
,[SystemModstamp]
,[Type]
,[X800_Number__c]
,[BIUpdateDatetime]
,Campaign_800_Number__c) 
	  VALUES ([ActualCost]
,[AmountAllOpportunities]
,[AmountWonOpportunities]
,[BudgetedCost]
,[CampaignMemberRecordTypeId]
,[Charity__c]
,[CreatedById]
,[CreatedDate]
,[Description]
,[EndDate]
,[ExpectedResponse]
,[ExpectedRevenue]
,[Five9__Five9CallNow__c]
,[Five9__Five9list__c]
,[Id]
,[IsActive]
,[IsDeleted]
,[LastActivityDate]
,[LastModifiedById]
,[LastModifiedDate]
,[Lead__c]
,[Name]
,[NumberOfContacts]
,[NumberOfConvertedLeads]
,[NumberOfLeads]
,[NumberOfOpportunities]
,[NumberOfResponses]
,[NumberOfWonOpportunities]
,[NumberSent]
,[OwnerId]
,[ParentId]
,[StartDate]
,[Status]
,[SubLead__c]
,[SystemModstamp]
,[Type]
,[X800_Number__c]
,@BIUpdateDatetime
,Campaign_800_Number__c)
	     
OUTPUT $action INTO #tmp_RecordCount;

SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #tmp_RecordCount

/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC EDW.[dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		COMMIT TRAN

BEGIN TRAN

Truncate Table dbo.CharityQuestionsAnswers

INSERT INTO [dbo].[CharityQuestionsAnswers]
           ([DONATION__C]
           ,[Q1]
           ,[A1]
           ,[Q2]
           ,[A2]
           ,[Q3]
           ,[A3]
           ,[Q4]
           ,[A4]
           ,[Q5]
           ,[A5]
           ,[Q6]
           ,[A6]
           ,[Q7]
           ,[A7])
SELECT Q.DONATION__C,Q1,A1,Q2,A2,Q3,A3,Q4,A4,Q5,A5,Q6,A6,Q7,A7 
--INTO dbo.CharityQuestionsAnswers
FROM 
							(Select DONATION__C, [1] AS Q1, [2] AS Q2, [3] AS Q3, [4] AS Q4,[5] AS Q5,[6] AS Q6,[7] AS Q7
							FROM 
							(SELECT ROW_NUMBER() OVER(PARTITION BY CD.CharityDonationsId ORDER BY CQ.[NAME]) AS Row,
							CD.CharityDonationsId as DONATION__C, QUESTION_TEXT__C AS QUESTIONTEXT__C
							FROM dbo.CharityQuestion CQ WITH (NOLOCK)
						    INNER JOIN dbo.CharityEntities  ce WITH (NOLOCK)
								on CQ.charity__C = ce.ParentId						
							INNER JOIN dbo.CharityDonations CD WITH (NOLOCK)
								ON CE.CharityEntitiesId = CD.Charity__c
							LEFT OUTER JOIN (SELECT ID,DONATION__C,QUESTION__C FROM dbo.CharityDonorResponse WITH (NOLOCK) WHERE ISNULL(IsMostRecent,1) = 1) CDR
								ON CQ.ID = CDR.QUESTION__C and CD.CharityDonationsId = CDR.DONATION__C
							WHERE CQ.ISDELETED = 'false' or CDR.ID IS NOT NULL) As Temp
							PIVOT
							(max(QUESTIONTEXT__C) 
								FOR Row In([1], [2], [3], [4],[5],[6],[7])) As pt) Q
							LEFT OUTER JOIN 
							(Select DONATION__C, [1] AS A1, [2] AS A2, [3] AS A3, [4] AS A4,[5] AS A5,[6] AS A6,[7] AS A7
							FROM 
							(SELECT ROW_NUMBER() OVER(PARTITION BY CD.CharityDonationsId ORDER BY CQ.[NAME]) AS Row,DONATION__C, ANSWER_TEXT__C 
							--FROM dbo.CharityDonorResponse WITH (NOLOCK) WHERE ISNULL(IsMostRecent,1) = 1
							FROM dbo.CharityQuestion CQ WITH (NOLOCK)
						    INNER JOIN dbo.CharityEntities  ce WITH (NOLOCK)
								on CQ.charity__C = ce.ParentId						
							INNER JOIN dbo.CharityDonations CD WITH (NOLOCK)
								ON CE.CharityEntitiesId = CD.Charity__c
							INNER JOIN (SELECT ID,DONATION__C,QUESTION__C, ANSWER_TEXT__C FROM dbo.CharityDonorResponse WITH (NOLOCK) WHERE ISNULL(IsMostRecent,1) = 1) CDR
								ON CQ.ID = CDR.QUESTION__C and CD.CharityDonationsId = CDR.DONATION__C
							) As Temp
							PIVOT
							(max(ANSWER_TEXT__C) 
								FOR Row In([1], [2], [3], [4],[5],[6],[7])) As pt) A	
							ON Q.DONATION__C = A.DONATION__C

		COMMIT TRAN

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

		RETURN -1
	END CATCH
	END