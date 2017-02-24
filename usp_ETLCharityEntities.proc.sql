
/* 
USE EDW
GO 
==============================================================================
Author		: Matt Stelter
Create date	: 2/28/2013
Description	: Populate the EDW Charity Entities tables from TEMP Charity tables	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number
3/25/2013	MStelter			Added code for Salvage_Provider_ID__c > INT
4/5/2013	MStelter			Changed to use Salvage_Provider_ID__C instead of SP_ID__c
4/22/2013	MStelter			Changed to use PATINDEX('%[^0-9]%',Salvage_Provider_ID__c) = 0 instead of IsNumeric (Salvage_Provider_ID__c)= 1
11/4/2014	MStelter			Added Admin_Address_1__c,Admin_Address_2__c,Admin_City__c,Admin_Contact__c,Admin_Email__c,Admin_Phone__c,Admin_State__c,
									Admin_Zip_Code__c,Email_Sale_Reports__c
05/7/2015	BKutnick			Added Settled_In_ASAP__c
07/27/2016  Pratyusha Koduru    Added GP_ID__c
02/02/2017  Pratyusha Koduru    Added NPO_Affiliation__c
==============================================================================
Usage Example:
Exec EDW.dbo.usp_ETLCharityEntities
==============================================================================

TRUNCATE TABLE EDW.dbo.CharityEntities

*/

CREATE PROCEDURE [dbo].[usp_ETLCharityEntities]
AS     
BEGIN
DECLARE @ETLLoadID INT,
		--@ExtractRowCount_sp INT,
		@InsertRowCount INT,
		@UpdateRowCount INT
		--,@DeleteRowCount INT

----Run the Audit begin
EXEC @ETLLoadID = EDW.[dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLCharity'

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

SET @TableName = 'Staging.dbo.Charity_SF_Entities'

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
/************Merge Entities****************************************/
/********************************************************************/
UPDATE Staging.[dbo].[Charity_SF_Entities] SET Salvage_Provider_ID__c = 0 
--SELECT * FROM Staging.[dbo].[Charity_SF_Entities]
WHERE PATINDEX('%[^0-9]%',Salvage_Provider_ID__c) = 1 

UPDATE Staging.[dbo].[Charity_SF_Entities] SET Salvage_Provider_ID__c = 0 
--SELECT * FROM Staging.[dbo].[Charity_SF_Entities]
WHERE --Salvage_Provider_ID__c = 1 AND 
	--PATINDEX('%[^0-9]%',Salvage_Provider_ID__c) = 0 AND 
	CONVERT(BIGINT,Salvage_Provider_ID__c) > 2147483647

    MERGE EDW.[dbo].[CharityEntities] AS CE
    USING Staging.[dbo].[Charity_SF_Entities] AS SE
          ON CE.[CharityEntitiesId] = SE.[Id]
    WHEN MATCHED THEN 
        UPDATE SET 
			[CharityEntitiesId] = SE.[Id],
			[AccountNumber] = SE.[AccountNumber],
			[Active__c] = SE.[Active__c],
			[Advertising_Participant__c] = SE.[Advertising_Participant__c],
			[Advertising_Rate__c] = SE.[Advertising_Rate__c],
			[Advertising_Start_Date__c] = SE.[Advertising_Start_Date__c],
			[Affiliate_Manager_Name__c] = SE.[Affiliate_Manager_Name__c],
			[Affiliate_Number__c] = SE.[Affiliate_Number__c],
			[Allows_Drop_Off__c] = SE.[Allows_Drop_Off__c],
			[Alpha_Id__c] = SE.[Alpha_Id__c],
			[AnnualRevenue] = SE.[AnnualRevenue],
			[Best_Number_to_Call__c] = SE.[Best_Number_to_Call__c],
			[Best_Time_to_Call__c] = SE.[Best_Time_to_Call__c],
			[BillingCity] = SE.[BillingCity],
			[BillingCountry] = SE.[BillingCountry],
			[BillingPostalCode] = SE.[BillingPostalCode],
			[BillingState] = SE.[BillingState],
			[BillingStreet] = SE.[BillingStreet],
			[Branch_Manager_Name__c] = SE.[Branch_Manager_Name__c],
			[Branch_Number__c] = SE.[Branch_Number__c],
			[Cancel_Survey__c] = SE.[Cancel_Survey__c],
			[Charity_Abbreviation__c] = SE.[Charity_Abbreviation__c],
			[Charity_Chapter_Code__c] = SE.[Charity_Chapter_Code__c],
			[Charity_Code__c] = SE.[Charity_Code__c],
			[Charity_of_Choice__c] = SE.[Charity_of_Choice__c],
			[Contract_Number__c] = SE.[Contract_Number__c],
			[County__c] = SE.[County__c],
			[CreatedById] = SE.[CreatedById],
			[CreatedDate] = SE.[CreatedDate],
			[Description] = SE.[Description],
			[Donor_Mobile_Phone__c] = SE.[Donor_Mobile_Phone__c],
			[Donor_Primary_Phone__c] = SE.[Donor_Primary_Phone__c],
			[Donor_Work_Phone__c] = SE.[Donor_Work_Phone__c],
			[Donor_Work_Phone_Extension__c] = SE.[Donor_Work_Phone_Extension__c],
			[Drop_Off_Gift__c] = SE.[Drop_Off_Gift__c],
			[Drop_Off_Incentive_Amount__c] = SE.[Drop_Off_Incentive_Amount__c],
			[Drop_Off_Start_Date__c] = SE.[Drop_Off_Start_Date__c],
			[Employer_Tracking__c] = SE.[Employer_Tracking__c],
			[Employer_Tracking_Start_Date__c] = SE.[Employer_Tracking_Start_Date__c],
			[Fax] = SE.[Fax],
			[Federal_Tax_ID__c] = SE.[Federal_Tax_ID__c],
			[FirstName] = SE.[FirstName],
			[Honor_InMemory__c] = SE.[Honor_InMemory__c],
			[Industry] = SE.[Industry],
			[IsDeleted] = SE.[IsDeleted],
			[IsPersonAccount] = SE.[IsPersonAccount],
			[LastActivityDate] = SE.[LastActivityDate],
			[LastModifiedById] = SE.[LastModifiedById],
			[LastModifiedDate] = SE.[LastModifiedDate],
			[LastName] = SE.[LastName],
			[Loaded_From_Alpha__c] = SE.[Loaded_From_Alpha__c],
			[Logo__c] = SE.[Logo__c],
			[Marketing_Rebate_Participant__c] = SE.[Marketing_Rebate_Participant__c],
			[Marketing_Rebate_Rate__c] = SE.[Marketing_Rebate_Rate__c],
			[Marketing_Rebate_Start_Date__c] = SE.[Marketing_Rebate_Start_Date__c],
			[Marketing_Source_Name__c] = SE.[Marketing_Source_Name__c],
			[MasterRecordId] = SE.[MasterRecordId],
			[Mission_Statement__c] = SE.[Mission_Statement__c],
			[Name] = SE.[Name],
			[Needed_Vehicle_Detail__c] = SE.[Needed_Vehicle_Detail__c],
			[Needed_Vehicle_Fee__c] = SE.[Needed_Vehicle_Fee__c],
			[NumberOfEmployees] = SE.[NumberOfEmployees],
			[OwnerId] = SE.[OwnerId],
			[Ownership] = SE.[Ownership],
			[ParentId] = SE.[ParentId],
			[PersonAssistantName] = SE.[PersonAssistantName],
			[PersonAssistantPhone] = SE.[PersonAssistantPhone],
			[PersonBirthdate] = SE.[PersonBirthdate],
			[PersonContactId] = SE.[PersonContactId],
			[PersonDepartment] = SE.[PersonDepartment],
			[PersonDoNotCall] = SE.[PersonDoNotCall],
			[PersonEmail] = SE.[PersonEmail],
			[PersonEmailBouncedDate] = SE.[PersonEmailBouncedDate],
			[PersonEmailBouncedReason] = SE.[PersonEmailBouncedReason],
			[PersonHasOptedOutOfEmail] = SE.[PersonHasOptedOutOfEmail],
			[PersonHasOptedOutOfFax] = SE.[PersonHasOptedOutOfFax],
			[PersonHomePhone] = SE.[PersonHomePhone],
			[PersonLastCURequestDate] = SE.[PersonLastCURequestDate],
			[PersonLastCUUpdateDate] = SE.[PersonLastCUUpdateDate],
			[PersonLeadSource] = SE.[PersonLeadSource],
			[PersonMailingCity] = SE.[PersonMailingCity],
			[PersonMailingCountry] = SE.[PersonMailingCountry],
			[PersonMailingPostalCode] = SE.[PersonMailingPostalCode],
			[PersonMailingState] = SE.[PersonMailingState],
			[PersonMailingStreet] = SE.[PersonMailingStreet],
			[PersonMobilePhone] = SE.[PersonMobilePhone],
			[PersonOtherCity] = SE.[PersonOtherCity],
			[PersonOtherCountry] = SE.[PersonOtherCountry],
			[PersonOtherPhone] = SE.[PersonOtherPhone],
			[PersonOtherPostalCode] = SE.[PersonOtherPostalCode],
			[PersonOtherState] = SE.[PersonOtherState],
			[PersonOtherStreet] = SE.[PersonOtherStreet],
			[PersonTitle] = SE.[PersonTitle],
			[Phone] = SE.[Phone],
			[Pick_Up_Times__c] = SE.[Pick_Up_Times__c],
			[Primary_SubCharity__c] = SE.[Primary_SubCharity__c],
			[Print_Receipt_Option__c] = SE.[Print_Receipt_Option__c],
			[Rate_Sheet_Number__c] = SE.[Rate_Sheet_Number__c],
			[Rating] = SE.[Rating],
			[RecordTypeId] = SE.[RecordTypeId],
			[Referral_Program_Fee__c] = SE.[Referral_Program_Fee__c],
			[Reject_Amount__c] = SE.[Reject_Amount__c],
			[Reject_Level__c] = SE.[Reject_Level__c],
			[Salutation] = SE.[Salutation],
			[Salvage_Provider_ID__c] = CASE WHEN ISNUMERIC(ISNULL(SE.Salvage_Provider_ID__c,0)) = 0 THEN 0 ELSE SE.[Salvage_Provider_ID__c] END,
			[ShippingCity] = SE.[ShippingCity],
			[ShippingCountry] = SE.[ShippingCountry],
			[ShippingPostalCode] = SE.[ShippingPostalCode],
			[ShippingState] = SE.[ShippingState],
			[ShippingStreet] = SE.[ShippingStreet],
			[Sic] = SE.[Sic],
			[Signature_Name__c] = SE.[Signature_Name__c],
			[Signature_Title__c] = SE.[Signature_Title__c],
			[Site] = SE.[Site],
			[SP_ID__c] = CASE WHEN ISNUMERIC(ISNULL(SE.Salvage_Provider_ID__c,0)) = 0 THEN 0 ELSE SE.[Salvage_Provider_ID__c] END,
			[Special_programs__c] = SE.[Special_programs__c],
			[Subsidy_Amount__c] = SE.[Subsidy_Amount__c],
			[Subsidy_Level__c] = SE.[Subsidy_Level__c],
			[Subsidy_Start_Date__c] = SE.[Subsidy_Start_Date__c],
			[Success_Survey__c] = SE.[Success_Survey__c],
			[SystemModstamp] = SE.[SystemModstamp],
			[Territories__c] = SE.[Territories__c],
			[Thank_You_Letter_Content__c] = SE.[Thank_You_Letter_Content__c],
			[TickerSymbol] = SE.[TickerSymbol],
			[Type] = SE.[Type],
			[Umbrella_Charity__c] = SE.[Umbrella_Charity__c],
			[Website] = SE.[Website],
			[Welcome_Pack_Info__c] = SE.[Welcome_Pack_Info__c],
			[X1_Car_1_Difference_Benefactor__c] = SE.[X1_Car_1_Difference_Benefactor__c],
			[X1_Car_1_Difference_Start_Date__c] = SE.[X1_Car_1_Difference_Start_Date__c],
			BIUpdateDatetime = null,
			OLD_ASAP_SPID = SE.[SP_ID__c],
			Admin_Address_1__c=SE.Admin_Address_1__c,
			Admin_Address_2__c=SE.Admin_Address_2__c,
			Admin_City__c=SE.Admin_City__c,
			Admin_Contact__c=SE.Admin_Contact__c,
			Admin_Email__c=SE.Admin_Email__c,
			Admin_Phone__c=SE.Admin_Phone__c,
			Admin_State__c=SE.Admin_State__c,
			Admin_Zip_Code__c=SE.Admin_Zip_Code__c,
			Email_Sale_Reports__c=CASE WHEN SE.Email_Sale_Reports__c = 'TRUE' THEN 1 ELSE 0 END,
			Settled_In_ASAP__c=CASE WHEN ISDATE(SE.Settled_In_ASAP_c)=1 THEN SE.Settled_In_ASAP_c ELSE NULL END,
			GP_ID__c=SE.GP_ID__c,
			NPO_Affiliation__c=SE.NPO_Affiliation__c
	WHEN NOT MATCHED THEN
    INSERT([CharityEntitiesId],
			[AccountNumber],
			[Active__c],
			[Advertising_Participant__c],
			[Advertising_Rate__c],
			[Advertising_Start_Date__c],
			[Affiliate_Manager_Name__c],
			[Affiliate_Number__c],
			[Allows_Drop_Off__c],
			[Alpha_Id__c],
			[AnnualRevenue],
			[Best_Number_to_Call__c],
			[Best_Time_to_Call__c],
			[BillingCity],
			[BillingCountry],
			[BillingPostalCode],
			[BillingState],
			[BillingStreet],
			[Branch_Manager_Name__c],
			[Branch_Number__c],
			[Cancel_Survey__c],
			[Charity_Abbreviation__c],
			[Charity_Chapter_Code__c],
			[Charity_Code__c],
			[Charity_of_Choice__c],
			[Contract_Number__c],
			[County__c],
			[CreatedById],
			[CreatedDate],
			[Description],
			[Donor_Mobile_Phone__c],
			[Donor_Primary_Phone__c],
			[Donor_Work_Phone__c],
			[Donor_Work_Phone_Extension__c],
			[Drop_Off_Gift__c],
			[Drop_Off_Incentive_Amount__c],
			[Drop_Off_Start_Date__c],
			[Employer_Tracking__c],
			[Employer_Tracking_Start_Date__c],
			[Fax],
			[Federal_Tax_ID__c],
			[FirstName],
			[Honor_InMemory__c],
			[Industry],
			[IsDeleted],
			[IsPersonAccount],
			[LastActivityDate],
			[LastModifiedById],
			[LastModifiedDate],
			[LastName],
			[Loaded_From_Alpha__c],
			[Logo__c],
			[Marketing_Rebate_Participant__c],
			[Marketing_Rebate_Rate__c],
			[Marketing_Rebate_Start_Date__c],
			[Marketing_Source_Name__c],
			[MasterRecordId],
			[Mission_Statement__c],
			[Name],
			[Needed_Vehicle_Detail__c],
			[Needed_Vehicle_Fee__c],
			[NumberOfEmployees],
			[OwnerId],
			[Ownership],
			[ParentId],
			[PersonAssistantName],
			[PersonAssistantPhone],
			[PersonBirthdate],
			[PersonContactId],
			[PersonDepartment],
			[PersonDoNotCall],
			[PersonEmail],
			[PersonEmailBouncedDate],
			[PersonEmailBouncedReason],
			[PersonHasOptedOutOfEmail],
			[PersonHasOptedOutOfFax],
			[PersonHomePhone],
			[PersonLastCURequestDate],
			[PersonLastCUUpdateDate],
			[PersonLeadSource],
			[PersonMailingCity],
			[PersonMailingCountry],
			[PersonMailingPostalCode],
			[PersonMailingState],
			[PersonMailingStreet],
			[PersonMobilePhone],
			[PersonOtherCity],
			[PersonOtherCountry],
			[PersonOtherPhone],
			[PersonOtherPostalCode],
			[PersonOtherState],
			[PersonOtherStreet],
			[PersonTitle],
			[Phone],
			[Pick_Up_Times__c],
			[Primary_SubCharity__c],
			[Print_Receipt_Option__c],
			[Rate_Sheet_Number__c],
			[Rating],
			[RecordTypeId],
			[Referral_Program_Fee__c],
			[Reject_Amount__c],
			[Reject_Level__c],
			[Salutation],
			[Salvage_Provider_ID__c],
			[ShippingCity],
			[ShippingCountry],
			[ShippingPostalCode],
			[ShippingState],
			[ShippingStreet],
			[Sic],
			[Signature_Name__c],
			[Signature_Title__c],
			[Site],
			[SP_ID__c],
			[Special_programs__c],
			[Subsidy_Amount__c],
			[Subsidy_Level__c],
			[Subsidy_Start_Date__c],
			[Success_Survey__c],
			[SystemModstamp],
			[Territories__c],
			[Thank_You_Letter_Content__c],
			[TickerSymbol],
			[Type],
			[Umbrella_Charity__c],
			[Website],
			[Welcome_Pack_Info__c],
			[X1_Car_1_Difference_Benefactor__c],
			[X1_Car_1_Difference_Start_Date__c],
			BIUpdateDatetime,
			OLD_ASAP_SPID,
			Admin_Address_1__c,Admin_Address_2__c,Admin_City__c,Admin_Contact__c,Admin_Email__c,Admin_Phone__c,Admin_State__c,
									Admin_Zip_Code__c,Email_Sale_Reports__c, Settled_In_ASAP__c,GP_ID__c,NPO_Affiliation__c) 
	  VALUES ([Id],
				[AccountNumber],
				[Active__c],
				[Advertising_Participant__c],
				[Advertising_Rate__c],
				[Advertising_Start_Date__c],
				[Affiliate_Manager_Name__c],
				[Affiliate_Number__c],
				[Allows_Drop_Off__c],
				[Alpha_Id__c],
				[AnnualRevenue],
				[Best_Number_to_Call__c],
				[Best_Time_to_Call__c],
				[BillingCity],
				[BillingCountry],
				[BillingPostalCode],
				[BillingState],
				[BillingStreet],
				[Branch_Manager_Name__c],
				[Branch_Number__c],
				[Cancel_Survey__c],
				[Charity_Abbreviation__c],
				[Charity_Chapter_Code__c],
				[Charity_Code__c],
				[Charity_of_Choice__c],
				[Contract_Number__c],
				[County__c],
				[CreatedById],
				[CreatedDate],
				[Description],
				[Donor_Mobile_Phone__c],
				[Donor_Primary_Phone__c],
				[Donor_Work_Phone__c],
				[Donor_Work_Phone_Extension__c],
				[Drop_Off_Gift__c],
				[Drop_Off_Incentive_Amount__c],
				[Drop_Off_Start_Date__c],
				[Employer_Tracking__c],
				[Employer_Tracking_Start_Date__c],
				[Fax],
				[Federal_Tax_ID__c],
				[FirstName],
				[Honor_InMemory__c],
				[Industry],
				[IsDeleted],
				[IsPersonAccount],
				[LastActivityDate],
				[LastModifiedById],
				[LastModifiedDate],
				[LastName],
				[Loaded_From_Alpha__c],
				[Logo__c],
				[Marketing_Rebate_Participant__c],
				[Marketing_Rebate_Rate__c],
				[Marketing_Rebate_Start_Date__c],
				[Marketing_Source_Name__c],
				[MasterRecordId],
				[Mission_Statement__c],
				[Name],
				[Needed_Vehicle_Detail__c],
				[Needed_Vehicle_Fee__c],
				[NumberOfEmployees],
				[OwnerId],
				[Ownership],
				[ParentId],
				[PersonAssistantName],
				[PersonAssistantPhone],
				[PersonBirthdate],
				[PersonContactId],
				[PersonDepartment],
				[PersonDoNotCall],
				[PersonEmail],
				[PersonEmailBouncedDate],
				[PersonEmailBouncedReason],
				[PersonHasOptedOutOfEmail],
				[PersonHasOptedOutOfFax],
				[PersonHomePhone],
				[PersonLastCURequestDate],
				[PersonLastCUUpdateDate],
				[PersonLeadSource],
				[PersonMailingCity],
				[PersonMailingCountry],
				[PersonMailingPostalCode],
				[PersonMailingState],
				[PersonMailingStreet],
				[PersonMobilePhone],
				[PersonOtherCity],
				[PersonOtherCountry],
				[PersonOtherPhone],
				[PersonOtherPostalCode],
				[PersonOtherState],
				[PersonOtherStreet],
				[PersonTitle],
				[Phone],
				[Pick_Up_Times__c],
				[Primary_SubCharity__c],
				[Print_Receipt_Option__c],
				[Rate_Sheet_Number__c],
				[Rating],
				[RecordTypeId],
				[Referral_Program_Fee__c],
				[Reject_Amount__c],
				[Reject_Level__c],
				[Salutation],
				CASE WHEN ISNUMERIC(ISNULL(SE.Salvage_Provider_ID__c,0)) = 0 THEN 0 ELSE SE.[Salvage_Provider_ID__c] END,
				[ShippingCity],
				[ShippingCountry],
				[ShippingPostalCode],
				[ShippingState],
				[ShippingStreet],
				[Sic],
				[Signature_Name__c],
				[Signature_Title__c],
				[Site],
				CASE WHEN ISNUMERIC(ISNULL(SE.Salvage_Provider_ID__c,0)) = 0 THEN 0 ELSE SE.[Salvage_Provider_ID__c] END,
				[Special_programs__c],
				[Subsidy_Amount__c],
				[Subsidy_Level__c],
				[Subsidy_Start_Date__c],
				[Success_Survey__c],
				[SystemModstamp],
				[Territories__c],
				[Thank_You_Letter_Content__c],
				[TickerSymbol],
				[Type],
				[Umbrella_Charity__c],
				[Website],
				[Welcome_Pack_Info__c],
				[X1_Car_1_Difference_Benefactor__c],
				[X1_Car_1_Difference_Start_Date__c],
				@BIUpdateDatetime,
				[SP_ID__c],
				Admin_Address_1__c,
				Admin_Address_2__c,
				Admin_City__c,
				Admin_Contact__c,
				Admin_Email__c,
				Admin_Phone__c,
				Admin_State__c,
				Admin_Zip_Code__c,
				CASE WHEN Email_Sale_Reports__c = 'TRUE' THEN 1 ELSE 0 END,
				CASE WHEN ISDATE(SE.Settled_In_ASAP_c)=1 THEN SE.Settled_In_ASAP_c ELSE NULL END,
				GP_ID__c,
				NPO_Affiliation__c)
	     
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



GO


