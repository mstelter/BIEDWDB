/* 
USE EDW
GO 
==============================================================================
Author		: Matt Stelter
Create date	: 2/28/2013
Description	: Populate the EDW Charity Donations tables from TEMP Charity tables	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number
3/13/2013	MStelter			Added Claim__c
3/28/2013	MStelter			Added join to branch for ASAP_Salvage_ID
4/5/2013	MStelter			Changed to use Salvage_Provider_ID__c from staging.Charity_SF_Entities instead of SP_ID__c
4/11/2013	MStelter			Code to NULL Salvage ID if Stock Number is null
9/25/2014	MStelter			Added MarketArea__c and skipped Do_Not_Load_to_BI__c = 1 records
6/23/2015	MStelter			Added Channel,Creative,Venue
11/8/2016	MStelter			Add fix to change BigInt Odometer to 0
==============================================================================
Usage Example:
Exec EDW.dbo.usp_ETLCharityDonations
==============================================================================

TRUNCATE TABLE EDW.dbo.CharityDonations

*/

CREATE PROCEDURE [dbo].[usp_ETLCharityDonations]
AS     
BEGIN
DECLARE @ETLLoadID INT,
		@InsertRowCount INT,
		@UpdateRowCount INT

----Run the Audit begin
EXEC @ETLLoadID = EDW.[dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLCharityDonations'

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

SET @TableName = 'Staging.dbo.Charity_SF_Donations'

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
				CASE WHEN RIGHT(['+@FieldName+'],1) = ''Z'' 
					THEN LEFT(['+@FieldName+
                  '],LEN(['+
                             @FieldName
                             +'])-1) 
					ELSE ['+
                             @FieldName+'] 
					END'

                  EXEC (@SQL)
              END
--SELECT @FieldName
            FETCH NEXT FROM TableFields INTO @FieldName
        END

      CLOSE TableFields;

      DEALLOCATE TableFields;

BEGIN TRAN
/********************************************************************/
/************Merge Donations****************************************/
/********************************************************************/
DELETE FROM Staging.[dbo].[Charity_SF_Donations] WHERE Do_Not_Load_to_BI__c = 'TRUE'

UPDATE Staging.[dbo].[Charity_SF_Donations] SET [ACV__c] = CASE WHEN RIGHT([ACV__c],1) = '"' THEN LEFT([ACV__c],LEN([ACV__c])-1) ELSE [ACV__c] END
UPDATE [Staging].[dbo].[Charity_SF_Donations] SET [Donation_Closed_Date__c]=REPLACE([Donation_Closed_Date__c],'"','') where [Donation_Closed_Date__c] like '%"%'
UPDATE [Staging].[dbo].[Charity_SF_Donations] SET [Donation_Closed_Date__c]= NULL  where ISDATE(ISNULL([Donation_Closed_Date__c],'12/31/2999')) = 0
UPDATE Staging.[dbo].[Charity_SF_Donations] SET [YEAR__C] = CASE WHEN LEFT([YEAR__C],1) = '"' THEN RIGHT([YEAR__C],LEN([YEAR__C])-1) ELSE [YEAR__C] END

UPDATE Staging.[dbo].[Charity_SF_Donations] SET [ODOMETER__C] = 0 WHERE LEN([ODOMETER__C]) > 9

UPDATE Donations SET SPID = Entities.SP_ID__c
--SELECT * 
FROM Staging.dbo.[Charity_SF_Donations] Donations WITH (NOLOCK)
inner join EDW.dbo.CharityEntities Entities WITH (NOLOCK)
ON Donations.CHARITY__C = Entities.[CharityEntitiesId]

    MERGE EDW.[dbo].[CharityDonations] AS CD
    USING Staging.[dbo].[Charity_SF_Donations] AS SCD
          ON CD.[CharityDonationsId] = SCD.[Id]
    WHEN MATCHED THEN 
        UPDATE SET 
			[CharityDonationsId]=SCD.[Id],
			[AccountId]=SCD.[AccountId],
			[ACV__c]=SCD.[ACV__c],
			[Affiliate__c]=SCD.[Affiliate__c],
			[Affiliate_ID__c]=SCD.[Affiliate_ID__c],
			[Affiliate_Name__c]=SCD.[Affiliate_Name__c],
			[Alpha_ID__c]=SCD.[Alpha_ID__c],
			[Alternate_CellPhone__c]=CASE WHEN LEN(SCD.[Alternate_CellPhone__c]) = 10 AND ISNUMERIC(SCD.[Alternate_CellPhone__c]) = 1 THEN  '('+SUBSTRING(SCD.[Alternate_CellPhone__c],1,3)+') '+SUBSTRING(SCD.[Alternate_CellPhone__c],4,3)+'-'+SUBSTRING(SCD.[Alternate_CellPhone__c],4,4) ELSE SCD.[Alternate_CellPhone__c] END,
			[Alternate_Contact__c]=SCD.[Alternate_Contact__c],
			[Alternate_Email__c]=SCD.[Alternate_Email__c],
			[Alternate_Phone__c]=CASE WHEN LEN(SCD.Alternate_Phone__c) = 10 AND ISNUMERIC(SCD.Alternate_Phone__c) = 1 THEN  '('+SUBSTRING(SCD.Alternate_Phone__c,1,3)+') '+SUBSTRING(SCD.Alternate_Phone__c,4,3)+'-'+SUBSTRING(SCD.Alternate_Phone__c,4,4) ELSE SCD.Alternate_Phone__c END,
			[Amount]=SCD.[Amount],
			[Amount_of_Check__c]=SCD.[Amount_of_Check__c],
			[ASAP_Last_Status_Received__c]=SCD.[ASAP_Last_Status_Received__c],
			[ASAP_Last_Status_Received_Date__c]=SCD.[ASAP_Last_Status_Received_Date__c],
			[Assignment_Date__c]=SCD.[CloseDate],--SCD.[Assignment_Date__c],
			[ASV__c]=SCD.[ASV__c],
			[ASV_Highest_Sales_Price__c]=SCD.[ASV_Highest_Sales_Price__c],
			[ASV_Lowest_Sales_Price__c]=SCD.[ASV_Lowest_Sales_Price__c],
			[ASV_Most_Recent_Sale_Date__c]=SCD.[ASV_Most_Recent_Sale_Date__c],
			[ASV_Oldest_Sale_Date__c]=SCD.[ASV_Oldest_Sale_Date__c],
			[ASV_Vehicle_Count__c]=SCD.[ASV_Vehicle_Count__c],
			[Auction_Date__c]=SCD.[Auction_Date__c],
			[Best_Pickup_Contact__c]=SCD.[Best_Pickup_Contact__c],
			[Body_Series__c]=SCD.[Body_Series__c],
			[Branch__c]=SCD.[Branch__c],
			[Branch_ID__c]=SCD.[Branch_ID__c],
			[Business_Name__c]=SCD.[Business_Name__c],
			[Business_or_Residence__c]=SCD.[Business_or_Residence__c],
			[CampaignId]=SCD.[CampaignId],
			[Cancel_Date__c]=SCD.[Cancel_Date__c],
			[Charity__c]=SCD.[Charity__c],
			[Checkin_Date__c]=SCD.[Checkin_Date__c],
			[Close_Code__c]=SCD.[Close_Code__c],
			[Close_Comment__c]=SCD.[Close_Comment__c],
			[Close_Date__c]=COALESCE(SCD.Donation_Closed_Date__c,SCD.Close_Date__c),
			[CloseDate]=SCD.[CloseDate],
			[Closest_Recycler__c]=SCD.[Closest_Recycler__c],
			[Color__c]=SCD.[Color__c],
			[Condition__c]=SCD.[Condition__c],
			[Condition_Comments__c]=SCD.[Condition_Comments__c],
			[Copy_of_Tow_Bill__c]=SCD.[Copy_of_Tow_Bill__c],
			[Country__c]=SCD.[Country__c],
			[CreatedById]=SCD.[CreatedById],
			[CreatedDate]=SCD.[CreatedDate],
			[Damaged_Areas_Detail__c]=SCD.[Damaged_Areas_Detail__c],
			[Description]=SCD.[Description],
			[Destination_Type__c]=SCD.[Destination_Type__c],
			[Discrepancy__c]=SCD.[Discrepancy__c],
			[Distance__c]=SCD.[Distance__c],
			[Do_Not_Email__c]=SCD.[Do_Not_Email__c],
			[Donation_State__c]=SCD.[Mailing_State__c], --SCD.[Donation_State__c],
			[Donation_Status__c]=SCD.[Donation_Status__c],
			[Donation_Stock_Number__c]=CASE WHEN ISNUMERIC(SCD.Donation_Stock_Number__c)=0 THEN NULL ELSE SCD.Donation_Stock_Number__c END, --SCD.[Donation_Stock_Number__c],
			[Donor_2_FirstName__c]=SCD.[Donor_2_FirstName__c],
			[Donor_2_LastName__c]=SCD.[Donor_2_LastName__c],
			--[Donor_Phone_Value__c]=SCD.[Donor_Phone_Value__c],
			[Employer_Name__c]=SCD.[Employer_Name__c],
			[Error_Code_Message__c]=SCD.[Error_Code_Message__c],
			[Estimated_ACV__c]=CAST(SCD.Estimated_ACV__c as FLOAT),--SCD.[Estimated_ACV__c],
			[Event__c]=SCD.[Event__c],
			[Event_City__c]=SCD.[Event_City__c],
			[Event_State__c]=SCD.[Event_State__c],
			[Event_Street__c]=SCD.[Event_Street__c],
			[EventPostalCode__c]=SCD.[EventPostalCode__c],
			[Excess_Mileage__c]=SCD.[Excess_Mileage__c],
			--[ExpectedRevenue]=SCD.[ExpectedRevenue],
			[Fiscal]=SCD.[Fiscal],
			[FiscalQuarter]=SCD.[FiscalQuarter],
			[FiscalYear]=SCD.[FiscalYear],
			[ForecastCategory]=SCD.[ForecastCategory],
			[ForecastCategoryName]=SCD.[ForecastCategoryName],
			[Has_Damaged_Areas__c]=SCD.[Has_Damaged_Areas__c],
			[Has_Missing_Parts__c]=SCD.[Has_Missing_Parts__c],
			[Has_Specialty_Options__c]=SCD.[Has_Specialty_Options__c],
			[Has_Subsidy__c]=SCD.[Has_Subsidy__c],
			[HasOpportunityLineItem]=SCD.[HasOpportunityLineItem],
			[Honorary__c]=SCD.[Honorary__c],
			[In_Memory_Of__c]=SCD.[In_Memory_Of__c],
			[Is_Blocked__c]=SCD.[Is_Blocked__c],
			[Is_Driveable__c]=SCD.[Is_Driveable__c],
			[Is_Key_with_Vehicle__c]=SCD.[Is_Key_with_Vehicle__c],
			[Is_Title_with_Vehicle__c]=SCD.[Is_Title_with_Vehicle__c],
			[Is_Towable__c]=SCD.[Is_Towable__c],
			[Is_Vehicle_On_Hold__c]=SCD.[Is_Vehicle_On_Hold__c],
			[IsClosed]=SCD.[IsClosed],
			[IsDeleted]=SCD.[IsDeleted],
			--[IsPrivate]=SCD.[IsPrivate],
			[IsWon]=SCD.[IsWon],
			[Last_on_Road__c]=SCD.[Last_on_Road__c],
			[LastActivityDate]=SCD.[LastActivityDate],
			[LastModifiedById]=SCD.[LastModifiedById],
			[LastModifiedDate]=SCD.[LastModifiedDate],
			[LeadSource]=SCD.LEAD_SOURCE__C,
			[Legacy_Information__c]=SCD.[Legacy_Information__c],
			[Legacy_Stock_Number__c]=SCD.[Legacy_Stock_Number__c],
			[License_Plate_Number__c]=SCD.[License_Plate_Number__c],
			[Limit__c]=SCD.[Limit__c],
			[Loaded_From_Alpha__c]=SCD.[Loaded_From_Alpha__c],
			[Location_Address_Same_As_Donor_s_address__c]=SCD.[Location_Address_Same_As_Donor_s_address__c],
			[Mailing_City__c]=SCD.[Mailing_City__c],
			[Mailing_Country__c]=SCD.[Mailing_Country__c],
			[Mailing_Postal_Code__c]=SCD.[Mailing_Postal_Code__c],
			[Mailing_State__c]=SCD.[Mailing_State__c],
			[Mailing_Street1__c]=SCD.[Mailing_Street1__c],
			[Mailing_Street2__c]=SCD.[Mailing_Street2__c],
			[Make__c]=SCD.[Make__c],
			[Marketing_Source__c]=SCD.[Marketing_Source__c],
			[Missing_Parts_Detail__c]=SCD.[Missing_Parts_Detail__c],
			[Model__c]=SCD.[Model__c],
			[Name]=SCD.[Name],
			[NextStep]=SCD.[NextStep],
			[Odometer__c]=CONVERT(INT,CONVERT(float,SCD.[Odometer__c])),
			[Owner_Present__c]=SCD.[Owner_Present__c],
			[OwnerId]=SCD.[OwnerId],
			[Payment_Zip_Code__c]=SCD.[Payment_Zip_Code__c],
			[Pickup_Contact__c]=SCD.[Pickup_Contact__c],
			[Pickup_Contact_Alt_Phone__c]=SCD.[Pickup_Contact_Alt_Phone__c],
			[Pickup_Contact_Phone__c]=CASE WHEN LEN(SCD.[Pickup_Contact_Phone__c]) = 10 AND ISNUMERIC(SCD.[Pickup_Contact_Phone__c]) = 1 THEN  '('+SUBSTRING(SCD.[Pickup_Contact_Phone__c],1,3)+') '+SUBSTRING(SCD.[Pickup_Contact_Phone__c],4,3)+'-'+SUBSTRING(SCD.[Pickup_Contact_Phone__c],4,4) ELSE SCD.[Pickup_Contact_Phone__c] END,
			[Pickup_Date__c]=SCD.[Pickup_Date__c],
			[Pricebook2Id]=SCD.[Pricebook2Id],
			[Probability]=SCD.[Probability],
			[Proceeds_Disbursement_Date__c]=SCD.[Proceeds_Disbursement_Date__c],
			[Ready_For_Sale_Date__c]=SCD.[Ready_For_Sale_Date__c],
			[Reason_for_Choosing_Charity__c]=SCD.[Reason_for_Choosing_Charity__c],
			[Rebate_Amount__c]=SCD.[Rebate_Amount__c],
			[Rebate_Date__c]=SCD.[Rebate_Date__c],
			[Reject_Code__c]=SCD.[Reject_Code__c],
			[Reject_Comment__c]=SCD.[Reject_Comment__c],
			[Rejected_Approval__c]=SCD.[Rejected_Approval__c],
			[Rejected_Date__c]=CASE WHEN SCD.Donation_Status__c = 'Closed - Rejected' THEN SCD.CLOSE_DATE__C ELSE NULL END, --SCD.[Rejected_Date__c],
			[Release_Date__c]=SCD.[Release_Date__c],
			[Release_Time__c]=SCD.[Release_Time__c],
			[Salvage_Drop_Off_Date__c]=SCD.[Salvage_Drop_Off_Date__c],
			[Settlement_Date__c]=SCD.[Settlement_Date__c],
			[Sold_Date__c]=SCD.[Sold_Date__c],
			[Specialty_Option_Details__c]=SCD.[Specialty_Option_Details__c],
			[StageName]=SCD.[StageName],
			[Sub_Charity__c]=SCD.[Sub_Charity__c],
			[Sub_lead_Detail__c]=SCD.[Sub_lead_Detail__c],
			[Sub_lead_Source__c]=SCD.[Sub_lead_Source2__c],
			[Subsidy_Accepted__c]=SCD.[Subsidy_Accepted__c],
			[Subsidy_Check_Received__c]=SCD.[Subsidy_Check_Received__c],
			[SystemModstamp]=SCD.[SystemModstamp],
			[Tires_Inflated__c]=SCD.[Tires_Inflated__c],
			[Title_Available__c]=SCD.[Title_Available__c],
			[Title_Name_1__c]=SCD.[Title_Name_1__c],
			[Title_Name_1_SSN__c]=SCD.[Title_Name_1_SSN__c],
			[Title_Name_2__c]=SCD.[Title_Name_2__c],
			[Title_Name_2_SSN__c]=SCD.[Title_Name_2_SSN__c],
			[Title_Rec_From_DMV_Date__c]=SCD.[Title_Rec_From_DMV_Date__c],
			[Title_Rec_From_Salvage_Provider_Date__c]=SCD.[Title_Rec_From_Salvage_Provider_Date__c],
			[Title_Sent_to_DMV_Date__c]=SCD.[Title_Sent_to_DMV_Date__c],
			[Title_State_Province__c]=SCD.[Title_State_Province__c],
			--[TotalOpportunityQuantity]=SCD.[TotalOpportunityQuantity],
			[Tower_Notes__c]=SCD.[Tower_Notes__c],
			[TS_Notes__c]=SCD.[TS_Notes__c],
			[TS_Work_Order_Number__c]=SCD.[TS_Work_Order_Number__c],
			[Type]=SCD.[Type],
			[Vehicle_City__c]=SCD.[Vehicle_City__c],
			[Vehicle_Country__c]=SCD.[Vehicle_Country__c],
			[Vehicle_Parked_Location__c]=SCD.[Vehicle_Parked_Location__c],
			[Vehicle_State_Province__c]=SCD.[Vehicle_State_Province__c],
			[Vehicle_Street_1__c]=SCD.[Vehicle_Street_1__c],
			[Vehicle_Street_2__c]=SCD.[Vehicle_Street_2__c],
			[Vehicle_Type__c]=SCD.[Vehicle_Type__c],
			[Vehicle_Zip__c]=SCD.[Vehicle_Zip__c],
			[VIN__c]=SCD.[VIN__c],
			[Year__c]=SCD.[Year__c],
			[Charity_of_Choice] = CHARITY_OF_CHOICE__C,
			[Other_Charity_of_Choice] = OTHER_CHARITY_OF_CHOICE__C,
			Mailing_County__c = SCD.MAILING_COUNTY__C,
			Donor_HomePhone__c = CASE WHEN LEN(SCD.DONOR_HOMEPHONE__C) = 10 AND ISNUMERIC(SCD.DONOR_HOMEPHONE__C) = 1 THEN  '('+SUBSTRING(SCD.DONOR_HOMEPHONE__C,1,3)+') '+SUBSTRING(SCD.DONOR_HOMEPHONE__C,4,3)+'-'+SUBSTRING(SCD.DONOR_HOMEPHONE__C,4,4) ELSE SCD.DONOR_HOMEPHONE__C END,
			Donor_CellPhone__c = CASE WHEN LEN(SCD.DONOR_CELL_PHONE__C) = 10 AND ISNUMERIC(SCD.DONOR_CELL_PHONE__C) = 1 THEN  '('+SUBSTRING(SCD.DONOR_CELL_PHONE__C,1,3)+') '+SUBSTRING(SCD.DONOR_CELL_PHONE__C,4,3)+'-'+SUBSTRING(SCD.DONOR_CELL_PHONE__C,4,4) ELSE SCD.DONOR_CELL_PHONE__C END,
			Subsidy_Amount__c = CASE WHEN ISNUMERIC(SCD.Subsidy_Amount__c)=1 THEN SCD.Subsidy_Amount__c ELSE 0 END ,
			Call_In_Date__c = SCD.CALL_IN_DATE__C,
			BIUpdateDatetime = @BIUpdateDatetime,
			[AGE_SINCE_CREATION__C]=SCD.[AGE_SINCE_CREATION__C],
			[BODY_TYPE__C]=SCD.[BODY_TYPE__C],
			[DONOR_1_FIRST_NAME__C]=SCD.[DONOR_1_FIRST_NAME__C],
			[DONOR_1_LAST_NAME__C]=SCD.[DONOR_1_LAST_NAME__C],
			[DONOR_1_MIDDLE__C]=SCD.[DONOR_1_MIDDLE__C],
			[DONOR_1_TITLE__C]=SCD.[DONOR_1_TITLE__C],
			[DONOR_DROP_OFF__C]=SCD.[DONOR_DROP_OFF__C],
			[ID]=SCD.[ID],
			[INCLUDED_IN_YEAR_END_REPORT__C]=SCD.[INCLUDED_IN_YEAR_END_REPORT__C],
			[INTERNET_DONATION__C]=SCD.[INTERNET_DONATION__C],
			[IRS_REPORTED__C]=SCD.[IRS_REPORTED__C],
			[IRS_REPORTED_DATE__C]=SCD.[IRS_REPORTED_DATE__C],
			[LEAD_SOURCE__C]=SCD.LEAD_SOURCE__C,
			[MAKE2__C]=SCD.[MAKE2__C],
			[NEEDED_DONATION__C]=SCD.[NEEDED_DONATION__C],
			[NO_SOCIAL__C]=SCD.[NO_SOCIAL__C],
			[PICKUP_CONTACT_EMAIL__C]=SCD.[PICKUP_CONTACT_EMAIL__C],
			[REFERRAL_DONATION__C]=SCD.[REFERRAL_DONATION__C],
			[TS_NEEDED__C]=SCD.[TS_NEEDED__C],
			donor_email__c = SCD.donor_email__c,
			SPID = SCD.SPID,
			[ASAP_Close_Date__c]=SCD.Close_Date__c,
			Charity_Name__c = SCD.Charity_Name__c,
			Claim__c = SCD.Claim__c,
			MarketArea__c = SCD.MarketArea__c,
			[Channel] = SCD.Channel,
			[Creative] = SCD.Creative,
			[Venue] = SCD.[Venue]
	WHEN NOT MATCHED THEN
    INSERT([CharityDonationsId]
			,[AccountId]
			,[ACV__c]
			,[Affiliate__c]
			,[Affiliate_ID__c]
			,[Affiliate_Name__c]
			,[Alpha_ID__c]
			,[Alternate_CellPhone__c]
			,[Alternate_Contact__c]
			,[Alternate_Email__c]
			,[Alternate_Phone__c]
			,[Amount]
			,[Amount_of_Check__c]
			,[ASAP_Last_Status_Received__c]
			,[ASAP_Last_Status_Received_Date__c]
			,[Assignment_Date__c]
			,[ASV__c]
			,[ASV_Highest_Sales_Price__c]
			,[ASV_Lowest_Sales_Price__c]
			,[ASV_Most_Recent_Sale_Date__c]
			,[ASV_Oldest_Sale_Date__c]
			,[ASV_Vehicle_Count__c]
			,[Auction_Date__c]
			,[Best_Pickup_Contact__c]
			,[Body_Series__c]
			,[Branch__c]
			,[Branch_ID__c]
			,[Business_Name__c]
			,[Business_or_Residence__c]
			,[CampaignId]
			,[Cancel_Date__c]
			,[Charity__c]
			,[Checkin_Date__c]
			,[Close_Code__c]
			,[Close_Comment__c]
			,[Close_Date__c]
			,[CloseDate]
			,[Closest_Recycler__c]
			,[Color__c]
			,[Condition__c]
			,[Condition_Comments__c]
			,[Copy_of_Tow_Bill__c]
			,[Country__c]
			,[CreatedById]
			,[CreatedDate]
			,[Damaged_Areas_Detail__c]
			,[Description]
			,[Destination_Type__c]
			,[Discrepancy__c]
			,[Distance__c]
			,[Do_Not_Email__c]
			,[Donation_State__c]
			,[Donation_Status__c]
			,[Donation_Stock_Number__c]
			,[Donor_2_FirstName__c]
			,[Donor_2_LastName__c]
			--,[Donor_Phone_Value__c]
			,[Employer_Name__c]
			,[Error_Code_Message__c]
			,[Estimated_ACV__c]
			,[Event__c]
			,[Event_City__c]
			,[Event_State__c]
			,[Event_Street__c]
			,[EventPostalCode__c]
			,[Excess_Mileage__c]
			--,[ExpectedRevenue]
			,[Fiscal]
			,[FiscalQuarter]
			,[FiscalYear]
			,[ForecastCategory]
			,[ForecastCategoryName]
			,[Has_Damaged_Areas__c]
			,[Has_Missing_Parts__c]
			,[Has_Specialty_Options__c]
			,[Has_Subsidy__c]
			,[HasOpportunityLineItem]
			,[Honorary__c]
			,[In_Memory_Of__c]
			,[Is_Blocked__c]
			,[Is_Driveable__c]
			,[Is_Key_with_Vehicle__c]
			,[Is_Title_with_Vehicle__c]
			,[Is_Towable__c]
			,[Is_Vehicle_On_Hold__c]
			,[IsClosed]
			,[IsDeleted]
			--,[IsPrivate]
			,[IsWon]
			,[Last_on_Road__c]
			,[LastActivityDate]
			,[LastModifiedById]
			,[LastModifiedDate]
			,[LeadSource]
			,[Legacy_Information__c]
			,[Legacy_Stock_Number__c]
			,[License_Plate_Number__c]
			,[Limit__c]
			,[Loaded_From_Alpha__c]
			,[Location_Address_Same_As_Donor_s_address__c]
			,[Mailing_City__c]
			,[Mailing_Country__c]
			,[Mailing_Postal_Code__c]
			,[Mailing_State__c]
			,[Mailing_Street1__c]
			,[Mailing_Street2__c]
			,[Make__c]
			,[Marketing_Source__c]
			,[Missing_Parts_Detail__c]
			,[Model__c]
			,[Name]
			,[NextStep]
			,[Odometer__c]
			,[Owner_Present__c]
			,[OwnerId]
			,[Payment_Zip_Code__c]
			,[Pickup_Contact__c]
			,[Pickup_Contact_Alt_Phone__c]
			,[Pickup_Contact_Phone__c]
			,[Pickup_Date__c]
			,[Pricebook2Id]
			,[Probability]
			,[Proceeds_Disbursement_Date__c]
			,[Ready_For_Sale_Date__c]
			,[Reason_for_Choosing_Charity__c]
			,[Rebate_Amount__c]
			,[Rebate_Date__c]
			,[Reject_Code__c]
			,[Reject_Comment__c]
			,[Rejected_Approval__c]
			,[Rejected_Date__c]
			,[Release_Date__c]
			,[Release_Time__c]
			,[Salvage_Drop_Off_Date__c]
			,[Settlement_Date__c]
			,[Sold_Date__c]
			,[Specialty_Option_Details__c]
			,[StageName]
			,[Sub_Charity__c]
			,[Sub_lead_Detail__c]
			,[Sub_lead_Source__c]
			,[Subsidy_Accepted__c]
			,[Subsidy_Check_Received__c]
			,[SystemModstamp]
			,[Tires_Inflated__c]
			,[Title_Available__c]
			,[Title_Name_1__c]
			,[Title_Name_1_SSN__c]
			,[Title_Name_2__c]
			,[Title_Name_2_SSN__c]
			,[Title_Rec_From_DMV_Date__c]
			,[Title_Rec_From_Salvage_Provider_Date__c]
			,[Title_Sent_to_DMV_Date__c]
			,[Title_State_Province__c]
			--,[TotalOpportunityQuantity]
			,[Tower_Notes__c]
			,[TS_Notes__c]
			,[TS_Work_Order_Number__c]
			,[Type]
			,[Vehicle_City__c]
			,[Vehicle_Country__c]
			,[Vehicle_Parked_Location__c]
			,[Vehicle_State_Province__c]
			,[Vehicle_Street_1__c]
			,[Vehicle_Street_2__c]
			,[Vehicle_Type__c]
			,[Vehicle_Zip__c]
			,[VIN__c]
			,[Year__c]
			,[Charity_of_Choice]
			,[Other_Charity_of_Choice]
			,Mailing_County__c
			,Donor_HomePhone__c
			,Donor_CellPhone__c
			,Subsidy_Amount__c
			,Call_In_Date__c
			,BIUpdateDatetime
			,[AGE_SINCE_CREATION__C],
			[BODY_TYPE__C],
			[DONOR_1_FIRST_NAME__C],
			[DONOR_1_LAST_NAME__C],
			[DONOR_1_MIDDLE__C],
			[DONOR_1_TITLE__C],
			[DONOR_DROP_OFF__C],
			[ID],
			[INCLUDED_IN_YEAR_END_REPORT__C],
			[INTERNET_DONATION__C],
			[IRS_REPORTED__C],
			[IRS_REPORTED_DATE__C],
			[LEAD_SOURCE__C],
			[MAKE2__C],
			[NEEDED_DONATION__C],
			[NO_SOCIAL__C],
			[PICKUP_CONTACT_EMAIL__C],
			[REFERRAL_DONATION__C],
			[TS_NEEDED__C],
			donor_email__c,
			SPID,
			[ASAP_Close_Date__c],
			Charity_Name__c,
			Claim__c,
			MarketArea__c,
			[Channel],
			[Creative],
			[Venue]) 
	  VALUES ([Id]
			,[AccountId]
			,[ACV__c]
			,[Affiliate__c]
			,[Affiliate_ID__c]
			,[Affiliate_Name__c]
			,[Alpha_ID__c]
			,[Alternate_CellPhone__c]
			,[Alternate_Contact__c]
			,[Alternate_Email__c]
			,[Alternate_Phone__c]
			,[Amount]
			,[Amount_of_Check__c]
			,[ASAP_Last_Status_Received__c]
			,[ASAP_Last_Status_Received_Date__c]
			,[CloseDate]--[Assignment_Date__c]
			,[ASV__c]
			,[ASV_Highest_Sales_Price__c]
			,[ASV_Lowest_Sales_Price__c]
			,[ASV_Most_Recent_Sale_Date__c]
			,[ASV_Oldest_Sale_Date__c]
			,[ASV_Vehicle_Count__c]
			,[Auction_Date__c]
			,[Best_Pickup_Contact__c]
			,[Body_Series__c]
			,[Branch__c]
			,[Branch_ID__c]
			,[Business_Name__c]
			,[Business_or_Residence__c]
			,[CampaignId]
			,[Cancel_Date__c]
			,[Charity__c]
			,[Checkin_Date__c]
			,[Close_Code__c]
			,[Close_Comment__c]
			,COALESCE(Donation_Closed_Date__c,Close_Date__c)
			,[CloseDate]
			,[Closest_Recycler__c]
			,[Color__c]
			,[Condition__c]
			,[Condition_Comments__c]
			,[Copy_of_Tow_Bill__c]
			,[Country__c]
			,[CreatedById]
			,[CreatedDate]
			,[Damaged_Areas_Detail__c]
			,[Description]
			,[Destination_Type__c]
			,[Discrepancy__c]
			,[Distance__c]
			,[Do_Not_Email__c]
			,[Mailing_State__c]--[Donation_State__c]
			,[Donation_Status__c]
			,CASE WHEN ISNUMERIC(Donation_Stock_Number__c)=0 THEN NULL ELSE Donation_Stock_Number__c END --[Donation_Stock_Number__c]
			,[Donor_2_FirstName__c]
			,[Donor_2_LastName__c]
			--,[Donor_Phone_Value__c]
			,[Employer_Name__c]
			,[Error_Code_Message__c]
			,CAST(Estimated_ACV__c as FLOAT)
			,[Event__c]
			,[Event_City__c]
			,[Event_State__c]
			,[Event_Street__c]
			,[EventPostalCode__c]
			,[Excess_Mileage__c]
			--,[ExpectedRevenue]
			,[Fiscal]
			,[FiscalQuarter]
			,[FiscalYear]
			,[ForecastCategory]
			,[ForecastCategoryName]
			,[Has_Damaged_Areas__c]
			,[Has_Missing_Parts__c]
			,[Has_Specialty_Options__c]
			,[Has_Subsidy__c]
			,[HasOpportunityLineItem]
			,[Honorary__c]
			,[In_Memory_Of__c]
			,[Is_Blocked__c]
			,[Is_Driveable__c]
			,[Is_Key_with_Vehicle__c]
			,[Is_Title_with_Vehicle__c]
			,[Is_Towable__c]
			,[Is_Vehicle_On_Hold__c]
			,[IsClosed]
			,[IsDeleted]
			--,[IsPrivate]
			,[IsWon]
			,[Last_on_Road__c]
			,[LastActivityDate]
			,[LastModifiedById]
			,[LastModifiedDate]
			,LEAD_SOURCE__C
			,[Legacy_Information__c]
			,[Legacy_Stock_Number__c]
			,[License_Plate_Number__c]
			,[Limit__c]
			,[Loaded_From_Alpha__c]
			,[Location_Address_Same_As_Donor_s_address__c]
			,[Mailing_City__c]
			,[Mailing_Country__c]
			,[Mailing_Postal_Code__c]
			,[Mailing_State__c]
			,[Mailing_Street1__c]
			,[Mailing_Street2__c]
			,[Make__c]
			,[Marketing_Source__c]
			,[Missing_Parts_Detail__c]
			,[Model__c]
			,[Name]
			,[NextStep]
			,CONVERT(INT,CONVERT(float,SCD.[Odometer__c]))
			,[Owner_Present__c]
			,[OwnerId]
			,[Payment_Zip_Code__c]
			,[Pickup_Contact__c]
			,[Pickup_Contact_Alt_Phone__c]
			,CASE WHEN LEN(SCD.[Pickup_Contact_Phone__c]) = 10 AND ISNUMERIC(SCD.[Pickup_Contact_Phone__c]) = 1 THEN  '('+SUBSTRING(SCD.[Pickup_Contact_Phone__c],1,3)+') '+SUBSTRING(SCD.[Pickup_Contact_Phone__c],4,3)+'-'+SUBSTRING(SCD.[Pickup_Contact_Phone__c],4,4) ELSE SCD.[Pickup_Contact_Phone__c] END
			,[Pickup_Date__c]
			,[Pricebook2Id]
			,[Probability]
			,[Proceeds_Disbursement_Date__c]
			,[Ready_For_Sale_Date__c]
			,[Reason_for_Choosing_Charity__c]
			,[Rebate_Amount__c]
			,[Rebate_Date__c]
			,[Reject_Code__c]
			,[Reject_Comment__c]
			,[Rejected_Approval__c]
			,CASE WHEN SCD.Donation_Status__c = 'Closed - Rejected' THEN SCD.CLOSE_DATE__C ELSE NULL END --[Rejected_Date__c]
			,[Release_Date__c]
			,[Release_Time__c]
			,[Salvage_Drop_Off_Date__c]
			,[Settlement_Date__c]
			,[Sold_Date__c]
			,[Specialty_Option_Details__c]
			,[StageName]
			,[Sub_Charity__c]
			,[Sub_lead_Detail__c]
			,[Sub_lead_Source2__c]
			,[Subsidy_Accepted__c]
			,[Subsidy_Check_Received__c]
			,[SystemModstamp]
			,[Tires_Inflated__c]
			,[Title_Available__c]
			,[Title_Name_1__c]
			,[Title_Name_1_SSN__c]
			,[Title_Name_2__c]
			,[Title_Name_2_SSN__c]
			,[Title_Rec_From_DMV_Date__c]
			,[Title_Rec_From_Salvage_Provider_Date__c]
			,[Title_Sent_to_DMV_Date__c]
			,[Title_State_Province__c]
			--,[TotalOpportunityQuantity]
			,[Tower_Notes__c]
			,[TS_Notes__c]
			,[TS_Work_Order_Number__c]
			,[Type]
			,[Vehicle_City__c]
			,[Vehicle_Country__c]
			,[Vehicle_Parked_Location__c]
			,[Vehicle_State_Province__c]
			,[Vehicle_Street_1__c]
			,[Vehicle_Street_2__c]
			,[Vehicle_Type__c]
			,[Vehicle_Zip__c]
			,[VIN__c]
			,[Year__c]
			,CHARITY_OF_CHOICE__C
			,OTHER_CHARITY_OF_CHOICE__C
			,SCD.MAILING_COUNTY__C
			,CASE WHEN LEN(SCD.DONOR_HOMEPHONE__C) = 10 AND ISNUMERIC(SCD.DONOR_HOMEPHONE__C) = 1 THEN  '('+SUBSTRING(SCD.DONOR_HOMEPHONE__C,1,3)+') '+SUBSTRING(SCD.DONOR_HOMEPHONE__C,4,3)+'-'+SUBSTRING(SCD.DONOR_HOMEPHONE__C,4,4) ELSE SCD.DONOR_HOMEPHONE__C END
			,CASE WHEN LEN(SCD.DONOR_CELL_PHONE__C) = 10 AND ISNUMERIC(SCD.DONOR_CELL_PHONE__C) = 1 THEN  '('+SUBSTRING(SCD.DONOR_CELL_PHONE__C,1,3)+') '+SUBSTRING(SCD.DONOR_CELL_PHONE__C,4,3)+'-'+SUBSTRING(SCD.DONOR_CELL_PHONE__C,4,4) ELSE SCD.DONOR_CELL_PHONE__C END
			,CASE WHEN ISNUMERIC(SCD.Subsidy_Amount__c)=1 THEN SCD.Subsidy_Amount__c ELSE 0 END
			,SCD.CALL_IN_DATE__C
			,@BIUpdateDatetime
			,[AGE_SINCE_CREATION__C],
			[BODY_TYPE__C],
			[DONOR_1_FIRST_NAME__C],
			[DONOR_1_LAST_NAME__C],
			[DONOR_1_MIDDLE__C],
			[DONOR_1_TITLE__C],
			[DONOR_DROP_OFF__C],
			[ID],
			[INCLUDED_IN_YEAR_END_REPORT__C],
			[INTERNET_DONATION__C],
			[IRS_REPORTED__C],
			[IRS_REPORTED_DATE__C],
			[LEAD_SOURCE__C],
			[MAKE2__C],
			[NEEDED_DONATION__C],
			[NO_SOCIAL__C],
			[PICKUP_CONTACT_EMAIL__C],
			[REFERRAL_DONATION__C],
			[TS_NEEDED__C],
			donor_email__c,
			SPID,
			Close_Date__c,
			Charity_Name__c,
			Claim__c,
			MarketArea__c,
			[Channel],
			[Creative],
			[Venue])
OUTPUT $action INTO #tmp_RecordCount;

--UPDATE Donations SET SPID = Entities.SP_ID__c
----SELECT * 
--FROM EDW.dbo.[CharityDonations] Donations WITH (NOLOCK)
--inner join Staging.dbo.Charity_SF_Entities Entities WITH (NOLOCK)
--ON Donations.CHARITY__C = Entities.ID
UPDATE Donations SET SPID = Entities.Salvage_Provider_ID__C--SP_ID__c
--SELECT  SPID , Entities.Salvage_Provider_ID__C
FROM EDW.dbo.[CharityDonations] Donations WITH (NOLOCK)
inner join Staging.dbo.Charity_SF_Entities Entities WITH (NOLOCK)
ON Donations.CHARITY__C = Entities.ID

UPDATE Donations SET Branch_ID__c = 921
--SELECT * 
FROM EDW.dbo.[CharityDonations] Donations WITH (NOLOCK)
WHERE  Branch_ID__c LIKE '921/%'

UPDATE Donations SET ASAP_Salvage_ID = S.Salvage_ID
--SELECT ASAP_Salvage_ID,S.Salvage_ID,*
FROM EDW.dbo.[CharityDonations] Donations WITH (NOLOCK)
inner join BI_ASAP_REP.dbo.Salvage S WITH (NOLOCK)
ON Donations.Donation_Stock_Number__c = S.Stock_Number
AND Donations.Branch_ID__c = S.Administrative_Branch_Number
WHERE ISNULL(ASAP_Salvage_ID,0)<>ISNULL(S.Salvage_ID,0)
AND S.Salvage_ID <> 2000000000

UPDATE Donations SET ASAP_Salvage_ID = NULL
--SELECT * 
FROM EDW.dbo.[CharityDonations] Donations WITH (NOLOCK)
WHERE Donation_Stock_Number__c IS NULL AND ASAP_Salvage_ID > 0

UPDATE Donations SET ASAP_Salvage_ID = S.ASAPSalvageID
--SELECT * 
FROM EDW.dbo.[CharityDonations] Donations WITH (NOLOCK)
inner join EDW.dbo.DimStock S WITH (NOLOCK)
ON Donations.Donation_Stock_Number__c =S.StockNumber
AND Donations.Branch_ID__c = S.AdministrativeBranchNumber
WHERE ASAP_Salvage_ID IS NULL AND S.ASAPSalvageID <> 2000000000

UPDATE Donations SET ASAP_Salvage_ID = S.Salvage_ID
--SELECT Donation_Stock_Number__c,ASAP_Salvage_ID,S.Salvage_ID,Donations.Branch_ID__c,S.Administrative_Branch_Number, *
FROM EDW.dbo.[CharityDonations] Donations WITH (NOLOCK)
inner join BI_ASAP_REP.dbo.Salvage S WITH (NOLOCK)
ON Donations.Donation_Stock_Number__c = S.Stock_Number
AND ISNULL(Donations.Branch_ID__c,0) <> ISNULL(S.Administrative_Branch_Number,0)
WHERE ISNULL(ASAP_Salvage_ID,0)<>ISNULL(S.Salvage_ID,0)
AND S.Salvage_ID <> 2000000000
AND Assignment_Date__c >'2013-03-25 00:00:00.000'
--ORDER BY Donations.Donation_Stock_Number__c

UPDATE Donations SET SPID = S.CurrentProviderID
--SELECT * 
FROM EDW.dbo.[CharityDonations] Donations WITH (NOLOCK)
inner join EDW.dbo.DimStock S WITH (NOLOCK)
ON Donations.ASAP_Salvage_ID =S.ASAPSalvageID
WHERE SPID IS NULL

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
