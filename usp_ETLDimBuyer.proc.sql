/* 
USE EDW
GO 
==============================================================================
Author		: Sidd Murao
Create date	: 08/12/2011
Description	: Populate the EDW DimBuyer table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer			
8/17/2011	Sidd Murao		Changed to use CDC Net Change Function 
8/23/2011	Sidd Murao		Added Country Name 
8/28/2011   Sidd Murao		Added all the TO-Be Columns
10/3/2011   Sidd Murao		Added all the default Values
10/28/2011	Juan X. Jacome	Added code to drop temp Tables   (DBA Code Review)
5/26/2012   Sidd Murao      Added extra columns
6/1/2012    Sidd Murao		Updated Logic for PrimaryContactMethod
6/4/2012    Sidd Murao		Added logic for BuyerCurrentStatusEffectiveDatetime
6/7/2012	Juan X. Jacome	DBA Review added DROP TABLE #Buyer_Current_Status_Effective_Date to end of try statement
6/14/2012   Sidd Murao      Additional Object
8/30/2012   Venkata Vempali Got previous version and added logic to #Buyer_Current_Status_Effective_Date
8/30/2012   Preetham Duvva  Added BuyerStatusDescription
12/6/2013   Matt Stelter	Added BuyerMailCityName
2/21/2013   Matt Stelter	Updated BuyerStatusDescription
05/06/2015	Bruce Kutnick   Add IsForeignBuyer attribute
05/12/2015  Bruce Kutnick	IsForeignBuyer rule change - the buyer's Country must come from Buyer table, note Buyer_License table.
05/13/2015  Bruce Kutnick	Added License Type 'USP' (Routed Export Agreement) as a valid 'exporter' type if US location
05/18/2015	Matt Stelter	Update IsForeignBuyer to do final update
6/15/2015	Matt Stelter	Update DimBuyer Emails
==============================================================================
Usage Example:
Exec usp_ETLDimbuyer
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLDimBuyer]
AS
  BEGIN
      DECLARE @ETLLoadID       INT,
              @InsertRowCount  INT,
              @UpdateRowCount  INT,
              @DeleteRowCount  INT,
              @StartLSN        BINARY(10),
              @EndLSN          BINARY(10),
              @ExtractRowCount INT

      SET NOCOUNT ON;

      BEGIN  TRY
          --Run the Audit begin
          EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'uspDimBuyer'

          --Get the start and end LSNs
          SELECT @StartLSN=MinLSN,
                 @EndLSN=MaxLSN
          FROM   dbo.ETLRun WITH (NOLOCK)
          WHERE  ETLJobName='DailyETLJob'

          /*********************************************************************************************
          Create the temp table Buyer1 to GET BUYER specific info
          *********************************************************************************************/
          IF Object_id('tempdb..#Buyer1') IS NOT NULL
            DROP TABLE  #Buyer1

          CREATE TABLE #Buyer1
            ([BuyerID]                            [INT] NOT NULL,
             [BuyerName]                          [NVARCHAR](60) NULL,
             [BuyerTypeCode]                      [NCHAR](4) NULL,
             [PhoneExtension1]                    [DECIMAL](5) NULL,
             [PhoneExtension2]                    [DECIMAL](5) NULL,
             [PhoneNumber1]                       [NVARCHAR](20) NULL,
             [PhoneNumber2]                       [NVARCHAR](20) NULL,
             [HomeBranchNumber]                   [INT] NULL,
             [InitialRegistrationDayID]           [INT] NULL,
             [InitialRegistrationDateTime]        [DATETIME] NULL,
             [InitialRegistrationRequestDayID]    [INT] NULL,
             [InitialRegistrationRequestDateTime] [DATETIME] NULL,
             [ParentCompanyBuyerID]               [INT] NULL,
             [IsPreferredBuyer]                   [TINYINT] NOT NULL,
             [RegistrationSourceCode]             [NCHAR](3) NULL,
             [StatusCode]                         [NCHAR](3) NULL,
             [AddressLine1]                       [NVARCHAR](40) NULL,
             [AddressLine2]                       [NVARCHAR](40) NULL,
             [CityName]                           [NVARCHAR](40) NULL,
             [CountyName]                         [NVARCHAR] (30) NULL,
             [StateAbbreviation]                  [NCHAR](3) NULL,
             [ZipCode]                            [NVARCHAR](20) NULL,
             [CountryAbbreviation]                [NCHAR](10) NULL,
             [EmailAddress]                       [NVARCHAR](60) NULL,
             [FaxNumber]                          [NVARCHAR](20) NULL,
             [IsArchive]                          [TINYINT] NULL,
             [LBSParentBuyerID]                   [INT] NULL,
             [IsLBSParent]                        [TINYINT] NULL,
             [IsRenege]                           [TINYINT] NULL,
             [IsOneDayGraceGrantedInd]            [TINYINT],
             [IsCreditCardAllowedInd]             [TINYINT],
             [BuyerFederalTaxID]                  [NVARCHAR] (9) NULL,
             [BuyerMailAddressLine1]              [NVARCHAR] (40) NULL,
             [BuyerMailAddressLine2]              [NVARCHAR] (40) NULL,
             [BuyerMailCountryAbbreviation]       [NCHAR] (3) NULL,
             [BuyerMailCountyName]                [NVARCHAR] (30) NULL,
             [BuyerMailStateAbbreviation]         [NCHAR] (3) NULL,
             [BuyerMailZipCode]                   [NVARCHAR] (20) NULL,
             [Primary_Contact_ID]                 [INT] NULL,
             [IsBuyerCheckAllowed]                [TINYINT] NULL,
			 [BuyerMailCityName]				  [VARCHAR] (40) NULL
             )
          /*********************************************************************************************
          Create the temp table to consolidate the Buyer_registration
          *********************************************************************************************/
          IF Object_id('tempdb..#Buyer_Registration') IS NOT NULL
            DROP TABLE #Buyer_Registration

          CREATE TABLE #Buyer_Registration
            ([BuyerID]                   INT,
             RegistrationEffectiveDayID  INT,
             RegistrationEffectiveDate   DATETIME,
             RegistrationExpirationDayID INT,
             RegistrationExpirationDate  DATETIME)

          /*********************************************************************************************
          Create the temp table to consolidate the Buyer_Charge
          *********************************************************************************************/
          IF Object_id('tempdb..#Buyer_Charge') IS NOT NULL
            DROP TABLE #Buyer_Charge

          CREATE TABLE #Buyer_Charge
            ([BuyerID]       INT,
             Charge_DateTime DATETIME)
  /*********************************************************************************************
          Create the temp table to consolidate the Buyer_Charge_insert
          *********************************************************************************************/
          IF Object_id('tempdb..#Buyer_Charge_insert') IS NOT NULL
            DROP TABLE #Buyer_Charge_insert

          CREATE TABLE #Buyer_Charge_insert
            ([BuyerID]       INT,
             Charge_DateTime DATETIME)
        
          /*********************************************************************************************
          Create the temp table to consolidate the Buyer_Questions
          *********************************************************************************************/
          IF Object_id('tempdb..#Buyer_Question') IS NOT NULL
            DROP TABLE #Buyer_Question

          CREATE TABLE #Buyer_Question
            (buyerid         INT,
             Other_Answer_1  [NVARCHAR] (60),
             Other_Answer_2  [NVARCHAR] (60),
             Question_Code_1 [NCHAR] (3),
             Question_Code_2 [NCHAR] (3))
             
             
          /*********************************************************************************************
          Create the temp table to Add Buyer Current Status Effective Date
          *********************************************************************************************/
       

          CREATE TABLE #Buyer_Current_Status_Effective_Date
            (buyerid         INT,
             Buyer_Current_Status_Effective_Date  [DATETIME] NULL ,
             Buyer_Current_Status_Effective_DayID [INT] NULL
           )



          /*********************************************************************************************
          Create the temp table to Add Buyer User Info
          *********************************************************************************************/
       

          CREATE TABLE #Buyer_User_info
            ([buyerid]         INT,
             [Primary_Contact_Method_Code] NVARCHAR(2) NULL,
             [Email_address_1] NVARCHAR(60) NULL,
             [Email_address_2] NVARCHAR(60) NULL,
             [first_name]  NVARCHAR(30) NULL,
             [Last_name]  NVARCHAR(30) NULL,
             [Mobile_Phone_Number] BIGINT NULL
           )
           
           
          /*********************************************************************************************
          Create the temp table to Add Buyer Promo
          *********************************************************************************************/
       

          CREATE TABLE #Buyer_Promo
            ([buyerid]         INT,
             [RAN] INT,
             [BuyerPromoCode]  NVARCHAR(1024) NULL)

          /*********************************************************************************************
          Create the temp table for Buyer Final to consolidate all buyer changes
          *********************************************************************************************/
          IF Object_id('tempdb..#Final_Buyer') IS NOT NULL
            DROP TABLE #Final_Buyer

          CREATE TABLE #Final_Buyer
            ([BuyerID]                            [INT] NOT NULL,
             [BuyerName]                          [NVARCHAR](60) NULL,
             [BuyerTypeCode]                      [NCHAR](4) NULL,
             [BuyerTypeDescription]               [NVARCHAR](20) NULL,
             [PhoneExtension1]                    [DECIMAL](5) NULL,
             [PhoneExtension2]                    [DECIMAL](5) NULL,
             [PhoneNumber1]                       [NVARCHAR](20) NULL,
             [PhoneNumber2]                       [NVARCHAR](20) NULL,
             [HomeBranchNumber]                   [INT] NULL,
             [InitialRegistrationDayID]           [INT] NULL,
             [InitialRegistrationDateTime]        [DATETIME] NULL,
             [InitialRegistrationRequestDayID]    [INT] NULL,
             [InitialRegistrationRequestDateTime] [DATETIME] NULL,
             [ParentCompanyBuyerID]               [INT] NULL,
             [IsPreferredBuyer]                   [TINYINT],
             [RegistrationEffectiveDayID]         [INT] NULL,
             [RegistrationEffectiveDateTime]      [DATETIME] NULL,
             [RegistrationExpirationDayID]        [INT] NULL,
             [RegistrationExpirationDateTime]     [DATETIME] NULL,
             [RegistrationRenewalDayID]           [INT] NULL,
             [RegistrationRenewalDateTime]        [DATETIME] NULL,
             [RegistrationSourceCode]             [NCHAR](3) NULL,
             [StatusCode]                         [NCHAR](3) NULL,
             [AddressLine1]                       [NVARCHAR](40) NULL,
             [AddressLine2]                       [NVARCHAR](40) NULL,
             [CityName]                           [NVARCHAR](40) NULL,
             [StateAbbreviation]                  [NCHAR](3) NULL,
             [ZipCode]                            [NVARCHAR](20) NULL,
             [CountryAbbreviation]                [NCHAR](10) NULL,
             [CountyName]                         [NVARCHAR](30) NULL,
             [CountryName]                        [NVARCHAR](40) NULL,
             [EmailAddress]                       [NVARCHAR](60) NULL,
             [FaxNumber]                          [NVARCHAR](20) NULL,
             [PrimaryContactName]                 [NVARCHAR](100) NULL,
             [PrimaryContactMethod]               [NVARCHAR](60) NULL,
             [PrimaryContactCellPhone]            [NVARCHAR](20) NULL,
             [IsArchive]                          [TINYINT] NOT NULL,
             [LBSParentBuyerID]                   [INT] NULL,
             [IsLBSParent]                        [TINYINT] NOT NULL,
             [IsRenege]                           [TINYINT] NOT NULL,
             [ASAPUserid]                         [INT] NULL,
             [IsOneDayGraceGrantedInd]            [TINYINT],
             [IsCreditCardAllowedInd]             [TINYINT],
             [BuyerFederalTaxID]                  [NVARCHAR] (9) NULL,
             [BuyerMailAddressLine1]              [NVARCHAR] (40) NULL,
             [BuyerMailAddressLine2]              [NVARCHAR] (40) NULL,
             [BuyerMailCountryAbbreviation]       [NCHAR] (3) NULL,
             [BuyerMailCountyName]                [NVARCHAR] (30) NULL,
             [BuyerMailStateAbbreviation]         [NCHAR] (3) NULL,
             [BuyerMailZipCode]                   [NVARCHAR] (20) NULL,
             [BuyerPrimaryPurpose]                [NVARCHAR] (100) NULL,
             [BuyerSecondaryPurpose]              [NVARCHAR] (100) NULL,
             [BuyerPrimaryOther]                  [NVARCHAR] (60) NULL,
             [BuyerSecondaryOther]                [NVARCHAR] (60) NULL,
             [IsBuyerCheckAllowed]                [TINYINT] NOT NULL,
             [BuyerPromoCode]                     [nvarchar] (1024) NULL,
             [BuyerStatusDescription]             [nvarchar] (40) NULL,
			 [BuyerMailCityName]				  [VARCHAR] (40) NULL,
			 [IsForeignBuyer]					  [TINYINT]  NULL 
             )

          IF Object_id('tempdb..#recordCount') IS NOT NULL
            DROP TABLE #recordCount

          CREATE TABLE #recordCount
            (ChangeType VARCHAR(10))

          /*********************************************************************************************
          Insert the values from the CDC Buyer into the temp table Buyer
          *********************************************************************************************/
          INSERT INTO #Buyer1
                      ([BuyerID],
                       [BuyerName],
                       [BuyerTypeCode],
                       [PhoneExtension1],
                       [PhoneExtension2],
                       [PhoneNumber1],
                       [PhoneNumber2],
                       [InitialRegistrationDayID],
                       [InitialRegistrationDateTime],
                       [InitialRegistrationRequestDayID],
                       [InitialRegistrationRequestDateTime],
                       [ParentCompanyBuyerID],
                       [IsPreferredBuyer],
                       [RegistrationSourceCode],
                       [StatusCode],
                       [AddressLine1],
                       [AddressLine2],
                       [CityName],
                       [CountyName],
                       [StateAbbreviation],
                       [ZipCode],
                       [CountryAbbreviation],
                       [EmailAddress],
                       [FaxNumber],
                       [IsArchive],
                       [LBSParentBuyerID],
                       [Primary_Contact_ID],
                       [IsLBSParent],
                       [IsRenege],
                       [IsOneDayGraceGrantedInd],
                       [IsCreditCardAllowedInd],
                       [BuyerFederalTaxID],
                       [BuyerMailAddressLine1],
                       [BuyerMailAddressLine2],
                       [BuyerMailCountryAbbreviation],
                       [BuyerMailCountyName],
                       [BuyerMailStateAbbreviation],
                       [BuyerMailZipCode],
                       [HomeBranchNumber],
                       [IsBuyerCheckAllowed],
					   [BuyerMailCityName]
                       )
          SELECT [Buyer_ID],
                 Isnull([Buyer_Name], ''),
                 Isnull([Buyer_Type_Code], ''),
                 [Phone_Extension_1],
                 [Phone_Extension_2],
                 Isnull([Phone_Number_1], ''),
                 Isnull([Phone_Number_2], ''),
                 Isnull(CONVERT(INT, CONVERT(CHAR(8), Initial_Registration_Date, 112)), 29991231),
                 Isnull([Initial_Registration_Date], '2999-12-31 00:00:00.000'),
                 Isnull(CONVERT(INT, CONVERT(CHAR(8), Initial_Registration_Request_Date, 112)), 29991231),
                 Isnull([Initial_Registration_Request_Date], '2999-12-31 00:00:00.000'),
                 Isnull([Parent_Buyer_Company_ID], 0),
                 Isnull([Preferred_Buyer_Ind], 0),
                 Isnull([Registration_Source_Code], ''),
                 Isnull([Status_Code], ''),
                 Isnull([Address_Line_1], ''),
                 Isnull([Address_Line_2], ''),
                 Isnull([City_Name], ''),
                 Isnull([County_Name], ''),
                 Isnull([State_Abbreviation], ''),
                 Isnull([Zip_Code], ''),
                 Isnull([Country_Abbreviation], ''),
                 Isnull([EMail_Address], ''),
                 Isnull([Fax_Number], ''),
                 Isnull([Archive_Ind], 0),
                 Isnull([LBS_Parent_Buyer_ID], 0),
                 Isnull([Primary_Contact_ID], 0),
                 Isnull([LBS_Parent_Ind], 0),
                 Isnull([Renege_Ind], 0),
                 Isnull([One_Day_Grace_Granted_Ind], 0),
                 Isnull([Credit_Card_Allowed_Ind], 0),
                 Isnull([Federal_Tax_ID], 0),
                 Isnull([Mail_Address_Line_1], ''),
                 Isnull([Mail_Address_Line_2], ''),
                 Isnull([Mail_Country_Abbreviation], ''),
                 Isnull([Mail_County_Name], ''),
                 Isnull([Mail_State_Abbreviation], ''),
                 Isnull([Mail_Zip_Code], ''),
                 Isnull([Home_Branch_Number], 0),
                 Isnull([Check_Allowed_Ind],0),
				 Isnull([Mail_City_Name], '')
          FROM   --[bi_asap_Rep].cdc.dbo_Buyer_CT
          --[BI_ASAP_Rep].dbo.buyer
          [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_buyer] (@StartLSN, @EndLSN, 'all with merge')
          WHERE  [__$operation]=5

          --select * from #buyer1
          /*********************************************************************************************
          Insert the values from the CDC Buyer_registration into the temp table
          *********************************************************************************************/
          INSERT INTO #Buyer_Registration
                      ([BuyerID],
                       RegistrationEffectiveDayID,
                       RegistrationEffectiveDate,
                       RegistrationExpirationDayID,
                       RegistrationExpirationDate)
          SELECT [Buyer_ID],
                 CONVERT(INT, CONVERT(CHAR(8), Effective_Date, 112)),
                 Effective_Date,
                 CONVERT(INT, CONVERT(CHAR(8), Expiration_Date, 112)),
                 Expiration_Date
          -- FROM [BI_ASAP_REP].DBO.Buyer_Registration
          FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_buyer_registration] (@StartLSN, @EndLSN, 'all with merge')
          WHERE  [__$operation]=5

          /*********************************************************************************************
          Insert the values from the CDC Buyer_Charge into the temp table
          *********************************************************************************************/
          INSERT INTO #Buyer_Charge
                      ([BuyerID],
                       Charge_DateTime)
          SELECT [Buyer_ID],
                 Max(Charge_DateTime)
          -- from [BI_ASAP_Rep].[dbo].[buyer_charge]
          FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_buyer_charge] (@StartLSN, @EndLSN, 'all with merge')
          WHERE  [__$operation]=5
             AND charge_type_id=30
             AND status_code='BKP'
          GROUP  BY buyer_id

         /*********************************************************************************************
          Insert the values from the Buyer_Charge into the temp table
          *********************************************************************************************/
          INSERT INTO #Buyer_Charge_insert
                      ([BuyerID],
                       Charge_DateTime)
          SELECT [Buyer_ID],
                 Max(Charge_DateTime)
          -- from [BI_ASAP_Rep].[dbo].[buyer_charge]
          FROM   [bi_asap_rep].dbo.Buyer_Charge bci WITH (NOLOCK)
          inner join #Buyer1 Bc1 WITH (NOLOCK)
          on (bci.buyer_id=bc1.BuyerID)
          WHERE   charge_type_id=30
             AND status_code='BKP'
          GROUP  BY buyer_id
          /*********************************************************************************************
          Insert the values from the CDC Buyer_Question into the temp table
          *********************************************************************************************/
          INSERT INTO #Buyer_Question
                      ([BuyerID],
                       Other_Answer_1,
                       Other_Answer_2,
                       Question_Code_1,
                       Question_Code_2)
          SELECT [Buyer_ID],
                 Other_Answer_1,
                 Other_Answer_2,
                 Question_Code_1,
                 Question_Code_2
          -- from [BI_ASAP_Rep].[dbo].[buyer_question]
          FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_buyer_question] (@StartLSN, @EndLSN, 'all with merge')
          WHERE  [__$operation]=5
          
          
          
          /*********************************************************************************************
          Insert the values from the CDC Buyer_Audit into the temp table
          *********************************************************************************************/
         
      INSERT INTO #Buyer_Current_Status_Effective_Date
                  (BuyerID,                  
                   Buyer_Current_Status_Effective_Date,
                   Buyer_Current_Status_Effective_DayID )
      SELECT BA.Buyer_ID,
                  BA.Update_DateTime,
             CONVERT(INT, CONVERT(CHAR(8), BA.Update_DateTime, 112)) 
      FROM   BI_ASAP_Audit_Rep.dbo.Buyer_Audit BA WITH (NOLOCK)
			 INNER JOIN #Buyer1 B
				ON BA.Buyer_ID = B.BuyerID
             INNER JOIN (SELECT Buyer_ID,
                                Max(Update_DateTime) AS MaxUpdateDateTime
                         FROM   BI_ASAP_Audit_Rep.dbo.Buyer_Audit WITH (NOLOCK)
                         --WHERE  Audit_Action_Code='U'
                         GROUP  BY Buyer_ID) MAXBA
               ON BA.Buyer_ID=MAXBA.Buyer_ID
                  AND BA.Update_DateTime=MAXBA.MaxUpdateDateTime
                  
     
    
                  
        /*********************************************************************************************
         Insert the values from  User Info CDC
          *********************************************************************************************/
       

          INSERT INTO #Buyer_User_info
            ([buyerid],
             [Primary_Contact_Method_Code] ,
             [Email_address_1] ,
             [Email_address_2] ,
             [first_name] ,
             [Last_name],
             [Mobile_Phone_Number]
           )
           Select DB.Buyer_ID,
                  UI.Primary_Contact_Method_Code,
                  UI.Email_Address_1,
                  UI.Email_Address_2,
                  UI.First_name,
                  UI.Last_name,
                  UI.Mobile_Phone_Number
             FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_User_info] (@StartLSN, @EndLSN, 'all with merge') UI 
                    INNER JOIN [BI_ASAP_REP].dbo.user_association UA WITH (NOLOCK) on (UI.User_id= UA.User_id)
                    INNER JOIN [BI_ASAP_REP].dbo.Buyer DB with (nolock) on (DB.Primary_Contact_ID = UA.Buyer_Employee_ID)
          WHERE  [__$operation]=5
             
             
             
             
             
          /*********************************************************************************************
          Insert the Values from  #Buyer_Promo
          *********************************************************************************************/
       

          INSERT INTO #Buyer_Promo
            ([buyerid],
             [BuyerPromoCode],
             [RAN]

           )
           SELECT
         
         BC.Buyer_ID
         , BC.Note_Text,
         RANK() over (partition by BC.Buyer_ID order by  BC.Update_Datetime desc ) 
          
FROM   [BI_ASAP_Rep].[cdc].[Fn_cdc_get_net_changes_dbo_Buyer_Charge] (@StartLSN, @EndLSN, 'all with merge') BC                         
       inner join [Bi_ASAP_rep].dbo.Buyer B WITH (NOLOCK) on (B.buyer_id = BC.Buyer_ID)
WHERE               
Charge_Type_ID IN (30)              

          /*********************************************************************************************
          MERGE the data from the temp table #Final_Buyer  into the DimBuyer table
          This does Update and Insert only.  No Deletes
          *********************************************************************************************/
          INSERT INTO #Final_Buyer
                      ([BuyerID],
                       [BuyerName],
                       [BuyerTypeCode],
                       [BuyerTypeDescription],
                       [PhoneExtension1],
                       [PhoneExtension2],
                       [PhoneNumber1],
                       [PhoneNumber2],
                       [HomeBranchNumber],
                       [InitialRegistrationDayID],
                       [InitialRegistrationDateTime],
                       [InitialRegistrationRequestDayID],
                       [InitialRegistrationRequestDateTime],
                       [ParentCompanyBuyerID],
                       [IsPreferredBuyer],
                       [RegistrationEffectiveDayID],
                       [RegistrationEffectiveDateTime],
                       [RegistrationExpirationDayID],
                       [RegistrationExpirationDateTime],
                       RegistrationRenewalDayID,
                       RegistrationRenewalDateTime,
                       [RegistrationSourceCode],
                       [StatusCode],
                       [AddressLine1],
                       [AddressLine2],
                       [CityName],
                       [StateAbbreviation],
                       [ZipCode],
                       [CountryAbbreviation],
                       [CountyName],
                       [CountryName],
                       [EmailAddress],
                       [FaxNumber],
                       [PrimaryContactName],
                       [PrimaryContactMethod],
                       [PrimaryContactCellPhone],
                       [IsArchive],
                       [LBSParentBuyerID],
                       [IsLBSParent],
                       [IsRenege],
                       [ASAPUserid],
                       [IsOneDayGraceGrantedInd],
                       [IsCreditCardAllowedInd],
                       [BuyerFederalTaxID],
                       [BuyerMailAddressLine1],
                       [BuyerMailAddressLine2],
                       [BuyerMailCountryAbbreviation],
                       [BuyerMailCountyName],
                       [BuyerMailStateAbbreviation],
                       [BuyerMailZipCode],
                       [BuyerPrimaryPurpose],
                       [BuyerSecondaryPurpose],
                       [BuyerPrimaryOther],
                       [BuyerSecondaryOther],

                       [IsBuyerCheckAllowed],
                       [BuyerPromoCode],
                       [BuyerStatusDescription],
					   [BuyerMailCityName],
					   [IsForeignBuyer]
					    )
          SELECT DISTINCT B.[BuyerID],
                 B.[BuyerName],
                 B.[BuyerTypeCode],
                 Buyer_Type.Buyer_Type_Description,
                 B.[PhoneExtension1],
                 B.[PhoneExtension2],
                 B.[PhoneNumber1],
                 B.[PhoneNumber2],
                 B.[HomeBranchNumber],
                 B.InitialRegistrationDayID,
                 B.[InitialRegistrationDateTime],
                 B.InitialRegistrationRequestDayID,
                 Isnull(B.[InitialRegistrationRequestDateTime], '2999-12-31 00:00:00.000'),
                 B.[ParentCompanyBuyerID],
                 B.[IsPreferredBuyer],
                 Isnull(CONVERT(INT, CONVERT(CHAR(8), BR.Effective_Date, 112)), 29991231),
                 Isnull(BR.Effective_Date, '2999-12-31 00:00:00.000'),
                 Isnull(CONVERT(INT, CONVERT(CHAR(8), BR.Expiration_Date, 112)), 29991231),
                 Isnull(BR. Expiration_Date, '2999-12-31 00:00:00.000'),
                 Isnull(CONVERT(INT,(BC.Charge_DateTime), 112), 29991231),
                 Isnull((BC.Charge_DateTime), '2999-12-31 00:00:00.000'),
                 B.[RegistrationSourceCode],
                 B.[StatusCode],
                 B.[AddressLine1],
                 B.[AddressLine2],
                 B.[CityName],
                 B.[StateAbbreviation],
                 B.[ZipCode],
                 B.[CountryAbbreviation],
                 B.[CountyName],
                 CTR.Country_Name,
                 ISNULL(coalesce (UI.email_address_1,UI.email_Address_2),''),
                 B.[FaxNumber],
                 Isnull(UI.first_name, '')+' '+Isnull(UI.last_name, '') AS Primary_Contact_Name,
                 UI.Primary_Contact_Method_Code AS Primary_Contact_Method,
                 UI.Mobile_Phone_Number AS Primary_Contact_Cell_Phone,
                 B.[IsArchive],
                 B.[LBSParentBuyerID],
                 B.[IsLBSParent],
                 B.[IsRenege],
                 UA.User_ID,
                 B.[IsOneDayGraceGrantedInd],
                 B.[IsCreditCardAllowedInd],
                 B.[BuyerFederalTaxID],
                 B.[BuyerMailAddressLine1],
                 B.[BuyerMailAddressLine2],
                 B.[BuyerMailCountryAbbreviation],
                 B.[BuyerMailCountyName],
                 B.[BuyerMailStateAbbreviation],
                 B.[BuyerMailZipCode],
                 isnull(BQX.[Question],''),
                 isnull(BQX2.[Question],''),
                 isnull(BQ.[Other_Answer_1],''),
                 isnull(BQ.[Other_Answer_2],''),
      
                 B.[IsBuyerCheckAllowed],
                 DB.[BuyerPromoCode],
                 '',
				 B.[BuyerMailCityName],
				 DB.[IsForeignBuyer]
          FROM   #Buyer1 AS B WITH (NOLOCK)
                 LEFT OUTER JOIN [BI_ASAP_REP].dbo.Buyer_Type AS Buyer_Type WITH (NOLOCK)
                   ON B.BuyerTypeCode=Buyer_Type.Buyer_Type_Code
                 LEFT OUTER JOIN [BI_ASAP_REP].dbo.Country AS CTR WITH (NOLOCK)
                   ON B.CountryAbbreviation=CTR.Country_Abbreviation
                 LEFT OUTER JOIN dbo.DimBuyer AS TRG_Buyer WITH (NOLOCK)
                   ON B.ParentCompanyBuyerID=TRG_Buyer.BuyerID
                 LEFT OUTER JOIN [BI_ASAP_REP].dbo.user_association UA WITH (NOLOCK)
                   ON B.Primary_Contact_ID=ua.buyer_employee_ID
                 LEFT OUTER JOIN [BI_ASAP_REP].DBO.User_INFO UI WITH (NOLOCK)
                   ON UI.User_ID=UA.User_ID
                 LEFT OUTER JOIN [BI_ASAP_REP].dbo.Buyer_Registration AS BR WITH (NOLOCK)
                   ON (B.BuyerID=BR.Buyer_ID)
                 LEFT OUTER JOIN #Buyer_Charge_insert AS BC WITH (NOLOCK)
                   ON (B.BuyerID=BC.BuyerID)               
                 LEFT OUTER JOIN [BI_ASAP_Rep].dbo.Buyer_Question BQ WITH (NOLOCK)
                   ON (BQ.buyer_id=B.BuyerID)
                 LEFT OUTER JOIN [BI_ASAP_REP].dbo.Buyer_Question_XRef bqx WITH (NOLOCK)
                   ON BQ.Question_Code_1=BQX.Question_Code
                 LEFT OUTER JOIN [BI_ASAP_REP].dbo.Buyer_Question_XRef bqx2 WITH (NOLOCK)
                   ON BQ.Question_Code_2=BQX2.Question_Code
                 LEFT OUTER JOIN #Buyer_Current_Status_Effective_Date BCSED WITH (NOLOCK)
                   ON (BCSED.buyerid=B.BuyerID)
                 LEFT OUTER JOIN Dimbuyer DB with (nolock) on (DB.buyerid = B.Buyerid)
  
  
  --- Update BuyerStatusDescription

UPDATE FB
--SET BuyerStatusDescription = CASE WHEN FB.StatusCode IN ('NEW') THEN 'NEW'
--                             WHEN FB.StatusCode IN ('ACT','PRO') THEN 'ACTIVE'
--                             WHEN FB.StatusCode IN ('ARH','BAN','INA','OUT') THEN 'INACTIVE'
--                             WHEN FB.StatusCode IN ('DUP') THEN 'DUPLICATE' ELSE '' END
SET BuyerStatusDescription = CASE 
	WHEN StatusCode IN ('NEW') THEN 'NEW'
	WHEN StatusCode IN ('ACT') THEN 'ACTIVE'
	WHEN StatusCode IN ('PRO') THEN 'PROSPECTIVE'
	WHEN StatusCode IN ('ARH') THEN 'ARCHIVED'
	WHEN StatusCode IN ('BAN') THEN 'BANNED'
	WHEN StatusCode IN ('INA') THEN 'INACTIVE'
	WHEN StatusCode IN ('OUT') THEN 'OUT OF BUSINESS'
	WHEN StatusCode IN ('DUP') THEN 'DUPLICATE' 
	ELSE '' END
FROM  #Final_Buyer FB  WITH(NOLOCK)

          --SELECT * FROM #FINAL_BUYER
          BEGIN TRANSACTION

      MERGE [dbo].[DimBuyer] AS B USING #Final_Buyer AS B1 ON B.buyerid = B1.Buyerid WHEN MATCHED THEN 
      UPDATE SET B.BuyerName=Isnull(B1.BuyerName,''),
            B.BuyerTypeCode=Isnull(B1.BuyerTypeCode,''), 
            B.BuyerTypeDescription=Isnull(B1.BuyerTypeDescription,''), 
            B.PhoneExtension1=Isnull(B1.PhoneExtension1,0),
            B.PhoneExtension2=Isnull(B1.PhoneExtension2,0),
            B.PhoneNumber1=Isnull(B1.PhoneNumber1,''), 
            B.PhoneNumber2=Isnull(B1.PhoneNumber2,''), 
            B.HomeBranchNumber=Isnull(B1.HomeBranchNumber,0),
            B.BuyerInitialRegistrationDayID=Isnull(B1.InitialRegistrationDayID,29991231), 
            B.BuyerInitialRegistrationDateTime=Isnull(B1.InitialRegistrationDateTime,'12/31/2999'),
            B.BuyerInitialRegistrationRequestDayID=Isnull(B1.InitialRegistrationRequestDayID,29991231),
            B.BuyerInitialRegistrationRequestDateTime=Isnull(B1.InitialRegistrationRequestDateTime,'12/31/2999'), 
            B.ParentCompanyBuyerID=Isnull(B1.ParentCompanyBuyerID,0),
            B.IsPreferredBuyer=Isnull(B1.IsPreferredBuyer, 0),
            B.BuyerRegistrationEffectiveDayID=Isnull(B1.RegistrationEffectiveDayID,29991231), 
            B.BuyerRegistrationEffectiveDateTime=Isnull(B1.RegistrationEffectiveDateTime,'12/31/2999'),
            B.BuyerRegistrationExpirationDayID=Isnull(B1.RegistrationExpirationDayID,29991231), 
            B.BuyerRegistrationExpirationDateTime=Isnull(B1.RegistrationExpirationDateTime,'12/31/2999'),
            B.BuyerRegistrationRenewalDayID=Isnull(B1.RegistrationRenewalDayID,29991231),
            B.BuyerRegistrationRenewalDateTime=Isnull(B1.RegistrationRenewalDateTime,'12/31/2999'), 
            B.RegistrationSourceCode=Isnull(B1.RegistrationSourceCode,''),
            B.BuyerStatusCode=Isnull(B1.StatusCode,''),
            B.AddressLine1=Isnull(B1.AddressLine1,''), 
            B.AddressLine2=Isnull(B1.AddressLine2,''), 
            B.CityName=Isnull(B1.CityName,''), 
            B.StateAbbreviation=Isnull(B1.StateAbbreviation,''),
            B.ZipCode=Isnull(B1.ZipCode,''),
            B.CountryAbbreviation=Isnull(B1.CountryAbbreviation,''), 
            B.CountyName=Isnull(B1.CountyName,''), 
            B.EmailAddress=Isnull(B1.EmailAddress,''), 
            B.FaxNumber=Isnull(B1.FaxNumber,''),
            B.PrimaryContactName=Isnull(B1.PrimaryContactName,''), 
            B.PrimaryContactMethod=Isnull(B1.PrimaryContactMethod,''),
            B.PrimaryContactCellPhone=Isnull(B1.PrimaryContactCellPhone,''), 
            B.IsArchive=Isnull(B1.IsArchive, 0),
            B.LBSParentBuyerID=Isnull(B1.LBSParentBuyerID,0), 
            B.IsLBSParent=Isnull(B1.IsLBSParent,0), 
            B.IsRenege=Isnull(B1.IsRenege,0), 
            B.ASAPUserid=Isnull(B1.ASAPUserid,0),
            ETLLoadUpdateID =Isnull( @ETLLoadID,0),
            B.[IsOneDayGraceGrantedInd]=Isnull(B1.[IsOneDayGraceGrantedInd],0), 
            B.[IsCreditCardAllowedInd] =Isnull(B1.[IsCreditCardAllowedInd],0), 
            B.[BuyerFederalTaxID]=Isnull(B1.[BuyerFederalTaxID],0), 
            B.[BuyerMailAddressLine1]=Isnull(B1.[BuyerMailAddressLine1],''), 
            B.[BuyerMailAddressLine2]=Isnull(B1.[BuyerMailAddressLine2],''),
            B.[BuyerMailCountryAbbreviation]=Isnull(B1.[BuyerMailCountryAbbreviation],''), 
            B.[BuyerMailCountyName]=Isnull(B1.[BuyerMailCountyName],''),
            B.[BuyerMailStateAbbreviation]=Isnull(B1.[BuyerMailStateAbbreviation],''),
            B.[BuyerMailZipCode]=Isnull(B1.[BuyerMailZipCode],''), 
            B.[BuyerPrimaryPurpose]=Isnull( B1.[BuyerPrimaryPurpose],''),
            B.[BuyerSecondaryPurpose]=Isnull(B1.[BuyerSecondaryPurpose],''), 
            B.[BuyerPrimaryOther]=Isnull(B1.[BuyerPrimaryOther],''), 
            B.[BuyerSecondaryOther] =Isnull(B1.[BuyerSecondaryOther],''),
			B.[CountryDescription] = Isnull(B1.[CountryName],''),
	
            B.[IsBuyerCheckAllowed] = Isnull (B1.[IsBuyerCheckAllowed],0),
            B.[BuyerPromoCode] = Isnull (B1.[BuyerPromoCode],0) ,
            B.BuyerStatusDescription = Isnull (B1.BuyerStatusDescription,''),
			B.[BuyerMailCityName] = Isnull (B1.[BuyerMailCityName],''),
			B.IsForeignBuyer = Isnull (B1.IsForeignBuyer,0)
      WHEN NOT MATCHED THEN 
      INSERT ( Buyerid, BuyerName, BuyerTypeCode,
      BuyerTypeDescription,
      PhoneExtension1, PhoneExtension2, PhoneNumber1, PhoneNumber2, HomeBranchNumber, BuyerInitialRegistrationDayID, BuyerInitialRegistrationDateTime,
      BuyerInitialRegistrationRequestDayID, BuyerInitialRegistrationRequestDateTime, ParentCompanyBuyerID, IsPreferredBuyer,
      BuyerRegistrationEffectiveDayID, BuyerRegistrationEffectiveDateTime, BuyerRegistrationExpirationDayID, BuyerRegistrationExpirationDateTime,
      BuyerRegistrationRenewalDayID, buyerRegistrationRenewalDateTime, RegistrationSourceCode, BuyerStatusCode, AddressLine1, AddressLine2, CityName,
      StateAbbreviation, ZipCode, CountryAbbreviation, CountyName, EmailAddress, FaxNumber, PrimaryContactName, PrimaryContactMethod,
      PrimaryContactCellPhone, IsArchive, LBSParentBuyerID, IsLBSParent, IsRenege, ASAPUserid, ETLLoadID, ETLLoadUpdateID, IsOneDayGraceGrantedInd,
      IsCreditCardAllowedInd, BuyerFederalTaxID, BuyerMailAddressLine1, BuyerMailAddressLine2, BuyerMailCountryAbbreviation, BuyerMailCountyName,
      BuyerMailStateAbbreviation, BuyerMailZipCode, [BuyerPrimaryPurpose], [BuyerSecondaryPurpose], [BuyerPrimaryOther], [BuyerSecondaryOther], [CountryDescription],
    [IsBuyerCheckAllowed],[BuyerPromoCode],BuyerStatusDescription,BuyerMailCityName,IsForeignBuyer)
      VALUES
      ( Isnull(B1.Buyerid,0), 
      Isnull(B1.BuyerName,''), 
      Isnull(B1.BuyerTypeCode,''), 
      Isnull(B1.BuyerTypeDescription,''), 
      Isnull(B1.PhoneExtension1,0), 
      Isnull(B1.PhoneExtension2,0), 
      Isnull(B1.PhoneNumber1,''), 
      Isnull(B1.PhoneNumber2,''),
      Isnull(B1.HomeBranchNumber,0), 
      Isnull(B1.InitialRegistrationDayID,29991231), 
      Isnull(B1.InitialRegistrationDateTime,'12/31/2999'), 
      Isnull(B1.InitialRegistrationRequestDayID,29991231),
      Isnull(B1.InitialRegistrationRequestDateTime,'12/31/2999'), 
      Isnull(B1.ParentCompanyBuyerID,0), 
      Isnull(B1.IsPreferredBuyer,0), 
      Isnull(B1.RegistrationEffectiveDayID,29991231),
      Isnull(B1.RegistrationEffectiveDateTime,'12/31/2999'),
      Isnull(B1.RegistrationExpirationDayID,29991231), 
      Isnull(B1.RegistrationExpirationDateTime,'12/31/2999'), 
      Isnull(B1.RegistrationRenewalDayID,29991231), 
      Isnull(B1.RegistrationRenewalDateTime,'12/31/2999'),
      Isnull(B1.RegistrationSourceCode,''), 
      Isnull(B1.StatusCode,''),
      Isnull(B1.AddressLine1,''), 
      Isnull(B1.AddressLine2,''), 
      Isnull(B1.CityName,''), 
      Isnull(B1.StateAbbreviation,''), 
      Isnull(B1.ZipCode,''), 
      Isnull(B1.CountryAbbreviation,''), 
      Isnull(B1.CountyName,''), 
      Isnull(B1.EmailAddress,''),
      Isnull(B1.FaxNumber,''),
      Isnull(B1.PrimaryContactName,''), 
      Isnull(B1.PrimaryContactMethod,''), 
      Isnull(B1.PrimaryContactCellPhone,''), 
      Isnull(B1.IsArchive,0), 
      Isnull(B1.LBSParentBuyerID,0), 
      Isnull(B1.IsLBSParent,0), 
      Isnull(B1.IsRenege,0),
      Isnull(ASAPUserid,0), 
      @ETLLoadID, 
      @ETLLoadID, 
      Isnull(B1.IsOneDayGraceGrantedInd,0), 
      Isnull(B1.IsCreditCardAllowedInd,0), 
      Isnull(B1.BuyerFederalTaxID,0), 
      Isnull(B1.BuyerMailAddressLine1,''),
      Isnull(B1.BuyerMailAddressLine2,''), 
      Isnull(B1.BuyerMailCountryAbbreviation,''), 
      Isnull(B1.BuyerMailCountyName,''), 
      Isnull(B1.BuyerMailStateAbbreviation,''), 
      Isnull(B1.BuyerMailZipCode,''),
      Isnull(B1.[BuyerPrimaryPurpose],''),
      Isnull(B1.[BuyerSecondaryPurpose],''), 
      Isnull(B1.[BuyerPrimaryOther],''), 
      Isnull(B1.[BuyerSecondaryOther],'') ,
	  Isnull(B1.[CountryName],''),
      Isnull (B1.[IsBuyerCheckAllowed],0),
      '',
      Isnull (B1.BuyerStatusDescription,''),
	  Isnull (B1.[BuyerMailCityName],''),
	  Isnull (B1.IsForeignBuyer,0) 
      ) OUTPUT $action INTO #recordCount;

          /*********************************************************************************************
          Get the counts from the MERGE operation
          *********************************************************************************************/
          SELECT @UpdateRowCount=Isnull(Sum(CASE
                                              WHEN ChangeType='UPDATE' THEN 1
                                              ELSE 0
                                            END), 0),
                 @InsertRowCount=Isnull(Sum(CASE
                                              WHEN ChangeType='INSERT' THEN 1
                                              ELSE 0
                                            END), 0)
          FROM   #recordCount

          --SELECT *
          --FROM   dimbuyer
          /*********************************************************************************************
          Update DimBuyer With Buyer Question Changes
          *********************************************************************************************/
          UPDATE Dimbuyer
          SET    BuyerPrimaryPurpose=Isnull(bqx3.Question, ''),
                 BuyerSecondaryPurpose=Isnull(bqx4.Question, ''),
                 BuyerPrimaryOther=Isnull(BQQ.Other_Answer_1, ''),
                 BuyerSecondaryOther=Isnull(BQQ.Other_Answer_2, '')
          FROM   Dimbuyer WITH (nolock)
                 INNER JOIN #Buyer_Question BQQ WITH (nolock)
                   ON (Dimbuyer.buyerid=BQQ.buyerid)
                 LEFT OUTER JOIN [BI_ASAP_REP].dbo.Buyer_Question_XRef bqx3 WITH (NOLOCK)
                   ON BQQ.Question_Code_1=BQx3.Question_Code
                 LEFT OUTER JOIN [BI_ASAP_REP].dbo.Buyer_Question_XRef bqx4 WITH (NOLOCK)
                   ON BQQ.Question_Code_2=BQX4.Question_Code

          /*********************************************************************************************
          Update DimBuyer With Buyer Registration Changes
          *********************************************************************************************/
          UPDATE Dimbuyer
          SET    BuyerRegistrationEffectiveDayID=Isnull(BR.RegistrationEffectiveDayID, 29991231),
                 BuyerRegistrationEffectiveDateTime=Isnull(BR.RegistrationEffectiveDate, '2999-12-31 00:00:00.000'),
                 BuyerRegistrationExpirationDayID=Isnull(BR.RegistrationExpirationDayID, 29991231),
                 BuyerRegistrationExpirationDatetime=Isnull(BR.RegistrationExpirationDate, '2999-12-31 00:00:00.000')
          FROM   Dimbuyer WITH (nolock)
                 INNER JOIN #Buyer_Registration BR WITH (nolock)
                   ON (Dimbuyer.buyerid=br.buyerid)

          /*********************************************************************************************
          Update DimBuyer With Buyer RenewalChanges
          *********************************************************************************************/
          UPDATE DimBuyer
          SET    BuyerRegistrationRenewalDateTime=Isnull(BC.Charge_DateTime, '2999-12-31 00:00:00.000'),
                 BuyerRegistrationRenewalDayID=Isnull(CONVERT(INT, CONVERT(CHAR(8), BC.Charge_DateTime, 112)), 29991231)
          FROM   DimBuyer WITH (NOLOCK)
                 INNER JOIN #Buyer_Charge BC WITH (NOLOCK)
                   ON (DimBuyer.buyerid=bc.buyerid)
           
		/*********************************************************************************************
          Update DimBuyer With Buyer Current Effective Datetime
          *********************************************************************************************/
		    UPDATE DimBuyer
          SET    BuyerCurrentStatusEffectiveDatetime=isnull(BCSED.Buyer_Current_Status_Effective_Date, '2999-12-31 00:00:00.000'),
                 BuyerCurrentStatusEffectiveDayID=isnull(BCSED.Buyer_Current_Status_Effective_DayID ,29991231)
				           FROM   DimBuyer WITH (NOLOCK)
                 INNER JOIN  #Buyer_Current_Status_Effective_Date BCSED WITH (NOLOCK)
                   ON (DimBuyer.buyerid=BCSED.buyerid)
                  
          /*********************************************************************************************
          Update DimBuyer With User Changes
          *********************************************************************************************/  
              
             UPDATE DimBuyer      
             Set 
             PrimaryContactMethod =  ISNULL(BUI.[Primary_Contact_Method_Code],'') ,
             EmailAddress = ISNULL(coalesce (BUI.email_address_1,BUI.email_Address_2),''),
             PrimaryContactName = Isnull(BUI.first_name, '')+' '+Isnull(BUI.last_name, ''),
             PrimaryContactCellPhone = Isnull(BUI.[Mobile_Phone_Number],0)             
             FROM Dimbuyer DB with (nolock) inner join #Buyer_User_info BUI with (nolock)
             ON (DB.BuyerID = BUI.buyerid)
           

          /*********************************************************************************************
          Update DimBuyer Emails
          *********************************************************************************************/  
			UPDATE DB SET EmailAddress = IIF(UI.Primary_Contact_Method_Code = 'PE', ISNULL(coalesce(UI.email_address_2,UI.email_Address_1),''),ISNULL(coalesce(UI.email_address_1,UI.email_Address_2),''))
			--SELECT db.EmailAddress , UI.EMail_Address_1,UI.EMail_Address_2,UI.Primary_Contact_Method_Code,
			--IIF(UI.Primary_Contact_Method_Code = 'PE', ISNULL(coalesce(UI.email_address_2,UI.email_Address_1),''),ISNULL(coalesce(UI.email_address_1,UI.email_Address_2),'')) AS NEWEmail
			FROM [BI_ASAP_Rep].dbo.buyer_employee AS BE
			INNER JOIN [BI_ASAP_Rep].dbo.buyer B WITH (NOLOCK)
				ON BE.Buyer_ID = B.Buyer_ID
			INNER JOIN EDW.dbo.DIMBuyer DB WITH (NOLOCK)
				ON DB.BuyerID = B.Buyer_ID	 
			  INNER join [BI_ASAP_REP].dbo.User_Association UA WITH (NOLOCK)
			  on B.Primary_Contact_ID = UA.Buyer_Employee_ID
			  left outer join [BI_ASAP_REP].dbo.User_Info UI WITH (NOLOCK)
			  on UA.[User_ID] = UI.[User_ID]
			  where db.EmailAddress <> IIF(UI.Primary_Contact_Method_Code = 'PE', ISNULL(coalesce(UI.email_address_2,UI.email_Address_1),''),ISNULL(coalesce(UI.email_address_1,UI.email_Address_2),''))

			UPDATE DB SET EmailAddress = IIF(UI.Primary_Contact_Method_Code = 'PE', ISNULL(coalesce(UI.email_address_2,UI.email_Address_1),''),ISNULL(coalesce(UI.email_address_1,UI.email_Address_2),''))
			--SELECT db.EmailAddress , UI.EMail_Address_1,UI.EMail_Address_2,UI.Primary_Contact_Method_Code,
			--IIF(UI.Primary_Contact_Method_Code = 'PE', ISNULL(coalesce(UI.email_address_2,UI.email_Address_1),''),ISNULL(coalesce(UI.email_address_1,UI.email_Address_2),'')) AS NEWEmail
			FROM [BI_ASAP_Rep].dbo.buyer_employee AS BE
			INNER JOIN [BI_ASAP_Rep].dbo.buyer B WITH (NOLOCK)
				ON BE.Buyer_ID = B.Buyer_ID
			INNER JOIN EDW.dbo.DIMBuyer DB WITH (NOLOCK)
				ON DB.BuyerID = B.Buyer_ID
			INNER JOIN (SELECT DISTINCT BE.Buyer_Employee_ID
						FROM [BI_ASAP_Rep].dbo.buyer_employee AS BE
						INNER JOIN [BI_ASAP_Rep].dbo.buyer B WITH (NOLOCK)
							ON BE.Buyer_ID = B.Buyer_ID
						left outer join [BI_ASAP_REP].dbo.User_Association UA WITH (NOLOCK)
							on B.Primary_Contact_ID = UA.Buyer_Employee_ID
						WHERE UA.Buyer_Employee_ID IS NULL and BE.Job_Title_Code = 'OWNR'
						and BE.Status_Code = 'ACT') OWNR	
				ON BE.Buyer_Employee_ID = OWNR.Buyer_Employee_ID		 
			left outer join [BI_ASAP_REP].dbo.User_Association UA WITH (NOLOCK)
				on BE.Buyer_Employee_ID = UA.Buyer_Employee_ID
			left outer join [BI_ASAP_REP].dbo.User_Info UI WITH (NOLOCK)
				on UA.[User_ID] = UI.[User_ID]
			where db.EmailAddress <> IIF(UI.Primary_Contact_Method_Code = 'PE', ISNULL(coalesce(UI.email_address_2,UI.email_Address_1),''),ISNULL(coalesce(UI.email_address_1,UI.email_Address_2),''))

         /*********************************************************************************************
          Update DimBuyer With Promo Code Change
          *********************************************************************************************/     
           
           UPDATE DimBuyer 
			Set BuyerPromoCode = isnull(BP.[BuyerPromoCode],'')
			from DimBuyer B With (Nolock) Inner join #Buyer_Promo BP
			on (B.BuyerID = BP.BuyerID and RAN =1)
           
         /*********************************************************************************************
          Update IsForeignBuyer
          *********************************************************************************************/      
			 UPDATE dbo.DimBuyer
			SET		IsForeignBuyer = 0

			UPDATE	dbo.DimBuyer
			SET		IsForeignBuyer = 1
			WHERE	CountryAbbreviation NOT IN ('US', 'XX')

			UPDATE	dbo.DimBuyer
			SET		IsForeignBuyer = 1
			FROM	dbo.DimBuyer db with (NOLOCK)
			INNER JOIN (SELECT DISTINCT	Buyer_Id
						FROM BI_ASAP_REP.dbo.Buyer_License BL WITH (NOLOCK)
						WHERE License_Type_Code IN ('USP', 'EXP') AND (Expiration_Date >= GETDATE() OR Expiration_Date IS NULL)) a
				ON db.BuyerID = a.Buyer_ID
           

          EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ExtractRowCount = @ExtractRowCount,@InsertRowCount = @InsertRowCount,
          @UpdateRowCount = @UpdateRowCount

          COMMIT TRAN
          
          IF OBJECT_ID('tempdb..#Buyer1') IS NOT NULL DROP TABLE #Buyer1
          IF OBJECT_ID('tempdb..#Buyer_Registration') IS NOT NULL DROP TABLE #Buyer_Registration
          IF OBJECT_ID('tempdb..#Buyer_Charge') IS NOT NULL DROP TABLE #Buyer_Charge
          IF OBJECT_ID('tempdb..#Buyer_Charge_insert') IS NOT NULL DROP TABLE #Buyer_Charge_insert
          IF OBJECT_ID('tempdb..#Buyer_Question') IS NOT NULL DROP TABLE #Buyer_Question
          IF OBJECT_ID('tempdb..#Final_Buyer') IS NOT NULL DROP TABLE #Final_Buyer
          IF OBJECT_ID('tempdb..#recordCount') IS NOT NULL DROP TABLE #recordCount
		  IF OBJECT_ID('tempdb..#Buyer_Current_Status_Effective_Date') IS NOT NULL DROP TABLE #Buyer_Current_Status_Effective_Date
		  IF OBJECT_ID('tempdb..#Buyer_Promo') IS NOT NULL DROP TABLE #Buyer_Promo
		  IF OBJECT_ID('tempdb..#Buyer_User_info') IS NOT NULL DROP TABLE #Buyer_User_info

          RETURN 0
      END TRY

      BEGIN CATCH
          SET NOCOUNT OFF

          ROLLBACK TRAN

          EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ExtractRowCount = @ExtractRowCount,@InsertRowCount = @InsertRowCount,
          @UpdateRowCount = @UpdateRowCount,@ErrorCode = @@ERROR

          DECLARE @ErrorMessage  NVARCHAR(4000),
                  @ErrorSeverity INT,
                  @ErrorState    INT;

          SELECT @ErrorMessage=Error_message(),
                 @ErrorSeverity=Error_severity(),
                 @ErrorState=Error_state();

          RAISERROR (@ErrorMessage,
                     @ErrorSeverity,
                     @ErrorState);
                     
          IF OBJECT_ID('tempdb..#Buyer1') IS NOT NULL DROP TABLE #Buyer1
          IF OBJECT_ID('tempdb..#Buyer_Registration') IS NOT NULL DROP TABLE #Buyer_Registration
          IF OBJECT_ID('tempdb..#Buyer_Charge') IS NOT NULL DROP TABLE #Buyer_Charge
          IF OBJECT_ID('tempdb..#Buyer_Charge_insert') IS NOT NULL DROP TABLE #Buyer_Charge_insert
          IF OBJECT_ID('tempdb..#Buyer_Question') IS NOT NULL DROP TABLE #Buyer_Question
          IF OBJECT_ID('tempdb..#Final_Buyer') IS NOT NULL DROP TABLE #Final_Buyer
          IF OBJECT_ID('tempdb..#recordCount') IS NOT NULL DROP TABLE #recordCount
          IF OBJECT_ID('tempdb..#Buyer_Current_Status_Effective_Date') IS NOT NULL DROP TABLE #Buyer_Current_Status_Effective_Date
		  IF OBJECT_ID('tempdb..#Buyer_Promo') IS NOT NULL DROP TABLE #Buyer_Promo
		  IF OBJECT_ID('tempdb..#Buyer_User_info') IS NOT NULL DROP TABLE #Buyer_User_info
          

          RETURN -1
      END CATCH
  END

