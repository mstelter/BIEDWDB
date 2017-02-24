

/*
==============================================================================
Author		: Mohammad Waheed
Create date	: 09/13/2016
Description	: Parse the XML data for file Envirnmental and Load it to the [EDW].[DBO].OHAInformation
==============================================================================
REVISION History
ChangeDate	Developer		Release/Problem Number	

==============================================================================
Usage Example:
--Exec [dbo].[usp_OHALoadXMLDataToOHAInformation_S]
--EXEC EDW.[dbo].[usp_LoadXMLData] 
Select * from Edw.[dbo].[OHALoadRawXML]
--Truncate table EDW.[dbo].[OHALoadRawXML]
Select * from EDW.[dbo].[OHAFormsXMLData]
--Truncate table EDW.[dbo].[OHAFormsXMLData]
Select * from [EDW].[DBO].OHAInformation
--Truncate Table  [EDW].[DBO].OHAInformation
--Drop Table #XmlData
==============================================================================

*/
Create PROCEDURE [dbo].[usp_OHALoadXMLDataToOHAInformation_S]
AS
  BEGIN

--select *
delete 
FROM [EDW].[DBO].OHAInformation  WHERE   Id in (	select oi.id
														from [EDW].[DBO].OHAInformation oi
															inner join EDW.[dbo].[OHALoadRawXML] lrx 
															          on  oi.[QtrYear]=lrx.[QtrYear] and
																	      oi.[BranchName]=lrx.[BranchName] and
												                          oi.FileModifiedDateTime<lrx.FileModifiedDateTime and
																		  oi.FileType=lrx.FileType
													   )



SELECT 
     FileType
    ,BranchName
	,QtrYear
	,FileModifiedDateTime
	
------------------------<S1>------------------------------
,x.value('(/myFields/S1Q1)[1]','varchar(2000)') AS S1_Q1Question
,x.value('(/myFields/S1Q2)[1]','varchar(2000)') AS S1_Q2Question
,x.value('(/myFields/S1Q3)[1]','varchar(2000)') AS S1_Q3Question
,x.value('(/myFields/S1Q4)[1]','varchar(2000)') AS S1_Q4Question
,x.value('(/myFields/S1Q5)[1]','varchar(2000)') AS S1_Q5Question
,x.value('(/myFields/S1Q6)[1]','varchar(2000)') AS S1_Q6Question
,x.value('(/myFields/S1Q7)[1]','varchar(2000)') AS S1_Q7Question
,x.value('(/myFields/S1Q8)[1]','varchar(2000)') AS S1_Q8Question
,x.value('(/myFields/S1Q9)[1]','varchar(2000)') AS S1_Q9Question
,x.value('(/myFields/S1Q10)[1]','varchar(2000)') AS S1_Q10Question
,x.value('(/myFields/S1Q11)[1]','varchar(2000)') AS S1_Q11Question
,x.value('(/myFields/S1Q12)[1]','varchar(2000)') AS S1_Q12Question
,x.value('(/myFields/S1Q13)[1]','varchar(2000)') AS S1_Q13Question
,x.value('(/myFields/S1Q14)[1]','varchar(2000)') AS S1_Q14Question
,x.value('(/myFields/S1Q15)[1]','varchar(2000)') AS S1_Q15Question
,x.value('(/myFields/S1Q16)[1]','varchar(2000)') AS S1_Q16Question
,x.value('(/myFields/S1Q17)[1]','varchar(2000)') AS S1_Q17Question
,x.value('(/myFields/S1Q18)[1]','varchar(2000)') AS S1_Q18Question
,x.value('(/myFields/S1Q19)[1]','varchar(2000)') AS S1_Q19Question
,x.value('(/myFields/S1Q20)[1]','varchar(2000)') AS S1_Q20Question
,x.value('(/myFields/S1Q21)[1]','varchar(2000)') AS S1_Q21Question
,x.value('(/myFields/S1Q22)[1]','varchar(2000)') AS S1_Q22Question
,x.value('(/myFields/S1Q23)[1]','varchar(2000)') AS S1_Q23Question


,x.value('(/myFields/S1/Q1YN)[1]','Int') AS S1_Q1Answer
,x.value('(/myFields/S1/Q2YN)[1]','Int') AS S1_Q2Answer
,x.value('(/myFields/S1/Q3YN)[1]','Int') AS S1_Q3Answer
,x.value('(/myFields/S1/Q4YN)[1]','Int') AS S1_Q4Answer
,x.value('(/myFields/S1/Q5YN)[1]','Int') AS S1_Q5Answer
,x.value('(/myFields/S1/Q6YN)[1]','Int') AS S1_Q6Answer
,x.value('(/myFields/S1/Q7YN)[1]','Int') AS S1_Q7Answer
,x.value('(/myFields/S1/Q8YN)[1]','Int') AS S1_Q8Answer
,x.value('(/myFields/S1/Q9YN)[1]','Int') AS S1_Q9Answer
,x.value('(/myFields/S1/Q10YN)[1]','Int') AS S1_Q10Answer
,x.value('(/myFields/S1/Q11YN)[1]','Int') AS S1_Q11Answer
,x.value('(/myFields/S1/Q12YN)[1]','Int') AS S1_Q12Answer
,x.value('(/myFields/S1/Q13YN)[1]','Int') AS S1_Q13Answer
,x.value('(/myFields/S1/Q14YN)[1]','Int') AS S1_Q14Answer
,x.value('(/myFields/S1/Q15YN)[1]','Int') AS S1_Q15Answer
,x.value('(/myFields/S1/Q16YN)[1]','Int') AS S1_Q16Answer
,x.value('(/myFields/S1/Q17YN)[1]','Int') AS S1_Q17Answer
,x.value('(/myFields/S1/Q18YN)[1]','Int') AS S1_Q18Answer
,x.value('(/myFields/S1/Q19YN)[1]','Int') AS S1_Q19Answer
,x.value('(/myFields/S1/Q20YN)[1]','Int') AS S1_Q20Answer		
,x.value('(/myFields/S1/Q21YN)[1]','Int') AS S1_Q21Answer
,x.value('(/myFields/S1/Q22YN)[1]','Int') AS S1_Q22Answer		
,x.value('(/myFields/S1/Q23YN)[1]','Int') AS S1_Q23Answer		
		


,x.value('(/myFields/S1Q1S)[1]','Float') AS S1_Q1Score	
,x.value('(/myFields/S1Q2S)[1]','Float') AS S1_Q2Score	
,x.value('(/myFields/S1Q3S)[1]','Float') AS S1_Q3Score	
,x.value('(/myFields/S1Q4S)[1]','Float') AS S1_Q4Score	
,x.value('(/myFields/S1Q5S)[1]','Float') AS S1_Q5Score	
,x.value('(/myFields/S1Q6S)[1]','Float') AS S1_Q6Score	
,x.value('(/myFields/S1Q7S)[1]','Float') AS S1_Q7Score	
,x.value('(/myFields/S1Q8S)[1]','Float') AS S1_Q8Score	
,x.value('(/myFields/S1Q9S)[1]','Float') AS S1_Q9Score	
,x.value('(/myFields/S1Q10S)[1]','Float') AS S1_Q10Score	
,x.value('(/myFields/S1Q11S)[1]','Float') AS S1_Q11Score
,x.value('(/myFields/S1Q12S)[1]','Float') AS S1_Q12Score	
,x.value('(/myFields/S1Q13S)[1]','Float') AS S1_Q13Score	
,x.value('(/myFields/S1Q14S)[1]','Float') AS S1_Q14Score	
,x.value('(/myFields/S1Q15S)[1]','Float') AS S1_Q15Score	
,x.value('(/myFields/S1Q16S)[1]','Float') AS S1_Q16Score	
,x.value('(/myFields/S1Q17S)[1]','Float') AS S1_Q17Score	
,x.value('(/myFields/S1Q18S)[1]','Float') AS S1_Q18Score
,x.value('(/myFields/S1Q19S)[1]','Float') AS S1_Q19Score	
,x.value('(/myFields/S1Q20S)[1]','Float') AS S1_Q20Score	
,x.value('(/myFields/S1Q21S)[1]','Float') AS S1_Q21Score
,x.value('(/myFields/S1Q22S)[1]','Float') AS S1_Q22Score	
,x.value('(/myFields/S1Q23S)[1]','Float') AS S1_Q23Score

,x.value('(/myFields/S1/Q1OPP)[1]','varchar(2000)') AS S1_Q1Comment
,x.value('(/myFields/S1/Q2OPP)[1]','varchar(2000)') AS S1_Q2Comment
,x.value('(/myFields/S1/Q3OPP)[1]','varchar(2000)') AS S1_Q3Comment
,x.value('(/myFields/S1/Q4OPP)[1]','varchar(2000)') AS S1_Q4Comment
,x.value('(/myFields/S1/Q5OPP)[1]','varchar(2000)') AS S1_Q5Comment
,x.value('(/myFields/S1/Q6OPP)[1]','varchar(2000)') AS S1_Q6Comment
,x.value('(/myFields/S1/Q7OPP)[1]','varchar(2000)') AS S1_Q7Comment
,x.value('(/myFields/S1/Q8OPP)[1]','varchar(2000)') AS S1_Q8Comment
,x.value('(/myFields/S1/Q9OPP)[1]','varchar(2000)') AS S1_Q9Comment
,x.value('(/myFields/S1/Q10OPP)[1]','varchar(2000)') AS S1_Q10Comment
,x.value('(/myFields/S1/Q11OPP)[1]','varchar(2000)') AS S1_Q11Comment
,x.value('(/myFields/S1/Q12OPP)[1]','varchar(2000)') AS S1_Q12Comment
,x.value('(/myFields/S1/Q13OPP)[1]','varchar(2000)') AS S1_Q13Comment
,x.value('(/myFields/S1/Q14OPP)[1]','varchar(2000)') AS S1_Q14Comment
,x.value('(/myFields/S1/Q15OPP)[1]','varchar(2000)') AS S1_Q15Comment
,x.value('(/myFields/S1/Q16OPP)[1]','varchar(2000)') AS S1_Q16Comment
,x.value('(/myFields/S1/Q17OPP)[1]','varchar(2000)') AS S1_Q17Comment
,x.value('(/myFields/S1/Q18OPP)[1]','varchar(2000)') AS S1_Q18Comment
,x.value('(/myFields/S1/Q19OPP)[1]','varchar(2000)') AS S1_Q19Comment
,x.value('(/myFields/S1/Q20OPP)[1]','varchar(2000)') AS S1_Q20Comment
,x.value('(/myFields/S1/Q21OPP)[1]','varchar(2000)') AS S1_Q21Comment
,x.value('(/myFields/S1/Q22OPP)[1]','varchar(2000)') AS S1_Q22Comment
,x.value('(/myFields/S1/Q23OPP)[1]','varchar(2000)') AS S1_Q23Comment
------------------------</S1>----------------------------------------


-------------------------<S2>----------------------------------------
,x.value('(/myFields/S2Q1)[1]','varchar(2000)') AS S2_Q1Question
,x.value('(/myFields/S2Q2)[1]','varchar(2000)') AS S2_Q2Question
,x.value('(/myFields/S2Q3)[1]','varchar(2000)') AS S2_Q3Question
,x.value('(/myFields/S2Q4)[1]','varchar(2000)') AS S2_Q4Question
,x.value('(/myFields/S2Q5)[1]','varchar(2000)') AS S2_Q5Question
,x.value('(/myFields/S2Q6)[1]','varchar(2000)') AS S2_Q6Question
,x.value('(/myFields/S2Q7)[1]','varchar(2000)') AS S2_Q7Question
,x.value('(/myFields/S2Q8)[1]','varchar(2000)') AS S2_Q8Question
,x.value('(/myFields/S2Q9)[1]','varchar(2000)') AS S2_Q9Question
,x.value('(/myFields/S2Q10)[1]','varchar(2000)') AS S2_Q10Question
,x.value('(/myFields/S2Q11)[1]','varchar(2000)') AS S2_Q11Question
,x.value('(/myFields/S2Q12)[1]','varchar(2000)') AS S2_Q12Question
,x.value('(/myFields/S2Q13)[1]','varchar(2000)') AS S2_Q13Question
,x.value('(/myFields/S2Q14)[1]','varchar(2000)') AS S2_Q14Question
,x.value('(/myFields/S2Q15)[1]','varchar(2000)') AS S2_Q15Question
,x.value('(/myFields/S2Q16)[1]','varchar(2000)') AS S2_Q16Question


,x.value('(/myFields/S2/S2Q1YN)[1]','Int') AS S2_Q1Answer
,x.value('(/myFields/S2/S2Q2YN)[1]','Int') AS S2_Q2Answer
,x.value('(/myFields/S2/S2Q3YN)[1]','Int') AS S2_Q3Answer
,x.value('(/myFields/S2/S2Q4YN)[1]','Int') AS S2_Q4Answer
,x.value('(/myFields/S2/S2Q5YN)[1]','Int') AS S2_Q5Answer
,x.value('(/myFields/S2/S2Q6YN)[1]','Int') AS S2_Q6Answer
,x.value('(/myFields/S2/S2Q7YN)[1]','Int') AS S2_Q7Answer
,x.value('(/myFields/S2/S2Q8YN)[1]','Int') AS S2_Q8Answer
,x.value('(/myFields/S2/S2Q9YN)[1]','Int') AS S2_Q9Answer
,x.value('(/myFields/S2/S2Q10YN)[1]','Int') AS S2_Q10Answer
,x.value('(/myFields/S2/S2Q11YN)[1]','Int') AS S2_Q11Answer
,x.value('(/myFields/S2/S2Q12YN)[1]','Int') AS S2_Q12Answer
,x.value('(/myFields/S2/S2Q13YN)[1]','Int') AS S2_Q13Answer
,x.value('(/myFields/S2/S2Q14YN)[1]','Int') AS S2_Q14Answer
,x.value('(/myFields/S2/S2Q15YN)[1]','Int') AS S2_Q15Answer
,x.value('(/myFields/S2/S2Q16YN)[1]','Int') AS S2_Q16Answer


,x.value('(/myFields/S2Q1S)[1]','Float') AS S2_Q1Score	
,x.value('(/myFields/S2Q2S)[1]','Float') AS S2_Q2Score	
,x.value('(/myFields/S2Q3S)[1]','Float') AS S2_Q3Score	
,x.value('(/myFields/S2Q4S)[1]','Float') AS S2_Q4Score	
,x.value('(/myFields/S2Q5S)[1]','Float') AS S2_Q5Score	
,x.value('(/myFields/S2Q6S)[1]','Float') AS S2_Q6Score	
,x.value('(/myFields/S2Q7S)[1]','Float') AS S2_Q7Score	
,x.value('(/myFields/S2Q8S)[1]','Float') AS S2_Q8Score	
,x.value('(/myFields/S2Q9S)[1]','Float') AS S2_Q9Score	
,x.value('(/myFields/S2Q10S)[1]','Float') AS S2_Q10Score	
,x.value('(/myFields/S2Q11S)[1]','Float') AS S2_Q11Score
,x.value('(/myFields/S2Q12S)[1]','Float') AS S2_Q12Score	
,x.value('(/myFields/S2Q13S)[1]','Float') AS S2_Q13Score	
,x.value('(/myFields/S2Q14S)[1]','Float') AS S2_Q14Score	
,x.value('(/myFields/S2Q15S)[1]','Float') AS S2_Q15Score	
,x.value('(/myFields/S2Q16S)[1]','Float') AS S2_Q16Score	


,x.value('(/myFields/S2/S2Q1OPP)[1]','varchar(2000)') AS S2_Q1Comment
,x.value('(/myFields/S2/S2Q2OPP)[1]','varchar(2000)') AS S2_Q2Comment
,x.value('(/myFields/S2/S2Q3OPP)[1]','varchar(2000)') AS S2_Q3Comment
,x.value('(/myFields/S2/S2Q4OPP)[1]','varchar(2000)') AS S2_Q4Comment
,x.value('(/myFields/S2/S2Q5OPP)[1]','varchar(2000)') AS S2_Q5Comment
,x.value('(/myFields/S2/S2Q6OPP)[1]','varchar(2000)') AS S2_Q6Comment
,x.value('(/myFields/S2/S2Q7OPP)[1]','varchar(2000)') AS S2_Q7Comment
,x.value('(/myFields/S2/S2Q8OPP)[1]','varchar(2000)') AS S2_Q8Comment
,x.value('(/myFields/S2/S2Q9OPP)[1]','varchar(2000)') AS S2_Q9Comment
,x.value('(/myFields/S2/S2Q10OPP)[1]','varchar(2000)') AS S2_Q10Comment
,x.value('(/myFields/S2/S2Q11OPP)[1]','varchar(2000)') AS S2_Q11Comment
,x.value('(/myFields/S2/S2Q12OPP)[1]','varchar(2000)') AS S2_Q12Comment
,x.value('(/myFields/S2/S2Q13OPP)[1]','varchar(2000)') AS S2_Q13Comment
,x.value('(/myFields/S2/S2Q14OPP)[1]','varchar(2000)') AS S2_Q14Comment
,x.value('(/myFields/S2/S2Q15OPP)[1]','varchar(2000)') AS S2_Q15Comment
,x.value('(/myFields/S2/S2Q16OPP)[1]','varchar(2000)') AS S2_Q16Comment
------------------------</S2>----------------------------------------

-------------------------<S3>----------------------------------------
,x.value('(/myFields/S3Q1)[1]','varchar(2000)') AS S3_Q1Question
,x.value('(/myFields/S3Q2)[1]','varchar(2000)') AS S3_Q2Question
,x.value('(/myFields/S3Q3)[1]','varchar(2000)') AS S3_Q3Question
,x.value('(/myFields/S3Q4)[1]','varchar(2000)') AS S3_Q4Question
,x.value('(/myFields/S3Q5)[1]','varchar(2000)') AS S3_Q5Question
,x.value('(/myFields/S3Q6)[1]','varchar(2000)') AS S3_Q6Question
,x.value('(/myFields/S3Q7)[1]','varchar(2000)') AS S3_Q7Question
,x.value('(/myFields/S3Q8)[1]','varchar(2000)') AS S3_Q8Question
,x.value('(/myFields/S3Q9)[1]','varchar(2000)') AS S3_Q9Question
,x.value('(/myFields/S3Q10)[1]','varchar(2000)') AS S3_Q10Question
,x.value('(/myFields/S3Q11)[1]','varchar(2000)') AS S3_Q11Question
,x.value('(/myFields/S3Q12)[1]','varchar(2000)') AS S3_Q12Question
,x.value('(/myFields/S3Q13)[1]','varchar(2000)') AS S3_Q13Question
,x.value('(/myFields/S3Q14)[1]','varchar(2000)') AS S3_Q14Question
,x.value('(/myFields/S3Q15)[1]','varchar(2000)') AS S3_Q15Question
,x.value('(/myFields/S3Q16)[1]','varchar(2000)') AS S3_Q16Question
,x.value('(/myFields/S3Q17)[1]','varchar(2000)') AS S3_Q17Question
,x.value('(/myFields/S3Q18)[1]','varchar(2000)') AS S3_Q18Question
,x.value('(/myFields/S3Q19)[1]','varchar(2000)') AS S3_Q19Question
,x.value('(/myFields/S3Q20)[1]','varchar(2000)') AS S3_Q20Question
,x.value('(/myFields/S3Q21)[1]','varchar(2000)') AS S3_Q21Question

,x.value('(/myFields/S3/S3Q1YN)[1]','Int') AS S3_Q1Answer
,x.value('(/myFields/S3/S3Q2YN)[1]','Int') AS S3_Q2Answer
,x.value('(/myFields/S3/S3Q3YN)[1]','Int') AS S3_Q3Answer
,x.value('(/myFields/S3/S3Q4YN)[1]','Int') AS S3_Q4Answer
,x.value('(/myFields/S3/S3Q5YN)[1]','Int') AS S3_Q5Answer
,x.value('(/myFields/S3/S3Q6YN)[1]','Int') AS S3_Q6Answer
,x.value('(/myFields/S3/S3Q7YN)[1]','Int') AS S3_Q7Answer
,x.value('(/myFields/S3/S3Q8YN)[1]','Int') AS S3_Q8Answer
,x.value('(/myFields/S3/S3Q9YN)[1]','Int') AS S3_Q9Answer
,x.value('(/myFields/S3/S3Q10YN)[1]','Int') AS S3_Q10Answer
,x.value('(/myFields/S3/S3Q11YN)[1]','Int') AS S3_Q11Answer
,x.value('(/myFields/S3/S3Q12YN)[1]','Int') AS S3_Q12Answer
,x.value('(/myFields/S3/S3Q13YN)[1]','Int') AS S3_Q13Answer
,x.value('(/myFields/S3/S3Q14YN)[1]','Int') AS S3_Q14Answer
,x.value('(/myFields/S3/S3Q15YN)[1]','Int') AS S3_Q15Answer
,x.value('(/myFields/S3/S3Q16YN)[1]','Int') AS S3_Q16Answer
,x.value('(/myFields/S3/S3Q17YN)[1]','Int') AS S3_Q17Answer
,x.value('(/myFields/S3/S3Q18YN)[1]','Int') AS S3_Q18Answer
,x.value('(/myFields/S3/S3Q19YN)[1]','Int') AS S3_Q19Answer
,x.value('(/myFields/S3/S3Q20YN)[1]','Int') AS S3_Q20Answer
,x.value('(/myFields/S3/S3Q21YN)[1]','Int') AS S3_Q21Answer

,x.value('(/myFields/S3Q1S)[1]','Float') AS S3_Q1Score	
,x.value('(/myFields/S3Q2S)[1]','Float') AS S3_Q2Score	
,x.value('(/myFields/S3Q3S)[1]','Float') AS S3_Q3Score	
,x.value('(/myFields/S3Q4S)[1]','Float') AS S3_Q4Score	
,x.value('(/myFields/S3Q5S)[1]','Float') AS S3_Q5Score	
,x.value('(/myFields/S3Q6S)[1]','Float') AS S3_Q6Score	
,x.value('(/myFields/S3Q7S)[1]','Float') AS S3_Q7Score	
,x.value('(/myFields/S3Q8S)[1]','Float') AS S3_Q8Score	
,x.value('(/myFields/S3Q9S)[1]','Float') AS S3_Q9Score	
,x.value('(/myFields/S3Q10S)[1]','Float') AS S3_Q10Score	
,x.value('(/myFields/S3Q11S)[1]','Float') AS S3_Q11Score
,x.value('(/myFields/S3Q12S)[1]','Float') AS S3_Q12Score	
,x.value('(/myFields/S3Q13S)[1]','Float') AS S3_Q13Score	
,x.value('(/myFields/S3Q14S)[1]','Float') AS S3_Q14Score	
,x.value('(/myFields/S3Q15S)[1]','Float') AS S3_Q15Score	
,x.value('(/myFields/S3Q16S)[1]','Float') AS S3_Q16Score	
,x.value('(/myFields/S3Q17S)[1]','Float') AS S3_Q17Score	
,x.value('(/myFields/S3Q18S)[1]','Float') AS S3_Q18Score
,x.value('(/myFields/S3Q19S)[1]','Float') AS S3_Q19Score	
,x.value('(/myFields/S3Q20S)[1]','Float') AS S3_Q20Score	
,x.value('(/myFields/S3Q21S)[1]','Float') AS S3_Q21Score

,x.value('(/myFields/S3/S3Q1OPP)[1]','varchar(2000)') AS S3_Q1Comment
,x.value('(/myFields/S3/S3Q2OPP)[1]','varchar(2000)') AS S3_Q2Comment
,x.value('(/myFields/S3/S3Q3OPP)[1]','varchar(2000)') AS S3_Q3Comment
,x.value('(/myFields/S3/S3Q4OPP)[1]','varchar(2000)') AS S3_Q4Comment
,x.value('(/myFields/S3/S3Q5OPP)[1]','varchar(2000)') AS S3_Q5Comment
,x.value('(/myFields/S3/S3Q6OPP)[1]','varchar(2000)') AS S3_Q6Comment
,x.value('(/myFields/S3/S3Q7OPP)[1]','varchar(2000)') AS S3_Q7Comment
,x.value('(/myFields/S3/S3Q8OPP)[1]','varchar(2000)') AS S3_Q8Comment
,x.value('(/myFields/S3/S3Q9OPP)[1]','varchar(2000)') AS S3_Q9Comment
,x.value('(/myFields/S3/S3Q10OPP)[1]','varchar(2000)') AS S3_Q10Comment
,x.value('(/myFields/S3/S3Q11OPP)[1]','varchar(2000)') AS S3_Q11Comment
,x.value('(/myFields/S3/S3Q12OPP)[1]','varchar(2000)') AS S3_Q12Comment
,x.value('(/myFields/S3/S3Q13OPP)[1]','varchar(2000)') AS S3_Q13Comment
,x.value('(/myFields/S3/S3Q14OPP)[1]','varchar(2000)') AS S3_Q14Comment
,x.value('(/myFields/S3/S3Q15OPP)[1]','varchar(2000)') AS S3_Q15Comment
,x.value('(/myFields/S3/S3Q16OPP)[1]','varchar(2000)') AS S3_Q16Comment
,x.value('(/myFields/S3/S3Q17OPP)[1]','varchar(2000)') AS S3_Q17Comment
,x.value('(/myFields/S3/S3Q18OPP)[1]','varchar(2000)') AS S3_Q18Comment
,x.value('(/myFields/S3/S3Q19OPP)[1]','varchar(2000)') AS S3_Q19Comment
,x.value('(/myFields/S3/S3Q20OPP)[1]','varchar(2000)') AS S3_Q20Comment
,x.value('(/myFields/S3/S3Q21OPP)[1]','varchar(2000)') AS S3_Q21Comment
------------------------</S3>-----------------------------------------------

into #XmlData	
from [EDW].[DBO].[OHAFormsXMLData] 
Cross Apply [XMLData].nodes('/myFields') AS T(x)
Where FileType='S'
and (  FileType+[QtrYear]+[BranchName] not in (select FileType+[QtrYear]+[BranchName] from Edw.dbo.OHAInformation ) )


--select * from #XmlData



------------------<S1>------------------------
Select BranchName,	QtrYear,FileModifiedDateTime,FileType,QuestionName, Question
into #S1_Question
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,S1_Q1Question,S1_Q2Question,S1_Q3Question,S1_Q4Question,S1_Q5Question,S1_Q6Question,S1_Q7Question,S1_Q8Question,S1_Q9Question,S1_Q10Question,S1_Q11Question,S1_Q12Question,S1_Q13Question,S1_Q14Question,S1_Q15Question,S1_Q16Question,S1_Q17Question,S1_Q18Question,S1_Q19Question,S1_Q20Question,S1_Q21Question,S1_Q22Question,S1_Q23Question

	 From #XmlData) as P
UNPIVOT
	(Question for QuestionName in 
					(  
					S1_Q1Question,S1_Q2Question,S1_Q3Question,S1_Q4Question,S1_Q5Question,S1_Q6Question,S1_Q7Question,S1_Q8Question,S1_Q9Question,S1_Q10Question,S1_Q11Question,S1_Q12Question,S1_Q13Question,S1_Q14Question,S1_Q15Question,S1_Q16Question,S1_Q17Question,S1_Q18Question,S1_Q19Question,S1_Q20Question,S1_Q21Question,S1_Q22Question,S1_Q23Question

					)
	) as unpvt

--Select * from #S1_Question


Select BranchName,	QtrYear,FileModifiedDateTime,FileType, AnswerName, Answer
Into #S1_Answer
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,S1_Q1Answer,S1_Q2Answer,S1_Q3Answer,S1_Q4Answer,S1_Q5Answer,S1_Q6Answer,S1_Q7Answer,S1_Q8Answer,S1_Q9Answer,S1_Q10Answer,S1_Q11Answer,S1_Q12Answer,S1_Q13Answer,S1_Q14Answer,S1_Q15Answer,S1_Q16Answer,S1_Q17Answer,S1_Q18Answer,S1_Q19Answer,S1_Q20Answer,S1_Q21Answer,S1_Q22Answer,S1_Q23Answer


	 From #XmlData) as P
UNPIVOT
	(Answer for AnswerName in 
					(
					S1_Q1Answer,S1_Q2Answer,S1_Q3Answer,S1_Q4Answer,S1_Q5Answer,S1_Q6Answer,S1_Q7Answer,S1_Q8Answer,S1_Q9Answer,S1_Q10Answer,S1_Q11Answer,S1_Q12Answer,S1_Q13Answer,S1_Q14Answer,S1_Q15Answer,S1_Q16Answer,S1_Q17Answer,S1_Q18Answer,S1_Q19Answer,S1_Q20Answer,S1_Q21Answer,S1_Q22Answer,S1_Q23Answer

					)
	) as unpvt

--Select * from #S1_Answer


Select BranchName,	QtrYear,FileModifiedDateTime,FileType,ScoreName, Score
into #S1_Score
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,S1_Q1Score,S1_Q2Score,S1_Q3Score,S1_Q4Score,S1_Q5Score,S1_Q6Score,S1_Q7Score,S1_Q8Score,S1_Q9Score,S1_Q10Score,S1_Q11Score,S1_Q12Score,S1_Q13Score,S1_Q14Score,S1_Q15Score,S1_Q16Score,S1_Q17Score,S1_Q18Score,S1_Q19Score,S1_Q20Score,S1_Q21Score,S1_Q22Score,S1_Q23Score
	 From #XmlData) as P
UNPIVOT
	(Score for ScoreName in 
					(  
					S1_Q1Score,S1_Q2Score,S1_Q3Score,S1_Q4Score,S1_Q5Score,S1_Q6Score,S1_Q7Score,S1_Q8Score,S1_Q9Score,S1_Q10Score,S1_Q11Score,S1_Q12Score,S1_Q13Score,S1_Q14Score,S1_Q15Score,S1_Q16Score,S1_Q17Score,S1_Q18Score,S1_Q19Score,S1_Q20Score,S1_Q21Score,S1_Q22Score,S1_Q23Score
					)
	) as unpvt

--Select * from #S1_Score





Select BranchName,	QtrYear,FileModifiedDateTime,FileType,CommentName, Comment
into #S1_Comment
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,S1_Q1Comment,S1_Q2Comment,S1_Q3Comment,S1_Q4Comment,S1_Q5Comment,S1_Q6Comment,S1_Q7Comment,S1_Q8Comment,S1_Q9Comment,S1_Q10Comment,S1_Q11Comment,S1_Q12Comment,S1_Q13Comment,S1_Q14Comment,S1_Q15Comment,S1_Q16Comment,S1_Q17Comment,S1_Q18Comment,S1_Q19Comment,S1_Q20Comment,S1_Q21Comment,S1_Q22Comment,S1_Q23Comment
	 From #XmlData) as P
UNPIVOT
	(Comment for CommentName in 
					( 
					S1_Q1Comment,S1_Q2Comment,S1_Q3Comment,S1_Q4Comment,S1_Q5Comment,S1_Q6Comment,S1_Q7Comment,S1_Q8Comment,S1_Q9Comment,S1_Q10Comment,S1_Q11Comment,S1_Q12Comment,S1_Q13Comment,S1_Q14Comment,S1_Q15Comment,S1_Q16Comment,S1_Q17Comment,S1_Q18Comment,S1_Q19Comment,S1_Q20Comment,S1_Q21Comment,S1_Q22Comment,S1_Q23Comment
					)
	) as unpvt

--Select * from #S1_Comment
------------------<S1>------------------------


------------------<S2>------------------------
Select BranchName,	QtrYear,FileModifiedDateTime,FileType,QuestionName, Question
into #S2_Question
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,S2_Q1Question,S2_Q2Question,S2_Q3Question,S2_Q4Question,S2_Q5Question,S2_Q6Question,S2_Q7Question,S2_Q8Question,S2_Q9Question,S2_Q10Question,S2_Q11Question,S2_Q12Question,S2_Q13Question,S2_Q14Question,S2_Q15Question,S2_Q16Question
	 From #XmlData) as P
UNPIVOT
	(Question for QuestionName in 
					(  
					S2_Q1Question,S2_Q2Question,S2_Q3Question,S2_Q4Question,S2_Q5Question,S2_Q6Question,S2_Q7Question,S2_Q8Question,S2_Q9Question,S2_Q10Question,S2_Q11Question,S2_Q12Question,S2_Q13Question,S2_Q14Question,S2_Q15Question,S2_Q16Question
					)
	) as unpvt
--Select * from #S2_Question


Select BranchName,	QtrYear,FileModifiedDateTime,FileType, AnswerName, Answer
Into #S2_Answer
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,S2_Q1Answer,S2_Q2Answer,S2_Q3Answer,S2_Q4Answer,S2_Q5Answer,S2_Q6Answer,S2_Q7Answer,S2_Q8Answer,S2_Q9Answer,S2_Q10Answer,S2_Q11Answer,S2_Q12Answer,S2_Q13Answer,S2_Q14Answer,S2_Q15Answer,S2_Q16Answer
	 From #XmlData) as P
UNPIVOT
	(Answer for AnswerName in 
					( 
					S2_Q1Answer,S2_Q2Answer,S2_Q3Answer,S2_Q4Answer,S2_Q5Answer,S2_Q6Answer,S2_Q7Answer,S2_Q8Answer,S2_Q9Answer,S2_Q10Answer,S2_Q11Answer,S2_Q12Answer,S2_Q13Answer,S2_Q14Answer,S2_Q15Answer,S2_Q16Answer
					)
	) as unpvt

--Select * from #S2_Answer

Select BranchName,	QtrYear,FileModifiedDateTime,FileType,ScoreName, Score
into #S2_Score
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,S2_Q1Score,S2_Q2Score,S2_Q3Score,S2_Q4Score,S2_Q5Score,S2_Q6Score,S2_Q7Score,S2_Q8Score,S2_Q9Score,S2_Q10Score,S2_Q11Score,S2_Q12Score,S2_Q13Score,S2_Q14Score,S2_Q15Score,S2_Q16Score
	 From #XmlData) as P
UNPIVOT
	(Score for ScoreName in 
					( 
					S2_Q1Score,S2_Q2Score,S2_Q3Score,S2_Q4Score,S2_Q5Score,S2_Q6Score,S2_Q7Score,S2_Q8Score,S2_Q9Score,S2_Q10Score,S2_Q11Score,S2_Q12Score,S2_Q13Score,S2_Q14Score,S2_Q15Score,S2_Q16Score
					)
	) as unpvt

--Select * from #S2_Score

Select BranchName,	QtrYear,FileModifiedDateTime,FileType,CommentName, Comment
into #S2_Comment
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,S2_Q1Comment,S2_Q2Comment,S2_Q3Comment,S2_Q4Comment,S2_Q5Comment,S2_Q6Comment,S2_Q7Comment,S2_Q8Comment,S2_Q9Comment,S2_Q10Comment,S2_Q11Comment,S2_Q12Comment,S2_Q13Comment,S2_Q14Comment,S2_Q15Comment,S2_Q16Comment
	 From #XmlData) as P
UNPIVOT
	(Comment for CommentName in 
					( 
					S2_Q1Comment,S2_Q2Comment,S2_Q3Comment,S2_Q4Comment,S2_Q5Comment,S2_Q6Comment,S2_Q7Comment,S2_Q8Comment,S2_Q9Comment,S2_Q10Comment,S2_Q11Comment,S2_Q12Comment,S2_Q13Comment,S2_Q14Comment,S2_Q15Comment,S2_Q16Comment
					)
	) as unpvt
--Select * from #S2_Comment
------------------</S2>------------------------


------------------<S3>------------------------
Select BranchName,	QtrYear,FileModifiedDateTime,FileType,QuestionName, Question
into #S3_Question
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,S3_Q1Question,S3_Q2Question,S3_Q3Question,S3_Q4Question,S3_Q5Question,S3_Q6Question,S3_Q7Question,S3_Q8Question,S3_Q9Question,S3_Q10Question,S3_Q11Question,S3_Q12Question,S3_Q13Question,S3_Q14Question,S3_Q15Question,S3_Q16Question,S3_Q17Question,S3_Q18Question,S3_Q19Question,S3_Q20Question,S3_Q21Question
	 From #XmlData) as P
UNPIVOT
	(Question for QuestionName in 
					(  
					S3_Q1Question,S3_Q2Question,S3_Q3Question,S3_Q4Question,S3_Q5Question,S3_Q6Question,S3_Q7Question,S3_Q8Question,S3_Q9Question,S3_Q10Question,S3_Q11Question,S3_Q12Question,S3_Q13Question,S3_Q14Question,S3_Q15Question,S3_Q16Question,S3_Q17Question,S3_Q18Question,S3_Q19Question,S3_Q20Question,S3_Q21Question
					)
	) as unpvt
--Select * from #S3_Question

Select BranchName,	QtrYear,FileModifiedDateTime,FileType, AnswerName, Answer
Into #S3_Answer
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,S3_Q1Answer,S3_Q2Answer,S3_Q3Answer,S3_Q4Answer,S3_Q5Answer,S3_Q6Answer,S3_Q7Answer,S3_Q8Answer,S3_Q9Answer,S3_Q10Answer,S3_Q11Answer,S3_Q12Answer,S3_Q13Answer,S3_Q14Answer,S3_Q15Answer,S3_Q16Answer,S3_Q17Answer,S3_Q18Answer,S3_Q19Answer,S3_Q20Answer,S3_Q21Answer
	 From #XmlData) as P
UNPIVOT
	(Answer for AnswerName in 
					( 
					S3_Q1Answer,S3_Q2Answer,S3_Q3Answer,S3_Q4Answer,S3_Q5Answer,S3_Q6Answer,S3_Q7Answer,S3_Q8Answer,S3_Q9Answer,S3_Q10Answer,S3_Q11Answer,S3_Q12Answer,S3_Q13Answer,S3_Q14Answer,S3_Q15Answer,S3_Q16Answer,S3_Q17Answer,S3_Q18Answer,S3_Q19Answer,S3_Q20Answer,S3_Q21Answer
					)
	) as unpvt

--Select * from #S3_Answer

Select BranchName,	QtrYear,FileModifiedDateTime,FileType,ScoreName, Score
into #S3_Score
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,S3_Q1Score,S3_Q2Score,S3_Q3Score,S3_Q4Score,S3_Q5Score,S3_Q6Score,S3_Q7Score,S3_Q8Score,S3_Q9Score,S3_Q10Score,S3_Q11Score,S3_Q12Score,S3_Q13Score,S3_Q14Score,S3_Q15Score,S3_Q16Score,S3_Q17Score,S3_Q18Score,S3_Q19Score,S3_Q20Score,S3_Q21Score
	 From #XmlData) as P
UNPIVOT
	(Score for ScoreName in 
					( 
					S3_Q1Score,S3_Q2Score,S3_Q3Score,S3_Q4Score,S3_Q5Score,S3_Q6Score,S3_Q7Score,S3_Q8Score,S3_Q9Score,S3_Q10Score,S3_Q11Score,S3_Q12Score,S3_Q13Score,S3_Q14Score,S3_Q15Score,S3_Q16Score,S3_Q17Score,S3_Q18Score,S3_Q19Score,S3_Q20Score,S3_Q21Score
					)
	) as unpvt

--Select * from #S3_Score

Select BranchName,	QtrYear,FileModifiedDateTime,FileType,CommentName, Comment
into #S3_Comment
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,S3_Q1Comment,S3_Q2Comment,S3_Q3Comment,S3_Q4Comment,S3_Q5Comment,S3_Q6Comment,S3_Q7Comment,S3_Q8Comment,S3_Q9Comment,S3_Q10Comment,S3_Q11Comment,S3_Q12Comment,S3_Q13Comment,S3_Q14Comment,S3_Q15Comment,S3_Q16Comment,S3_Q17Comment,S3_Q18Comment,S3_Q19Comment,S3_Q20Comment,S3_Q21Comment
	 From #XmlData) as P
UNPIVOT
	(Comment for CommentName in 
					( 
					S3_Q1Comment,S3_Q2Comment,S3_Q3Comment,S3_Q4Comment,S3_Q5Comment,S3_Q6Comment,S3_Q7Comment,S3_Q8Comment,S3_Q9Comment,S3_Q10Comment,S3_Q11Comment,S3_Q12Comment,S3_Q13Comment,S3_Q14Comment,S3_Q15Comment,S3_Q16Comment,S3_Q17Comment,S3_Q18Comment,S3_Q19Comment,S3_Q20Comment,S3_Q21Comment
					)
	) as unpvt
--Select * from #S3_Comment
------------------</S3>------------------------

insert into OHAInformation
(BranchName,QtrYear,FileModifiedDateTime,FileType,QuestionName,AnswerName,ScoreName,CommentName, Question,Answer,Score,Comment)
Select q.BranchName,q.QtrYear,q.FileModifiedDateTime,q.FileType,QuestionName,AnswerName,ScoreName,CommentName, Question,Answer,Score,Comment
from #S1_Question  q 
left join #S1_Score s on   Replace(Replace(s.ScoreName,'Q',''),'Score','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=s.BranchName and q.QtrYear=s.QtrYear and q.FileType=s.FileType
left join #S1_Answer a on  Replace(Replace(a.AnswerName,'Q',''),'Answer','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=a.BranchName and q.QtrYear=a.QtrYear and q.FileType=a.FileType
left join #S1_Comment c on Replace(Replace(c.CommentName,'Q',''),'Comment','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=c.BranchName and q.QtrYear=c.QtrYear and q.FileType=c.FileType
union all 
Select q.BranchName,q.QtrYear,q.FileModifiedDateTime,q.FileType,QuestionName, AnswerName,ScoreName,CommentName, Question, Answer, Score,Comment
from #S2_Question q
left join #S2_Score s on   Replace(Replace(s.ScoreName,'Q',''),'Score','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=s.BranchName and q.QtrYear=s.QtrYear and q.FileType=s.FileType
left join #S2_Answer a on  Replace(Replace(a.AnswerName,'Q',''),'Answer','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=a.BranchName and q.QtrYear=a.QtrYear and q.FileType=a.FileType
left join #S2_Comment c on Replace(Replace(c.CommentName,'Q',''),'Comment','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=c.BranchName and q.QtrYear=c.QtrYear and q.FileType=c.FileType
union all 
Select q.BranchName,q.QtrYear,q.FileModifiedDateTime,q.FileType,QuestionName, AnswerName,ScoreName,CommentName, Question, Answer, Score,Comment
from #S3_Question q
left join #S3_Score s on   Replace(Replace(s.ScoreName,'Q',''),'Score','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=s.BranchName and q.QtrYear=s.QtrYear and q.FileType=s.FileType
left join #S3_Answer a on  Replace(Replace(a.AnswerName,'Q',''),'Answer','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=a.BranchName and q.QtrYear=a.QtrYear and q.FileType=a.FileType
left join #S3_Comment c on Replace(Replace(c.CommentName,'Q',''),'Comment','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=c.BranchName and q.QtrYear=c.QtrYear and q.FileType=c.FileType



End


