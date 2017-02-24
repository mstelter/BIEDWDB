

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
--Exec [dbo].[usp_OHALoadXMLDataToOHAInformation_IC]
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
Create PROCEDURE [dbo].[usp_OHALoadXMLDataToOHAInformation_IC]
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
	
------------------------<CR>------------------------------
,x.value('(/myFields/CRQ1)[1]','varchar(2000)') AS CR_Q1Question
,x.value('(/myFields/CRQ2)[1]','varchar(2000)') AS CR_Q2Question
,x.value('(/myFields/CRQ3)[1]','varchar(2000)') AS CR_Q3Question
,x.value('(/myFields/CRQ4)[1]','varchar(2000)') AS CR_Q4Question
,x.value('(/myFields/CRQ5)[1]','varchar(2000)') AS CR_Q5Question
,x.value('(/myFields/CRQ6)[1]','varchar(2000)') AS CR_Q6Question
,x.value('(/myFields/CRQ7)[1]','varchar(2000)') AS CR_Q7Question
,x.value('(/myFields/CRQ8)[1]','varchar(2000)') AS CR_Q8Question
,x.value('(/myFields/CRQ9)[1]','varchar(2000)') AS CR_Q9Question
,x.value('(/myFields/CRQ10)[1]','varchar(2000)') AS CR_Q10Question
,x.value('(/myFields/CRQ11)[1]','varchar(2000)') AS CR_Q11Question
,x.value('(/myFields/CRQ12)[1]','varchar(2000)') AS CR_Q12Question
,x.value('(/myFields/CRQ13)[1]','varchar(2000)') AS CR_Q13Question
,x.value('(/myFields/CRQ14)[1]','varchar(2000)') AS CR_Q14Question
,x.value('(/myFields/CRQ15)[1]','varchar(2000)') AS CR_Q15Question
,x.value('(/myFields/CRQ16)[1]','varchar(2000)') AS CR_Q16Question
,x.value('(/myFields/CRQ17)[1]','varchar(2000)') AS CR_Q17Question
,x.value('(/myFields/CRQ18)[1]','varchar(2000)') AS CR_Q18Question
,x.value('(/myFields/CRQ19)[1]','varchar(2000)') AS CR_Q19Question
,x.value('(/myFields/CRQ20)[1]','varchar(2000)') AS CR_Q20Question
,x.value('(/myFields/CRQ21)[1]','varchar(2000)') AS CR_Q21Question
,x.value('(/myFields/CRQ22)[1]','varchar(2000)') AS CR_Q22Question

,x.value('(/myFields/CR/Q1YN)[1]','Int') AS CR_Q1Answer
,x.value('(/myFields/CR/Q2YN)[1]','Int') AS CR_Q2Answer
,x.value('(/myFields/CR/Q3YN)[1]','Int') AS CR_Q3Answer
,x.value('(/myFields/CR/Q4YN)[1]','Int') AS CR_Q4Answer
,x.value('(/myFields/CR/Q5YN)[1]','Int') AS CR_Q5Answer
,x.value('(/myFields/CR/Q6YN)[1]','Int') AS CR_Q6Answer
,x.value('(/myFields/CR/Q7YN)[1]','Int') AS CR_Q7Answer
,x.value('(/myFields/CR/Q8YN)[1]','Int') AS CR_Q8Answer
,x.value('(/myFields/CR/Q9YN)[1]','Int') AS CR_Q9Answer
,x.value('(/myFields/CR/Q10YN)[1]','Int') AS CR_Q10Answer
,x.value('(/myFields/CR/Q11YN)[1]','Int') AS CR_Q11Answer
,x.value('(/myFields/CR/Q12YN)[1]','Int') AS CR_Q12Answer
,x.value('(/myFields/CR/Q13YN)[1]','Int') AS CR_Q13Answer
,x.value('(/myFields/CR/Q14YN)[1]','Int') AS CR_Q14Answer
,x.value('(/myFields/CR/Q15YN)[1]','Int') AS CR_Q15Answer
,x.value('(/myFields/CR/Q16YN)[1]','Int') AS CR_Q16Answer
,x.value('(/myFields/CR/Q17YN)[1]','Int') AS CR_Q17Answer
,x.value('(/myFields/CR/Q18YN)[1]','Int') AS CR_Q18Answer
,x.value('(/myFields/CR/Q19YN)[1]','Int') AS CR_Q19Answer
,x.value('(/myFields/CR/Q20YN)[1]','Int') AS CR_Q20Answer		
,x.value('(/myFields/CR/Q21YN)[1]','Int') AS CR_Q21Answer
,x.value('(/myFields/CR/Q22YN)[1]','Int') AS CR_Q22Answer		

,x.value('(/myFields/CRQ1S)[1]','Float') AS CR_Q1Score	
,x.value('(/myFields/CRQ2S)[1]','Float') AS CR_Q2Score	
,x.value('(/myFields/CRQ3S)[1]','Float') AS CR_Q3Score	
,x.value('(/myFields/CRQ4S)[1]','Float') AS CR_Q4Score	
,x.value('(/myFields/CRQ5S)[1]','Float') AS CR_Q5Score	
,x.value('(/myFields/CRQ6S)[1]','Float') AS CR_Q6Score	
,x.value('(/myFields/CRQ7S)[1]','Float') AS CR_Q7Score	
,x.value('(/myFields/CRQ8S)[1]','Float') AS CR_Q8Score	
,x.value('(/myFields/CRQ9S)[1]','Float') AS CR_Q9Score	
,x.value('(/myFields/CRQ10S)[1]','Float') AS CR_Q10Score	
,x.value('(/myFields/CRQ11S)[1]','Float') AS CR_Q11Score
,x.value('(/myFields/CRQ12S)[1]','Float') AS CR_Q12Score	
,x.value('(/myFields/CRQ13S)[1]','Float') AS CR_Q13Score	
,x.value('(/myFields/CRQ14S)[1]','Float') AS CR_Q14Score	
,x.value('(/myFields/CRQ15S)[1]','Float') AS CR_Q15Score	
,x.value('(/myFields/CRQ16S)[1]','Float') AS CR_Q16Score	
,x.value('(/myFields/CRQ17S)[1]','Float') AS CR_Q17Score	
,x.value('(/myFields/CRQ18S)[1]','Float') AS CR_Q18Score
,x.value('(/myFields/CRQ19S)[1]','Float') AS CR_Q19Score	
,x.value('(/myFields/CRQ20S)[1]','Float') AS CR_Q20Score	
,x.value('(/myFields/CRQ21S)[1]','Float') AS CR_Q21Score
,x.value('(/myFields/CRQ22S)[1]','Float') AS CR_Q22Score	

,x.value('(/myFields/CR/Q1OPP)[1]','varchar(2000)') AS CR_Q1Comment
,x.value('(/myFields/CR/Q2OPP)[1]','varchar(2000)') AS CR_Q2Comment
,x.value('(/myFields/CR/Q3OPP)[1]','varchar(2000)') AS CR_Q3Comment
,x.value('(/myFields/CR/Q4OPP)[1]','varchar(2000)') AS CR_Q4Comment
,x.value('(/myFields/CR/Q5OPP)[1]','varchar(2000)') AS CR_Q5Comment
,x.value('(/myFields/CR/Q6OPP)[1]','varchar(2000)') AS CR_Q6Comment
,x.value('(/myFields/CR/Q7OPP)[1]','varchar(2000)') AS CR_Q7Comment
,x.value('(/myFields/CR/Q8OPP)[1]','varchar(2000)') AS CR_Q8Comment
,x.value('(/myFields/CR/Q9OPP)[1]','varchar(2000)') AS CR_Q9Comment
,x.value('(/myFields/CR/Q10OPP)[1]','varchar(2000)') AS CR_Q10Comment
,x.value('(/myFields/CR/Q11OPP)[1]','varchar(2000)') AS CR_Q11Comment
,x.value('(/myFields/CR/Q12OPP)[1]','varchar(2000)') AS CR_Q12Comment
,x.value('(/myFields/CR/Q13OPP)[1]','varchar(2000)') AS CR_Q13Comment
,x.value('(/myFields/CR/Q14OPP)[1]','varchar(2000)') AS CR_Q14Comment
,x.value('(/myFields/CR/Q15OPP)[1]','varchar(2000)') AS CR_Q15Comment
,x.value('(/myFields/CR/Q16OPP)[1]','varchar(2000)') AS CR_Q16Comment
,x.value('(/myFields/CR/Q17OPP)[1]','varchar(2000)') AS CR_Q17Comment
,x.value('(/myFields/CR/Q18OPP)[1]','varchar(2000)') AS CR_Q18Comment
,x.value('(/myFields/CR/Q19OPP)[1]','varchar(2000)') AS CR_Q19Comment
,x.value('(/myFields/CR/Q20OPP)[1]','varchar(2000)') AS CR_Q20Comment
,x.value('(/myFields/CR/Q21OPP)[1]','varchar(2000)') AS CR_Q21Comment
,x.value('(/myFields/CR/Q22OPP)[1]','varchar(2000)') AS CR_Q22Comment
------------------------</CR>----------------------------------------


------------------------- <General>----------------------------------------
,x.value('(/myFields/GQ1)[1]','varchar(2000)') AS General_Q1Question
,x.value('(/myFields/GQ2)[1]','varchar(2000)') AS General_Q2Question
,x.value('(/myFields/GQ3)[1]','varchar(2000)') AS General_Q3Question
,x.value('(/myFields/GQ4)[1]','varchar(2000)') AS General_Q4Question
,x.value('(/myFields/GQ5)[1]','varchar(2000)') AS General_Q5Question
,x.value('(/myFields/GQ6)[1]','varchar(2000)') AS General_Q6Question
,x.value('(/myFields/GQ7)[1]','varchar(2000)') AS General_Q7Question
,x.value('(/myFields/GQ8)[1]','varchar(2000)') AS General_Q8Question
,x.value('(/myFields/GQ9)[1]','varchar(2000)') AS General_Q9Question
,x.value('(/myFields/GQ10)[1]','varchar(2000)') AS General_Q10Question
,x.value('(/myFields/GQ11)[1]','varchar(2000)') AS General_Q11Question
,x.value('(/myFields/GQ12)[1]','varchar(2000)') AS General_Q12Question
,x.value('(/myFields/GQ13)[1]','varchar(2000)') AS General_Q13Question
,x.value('(/myFields/GQ14)[1]','varchar(2000)') AS General_Q14Question
,x.value('(/myFields/GQ15)[1]','varchar(2000)') AS General_Q15Question
,x.value('(/myFields/GQ16)[1]','varchar(2000)') AS General_Q16Question
,x.value('(/myFields/GQ17)[1]','varchar(2000)') AS General_Q17Question
,x.value('(/myFields/GQ18)[1]','varchar(2000)') AS General_Q18Question
,x.value('(/myFields/GQ19)[1]','varchar(2000)') AS General_Q19Question
,x.value('(/myFields/GQ20)[1]','varchar(2000)') AS General_Q20Question
,x.value('(/myFields/GQ21)[1]','varchar(2000)') AS General_Q21Question


,x.value('(/myFields/General/GQ1YN)[1]','Int') AS General_Q1Answer
,x.value('(/myFields/General/GQ2YN)[1]','Int') AS General_Q2Answer
,x.value('(/myFields/General/GQ3YN)[1]','Int') AS General_Q3Answer
,x.value('(/myFields/General/GQ4YN)[1]','Int') AS General_Q4Answer
,x.value('(/myFields/General/GQ5YN)[1]','Int') AS General_Q5Answer
,x.value('(/myFields/General/GQ6YN)[1]','Int') AS General_Q6Answer
,x.value('(/myFields/General/GQ7YN)[1]','Int') AS General_Q7Answer
,x.value('(/myFields/General/GQ8YN)[1]','Int') AS General_Q8Answer
,x.value('(/myFields/General/GQ9YN)[1]','Int') AS General_Q9Answer
,x.value('(/myFields/General/GQ10YN)[1]','Int') AS General_Q10Answer
,x.value('(/myFields/General/GQ11YN)[1]','Int') AS General_Q11Answer
,x.value('(/myFields/General/GQ12YN)[1]','Int') AS General_Q12Answer
,x.value('(/myFields/General/GQ13YN)[1]','Int') AS General_Q13Answer
,x.value('(/myFields/General/GQ14YN)[1]','Int') AS General_Q14Answer
,x.value('(/myFields/General/GQ15YN)[1]','Int') AS General_Q15Answer
,x.value('(/myFields/General/GQ16YN)[1]','Int') AS General_Q16Answer
,x.value('(/myFields/General/GQ17YN)[1]','Int') AS General_Q17Answer
,x.value('(/myFields/General/GQ18YN)[1]','Int') AS General_Q18Answer
,x.value('(/myFields/General/GQ19YN)[1]','Int') AS General_Q19Answer
,x.value('(/myFields/General/GQ20YN)[1]','Int') AS General_Q20Answer
,x.value('(/myFields/General/GQ21YN)[1]','Int') AS General_Q21Answer


,x.value('(/myFields/GQ1S)[1]','Float') AS General_Q1Score	
,x.value('(/myFields/GQ2S)[1]','Float') AS General_Q2Score	
,x.value('(/myFields/GQ3S)[1]','Float') AS General_Q3Score	
,x.value('(/myFields/GQ4S)[1]','Float') AS General_Q4Score	
,x.value('(/myFields/GQ5S)[1]','Float') AS General_Q5Score	
,x.value('(/myFields/GQ6S)[1]','Float') AS General_Q6Score	
,x.value('(/myFields/GQ7S)[1]','Float') AS General_Q7Score	
,x.value('(/myFields/GQ8S)[1]','Float') AS General_Q8Score	
,x.value('(/myFields/GQ9S)[1]','Float') AS General_Q9Score	
,x.value('(/myFields/GQ10S)[1]','Float') AS General_Q10Score	
,x.value('(/myFields/GQ11S)[1]','Float') AS General_Q11Score
,x.value('(/myFields/GQ12S)[1]','Float') AS General_Q12Score	
,x.value('(/myFields/GQ13S)[1]','Float') AS General_Q13Score	
,x.value('(/myFields/GQ14S)[1]','Float') AS General_Q14Score	
,x.value('(/myFields/GQ15S)[1]','Float') AS General_Q15Score	
,x.value('(/myFields/GQ16S)[1]','Float') AS General_Q16Score	
,x.value('(/myFields/GQ17S)[1]','Float') AS General_Q17Score	
,x.value('(/myFields/GQ18S)[1]','Float') AS General_Q18Score	
,x.value('(/myFields/GQ19S)[1]','Float') AS General_Q19Score	
,x.value('(/myFields/GQ20S)[1]','Float') AS General_Q20Score	
,x.value('(/myFields/GQ21S)[1]','Float') AS General_Q21Score	


,x.value('(/myFields/General/GQ1OPP)[1]','varchar(2000)') AS General_Q1Comment
,x.value('(/myFields/General/GQ2OPP)[1]','varchar(2000)') AS General_Q2Comment
,x.value('(/myFields/General/GQ3OPP)[1]','varchar(2000)') AS General_Q3Comment
,x.value('(/myFields/General/GQ4OPP)[1]','varchar(2000)') AS General_Q4Comment
,x.value('(/myFields/General/GQ5OPP)[1]','varchar(2000)') AS General_Q5Comment
,x.value('(/myFields/General/GQ6OPP)[1]','varchar(2000)') AS General_Q6Comment
,x.value('(/myFields/General/GQ7OPP)[1]','varchar(2000)') AS General_Q7Comment
,x.value('(/myFields/General/GQ8OPP)[1]','varchar(2000)') AS General_Q8Comment
,x.value('(/myFields/General/GQ9OPP)[1]','varchar(2000)') AS General_Q9Comment
,x.value('(/myFields/General/GQ10OPP)[1]','varchar(2000)') AS General_Q10Comment
,x.value('(/myFields/General/GQ11OPP)[1]','varchar(2000)') AS General_Q11Comment
,x.value('(/myFields/General/GQ12OPP)[1]','varchar(2000)') AS General_Q12Comment
,x.value('(/myFields/General/GQ13OPP)[1]','varchar(2000)') AS General_Q13Comment
,x.value('(/myFields/General/GQ14OPP)[1]','varchar(2000)') AS General_Q14Comment
,x.value('(/myFields/General/GQ15OPP)[1]','varchar(2000)') AS General_Q15Comment
,x.value('(/myFields/General/GQ16OPP)[1]','varchar(2000)') AS General_Q16Comment
,x.value('(/myFields/General/GQ17OPP)[1]','varchar(2000)') AS General_Q17Comment
,x.value('(/myFields/General/GQ18OPP)[1]','varchar(2000)') AS General_Q18Comment
,x.value('(/myFields/General/GQ19OPP)[1]','varchar(2000)') AS General_Q19Comment
,x.value('(/myFields/General/GQ20OPP)[1]','varchar(2000)') AS General_Q20Comment
,x.value('(/myFields/General/GQ21OPP)[1]','varchar(2000)') AS General_Q21Comment
------------------------</General>----------------------------------------

-------------------------<HV>----------------------------------------
,x.value('(/myFields/HVQ1)[1]','varchar(2000)') AS HV_Q1Question
,x.value('(/myFields/HVQ2)[1]','varchar(2000)') AS HV_Q2Question
,x.value('(/myFields/HVQ3)[1]','varchar(2000)') AS HV_Q3Question
,x.value('(/myFields/HVQ4)[1]','varchar(2000)') AS HV_Q4Question
,x.value('(/myFields/HVQ5)[1]','varchar(2000)') AS HV_Q5Question
,x.value('(/myFields/HVQ6)[1]','varchar(2000)') AS HV_Q6Question
,x.value('(/myFields/HVQ7)[1]','varchar(2000)') AS HV_Q7Question
,x.value('(/myFields/HVQ8)[1]','varchar(2000)') AS HV_Q8Question
,x.value('(/myFields/HVQ9)[1]','varchar(2000)') AS HV_Q9Question
,x.value('(/myFields/HVQ10)[1]','varchar(2000)') AS HV_Q10Question
,x.value('(/myFields/HVQ11)[1]','varchar(2000)') AS HV_Q11Question
,x.value('(/myFields/HVQ12)[1]','varchar(2000)') AS HV_Q12Question


,x.value('(/myFields/HV/HVQ1YN)[1]','Int') AS HV_Q1Answer
,x.value('(/myFields/HV/HVQ2YN)[1]','Int') AS HV_Q2Answer
,x.value('(/myFields/HV/HVQ3YN)[1]','Int') AS HV_Q3Answer
,x.value('(/myFields/HV/HVQ4YN)[1]','Int') AS HV_Q4Answer
,x.value('(/myFields/HV/HVQ5YN)[1]','Int') AS HV_Q5Answer
,x.value('(/myFields/HV/HVQ6YN)[1]','Int') AS HV_Q6Answer
,x.value('(/myFields/HV/HVQ7YN)[1]','Int') AS HV_Q7Answer
,x.value('(/myFields/HV/HVQ8YN)[1]','Int') AS HV_Q8Answer
,x.value('(/myFields/HV/HVQ9YN)[1]','Int') AS HV_Q9Answer
,x.value('(/myFields/HV/HVQ10YN)[1]','Int') AS HV_Q10Answer
,x.value('(/myFields/HV/HVQ11YN)[1]','Int') AS HV_Q11Answer
,x.value('(/myFields/HV/HVQ12YN)[1]','Int') AS HV_Q12Answer


,x.value('(/myFields/HVQ1S)[1]','Float') AS HV_Q1Score	
,x.value('(/myFields/HVQ2S)[1]','Float') AS HV_Q2Score	
,x.value('(/myFields/HVQ3S)[1]','Float') AS HV_Q3Score	
,x.value('(/myFields/HVQ4S)[1]','Float') AS HV_Q4Score	
,x.value('(/myFields/HVQ5S)[1]','Float') AS HV_Q5Score	
,x.value('(/myFields/HVQ6S)[1]','Float') AS HV_Q6Score	
,x.value('(/myFields/HVQ7S)[1]','Float') AS HV_Q7Score	
,x.value('(/myFields/HVQ8S)[1]','Float') AS HV_Q8Score	
,x.value('(/myFields/HVQ9S)[1]','Float') AS HV_Q9Score	
,x.value('(/myFields/HVQ10S)[1]','Float') AS HV_Q10Score	
,x.value('(/myFields/HVQ11S)[1]','Float') AS HV_Q11Score
,x.value('(/myFields/HVQ12S)[1]','Float') AS HV_Q12Score	


,x.value('(/myFields/HV/HVQ1OPP)[1]','varchar(2000)') AS HV_Q1Comment
,x.value('(/myFields/HV/HVQ2OPP)[1]','varchar(2000)') AS HV_Q2Comment
,x.value('(/myFields/HV/HVQ3OPP)[1]','varchar(2000)') AS HV_Q3Comment
,x.value('(/myFields/HV/HVQ4OPP)[1]','varchar(2000)') AS HV_Q4Comment
,x.value('(/myFields/HV/HVQ5OPP)[1]','varchar(2000)') AS HV_Q5Comment
,x.value('(/myFields/HV/HVQ6OPP)[1]','varchar(2000)') AS HV_Q6Comment
,x.value('(/myFields/HV/HVQ7OPP)[1]','varchar(2000)') AS HV_Q7Comment
,x.value('(/myFields/HV/HVQ8OPP)[1]','varchar(2000)') AS HV_Q8Comment
,x.value('(/myFields/HV/HVQ9OPP)[1]','varchar(2000)') AS HV_Q9Comment
,x.value('(/myFields/HV/HVQ10OPP)[1]','varchar(2000)') AS HV_Q10Comment
,x.value('(/myFields/HV/HVQ11OPP)[1]','varchar(2000)') AS HV_Q11Comment
,x.value('(/myFields/HV/HVQ12OPP)[1]','varchar(2000)') AS HV_Q12Comment
------------------------</HV>-----------------------------------------------

into #XmlData	
from [EDW].[DBO].[OHAFormsXMLData] 
Cross Apply [XMLData].nodes('/myFields') AS T(x)
Where FileType='IC'
and (  FileType+[QtrYear]+[BranchName] not in (select distinct FileType+[QtrYear]+[BranchName] from Edw.dbo.OHAInformation ) )


--select * from #XmlData



------------------<CR>------------------------
Select BranchName,	QtrYear,FileModifiedDateTime,FileType,QuestionName, Question
into #CR_Question
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,CR_Q1Question,CR_Q2Question,CR_Q3Question,CR_Q4Question,CR_Q5Question,CR_Q6Question,CR_Q7Question,CR_Q8Question,CR_Q9Question,CR_Q10Question,CR_Q11Question,CR_Q12Question,CR_Q13Question,CR_Q14Question,CR_Q15Question,CR_Q16Question,CR_Q17Question,CR_Q18Question,CR_Q19Question,CR_Q20Question,CR_Q21Question,CR_Q22Question
	 From #XmlData) as P
UNPIVOT
	(Question for QuestionName in 
					(  
					CR_Q1Question,CR_Q2Question,CR_Q3Question,CR_Q4Question,CR_Q5Question,CR_Q6Question,CR_Q7Question,CR_Q8Question,CR_Q9Question,CR_Q10Question,CR_Q11Question,CR_Q12Question,CR_Q13Question,CR_Q14Question,CR_Q15Question,CR_Q16Question,CR_Q17Question,CR_Q18Question,CR_Q19Question,CR_Q20Question,CR_Q21Question,CR_Q22Question
					)
	) as unpvt

--Select * from #CR_Question


Select BranchName,	QtrYear,FileModifiedDateTime,FileType, AnswerName, Answer
Into #CR_Answer
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,CR_Q1Answer,CR_Q2Answer,CR_Q3Answer,CR_Q4Answer,CR_Q5Answer,CR_Q6Answer,CR_Q7Answer,CR_Q8Answer,CR_Q9Answer,CR_Q10Answer,CR_Q11Answer,CR_Q12Answer,CR_Q13Answer,CR_Q14Answer,CR_Q15Answer,CR_Q16Answer,CR_Q17Answer,CR_Q18Answer,CR_Q19Answer,CR_Q20Answer,CR_Q21Answer,CR_Q22Answer
	 From #XmlData) as P
UNPIVOT
	(Answer for AnswerName in 
					(
					CR_Q1Answer,CR_Q2Answer,CR_Q3Answer,CR_Q4Answer,CR_Q5Answer,CR_Q6Answer,CR_Q7Answer,CR_Q8Answer,CR_Q9Answer,CR_Q10Answer,CR_Q11Answer,CR_Q12Answer,CR_Q13Answer,CR_Q14Answer,CR_Q15Answer,CR_Q16Answer,CR_Q17Answer,CR_Q18Answer,CR_Q19Answer,CR_Q20Answer,CR_Q21Answer,CR_Q22Answer
					)
	) as unpvt

--Select * from #CR_Answer


Select BranchName,	QtrYear,FileModifiedDateTime,FileType,ScoreName, Score
into #CR_Score
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,CR_Q1Score,CR_Q2Score,CR_Q3Score,CR_Q4Score,CR_Q5Score,CR_Q6Score,CR_Q7Score,CR_Q8Score,CR_Q9Score,CR_Q10Score,CR_Q11Score,CR_Q12Score,CR_Q13Score,CR_Q14Score,CR_Q15Score,CR_Q16Score,CR_Q17Score,CR_Q18Score,CR_Q19Score,CR_Q20Score,CR_Q21Score,CR_Q22Score
	 From #XmlData) as P
UNPIVOT
	(Score for ScoreName in 
					(  
					CR_Q1Score,CR_Q2Score,CR_Q3Score,CR_Q4Score,CR_Q5Score,CR_Q6Score,CR_Q7Score,CR_Q8Score,CR_Q9Score,CR_Q10Score,CR_Q11Score,CR_Q12Score,CR_Q13Score,CR_Q14Score,CR_Q15Score,CR_Q16Score,CR_Q17Score,CR_Q18Score,CR_Q19Score,CR_Q20Score,CR_Q21Score,CR_Q22Score
					)
	) as unpvt

--Select * from #CR_Score





Select BranchName,	QtrYear,FileModifiedDateTime,FileType,CommentName, Comment
into #CR_Comment
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,CR_Q1Comment,CR_Q2Comment,CR_Q3Comment,CR_Q4Comment,CR_Q5Comment,CR_Q6Comment,CR_Q7Comment,CR_Q8Comment,CR_Q9Comment,CR_Q10Comment,CR_Q11Comment,CR_Q12Comment,CR_Q13Comment,CR_Q14Comment,CR_Q15Comment,CR_Q16Comment,CR_Q17Comment,CR_Q18Comment,CR_Q19Comment,CR_Q20Comment,CR_Q21Comment,CR_Q22Comment

	 From #XmlData) as P
UNPIVOT
	(Comment for CommentName in 
					( 
CR_Q1Comment,CR_Q2Comment,CR_Q3Comment,CR_Q4Comment,CR_Q5Comment,CR_Q6Comment,CR_Q7Comment,CR_Q8Comment,CR_Q9Comment,CR_Q10Comment,CR_Q11Comment,CR_Q12Comment,CR_Q13Comment,CR_Q14Comment,CR_Q15Comment,CR_Q16Comment,CR_Q17Comment,CR_Q18Comment,CR_Q19Comment,CR_Q20Comment,CR_Q21Comment,CR_Q22Comment

    				)
	) as unpvt

--Select * from #CR_Comment
------------------<CR>------------------------


------------------<S2>------------------------
Select BranchName,	QtrYear,FileModifiedDateTime,FileType,QuestionName, Question
into #General_Question
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,General_Q1Question,General_Q2Question,General_Q3Question,General_Q4Question,General_Q5Question,General_Q6Question,General_Q7Question,General_Q8Question,General_Q9Question,General_Q10Question,General_Q11Question,General_Q12Question,General_Q13Question,General_Q14Question,General_Q15Question,General_Q16Question,General_Q17Question,General_Q18Question,General_Q19Question,General_Q20Question,General_Q21Question

	 From #XmlData) as P
UNPIVOT
	(Question for QuestionName in 
					(  
					General_Q1Question,General_Q2Question,General_Q3Question,General_Q4Question,General_Q5Question,General_Q6Question,General_Q7Question,General_Q8Question,General_Q9Question,General_Q10Question,General_Q11Question,General_Q12Question,General_Q13Question,General_Q14Question,General_Q15Question,General_Q16Question,General_Q17Question,General_Q18Question,General_Q19Question,General_Q20Question,General_Q21Question

					)
	) as unpvt
--Select * from #General_Question


Select BranchName,	QtrYear,FileModifiedDateTime,FileType, AnswerName, Answer
Into #General_Answer
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,General_Q1Answer,General_Q2Answer,General_Q3Answer,General_Q4Answer,General_Q5Answer,General_Q6Answer,General_Q7Answer,General_Q8Answer,General_Q9Answer,General_Q10Answer,General_Q11Answer,General_Q12Answer,General_Q13Answer,General_Q14Answer,General_Q15Answer,General_Q16Answer,General_Q17Answer,General_Q18Answer,General_Q19Answer,General_Q20Answer,General_Q21Answer
	 From #XmlData) as P
UNPIVOT
	(Answer for AnswerName in 
					( 
					General_Q1Answer,General_Q2Answer,General_Q3Answer,General_Q4Answer,General_Q5Answer,General_Q6Answer,General_Q7Answer,General_Q8Answer,General_Q9Answer,General_Q10Answer,General_Q11Answer,General_Q12Answer,General_Q13Answer,General_Q14Answer,General_Q15Answer,General_Q16Answer,General_Q17Answer,General_Q18Answer,General_Q19Answer,General_Q20Answer,General_Q21Answer
					)
	) as unpvt

--Select * from #General_Answer

Select BranchName,	QtrYear,FileModifiedDateTime,FileType,ScoreName, Score
into #General_Score
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,General_Q1Score,General_Q2Score,General_Q3Score,General_Q4Score,General_Q5Score,General_Q6Score,General_Q7Score,General_Q8Score,General_Q9Score,General_Q10Score,General_Q11Score,General_Q12Score,General_Q13Score,General_Q14Score,General_Q15Score,General_Q16Score,General_Q17Score,General_Q18Score,General_Q19Score,General_Q20Score,General_Q21Score
	 From #XmlData) as P
UNPIVOT
	(Score for ScoreName in 
					( 
					General_Q1Score,General_Q2Score,General_Q3Score,General_Q4Score,General_Q5Score,General_Q6Score,General_Q7Score,General_Q8Score,General_Q9Score,General_Q10Score,General_Q11Score,General_Q12Score,General_Q13Score,General_Q14Score,General_Q15Score,General_Q16Score,General_Q17Score,General_Q18Score,General_Q19Score,General_Q20Score,General_Q21Score
					)
	) as unpvt

--Select * from #General_Score

Select BranchName,	QtrYear,FileModifiedDateTime,FileType,CommentName, Comment
into #General_Comment
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,General_Q1Comment,General_Q2Comment,General_Q3Comment,General_Q4Comment,General_Q5Comment,General_Q6Comment,General_Q7Comment,General_Q8Comment,General_Q9Comment,General_Q10Comment,General_Q11Comment,General_Q12Comment,General_Q13Comment,General_Q14Comment,General_Q15Comment,General_Q16Comment,General_Q17Comment,General_Q18Comment,General_Q19Comment,General_Q20Comment,General_Q21Comment
	 From #XmlData) as P
UNPIVOT
	(Comment for CommentName in 
					( 
					General_Q1Comment,General_Q2Comment,General_Q3Comment,General_Q4Comment,General_Q5Comment,General_Q6Comment,General_Q7Comment,General_Q8Comment,General_Q9Comment,General_Q10Comment,General_Q11Comment,General_Q12Comment,General_Q13Comment,General_Q14Comment,General_Q15Comment,General_Q16Comment,General_Q17Comment,General_Q18Comment,General_Q19Comment,General_Q20Comment,General_Q21Comment
					)
	) as unpvt
--Select * from #General_Comment
------------------</General>------------------------


------------------<HV>------------------------
Select BranchName,	QtrYear,FileModifiedDateTime,FileType,QuestionName, Question
into #HV_Question
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,HV_Q1Question,HV_Q2Question,HV_Q3Question,HV_Q4Question,HV_Q5Question,HV_Q6Question,HV_Q7Question,HV_Q8Question,HV_Q9Question,HV_Q10Question,HV_Q11Question,HV_Q12Question
	 From #XmlData) as P
UNPIVOT
	(Question for QuestionName in 
					(  
					HV_Q1Question,HV_Q2Question,HV_Q3Question,HV_Q4Question,HV_Q5Question,HV_Q6Question,HV_Q7Question,HV_Q8Question,HV_Q9Question,HV_Q10Question,HV_Q11Question,HV_Q12Question
					)
	) as unpvt
--Select * from #HV_Question

Select BranchName,	QtrYear,FileModifiedDateTime,FileType, AnswerName, Answer
Into #HV_Answer
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,HV_Q1Answer,HV_Q2Answer,HV_Q3Answer,HV_Q4Answer,HV_Q5Answer,HV_Q6Answer,HV_Q7Answer,HV_Q8Answer,HV_Q9Answer,HV_Q10Answer,HV_Q11Answer,HV_Q12Answer
	 From #XmlData) as P
UNPIVOT
	(Answer for AnswerName in 
					( 
					HV_Q1Answer,HV_Q2Answer,HV_Q3Answer,HV_Q4Answer,HV_Q5Answer,HV_Q6Answer,HV_Q7Answer,HV_Q8Answer,HV_Q9Answer,HV_Q10Answer,HV_Q11Answer,HV_Q12Answer
					)
	) as unpvt

--Select * from #HV_Answer

Select BranchName,	QtrYear,FileModifiedDateTime,FileType,ScoreName, Score
into #HV_Score
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,HV_Q1Score,HV_Q2Score,HV_Q3Score,HV_Q4Score,HV_Q5Score,HV_Q6Score,HV_Q7Score,HV_Q8Score,HV_Q9Score,HV_Q10Score,HV_Q11Score,HV_Q12Score
	 From #XmlData) as P
UNPIVOT
	(Score for ScoreName in 
					( 
HV_Q1Score,HV_Q2Score,HV_Q3Score,HV_Q4Score,HV_Q5Score,HV_Q6Score,HV_Q7Score,HV_Q8Score,HV_Q9Score,HV_Q10Score,HV_Q11Score,HV_Q12Score
					)
	) as unpvt

--Select * from #HV_Score

Select BranchName,	QtrYear,FileModifiedDateTime,FileType,CommentName, Comment
into #HV_Comment
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,HV_Q1Comment,HV_Q2Comment,HV_Q3Comment,HV_Q4Comment,HV_Q5Comment,HV_Q6Comment,HV_Q7Comment,HV_Q8Comment,HV_Q9Comment,HV_Q10Comment,HV_Q11Comment,HV_Q12Comment
	 From #XmlData) as P
UNPIVOT
	(Comment for CommentName in 
					( 
					HV_Q1Comment,HV_Q2Comment,HV_Q3Comment,HV_Q4Comment,HV_Q5Comment,HV_Q6Comment,HV_Q7Comment,HV_Q8Comment,HV_Q9Comment,HV_Q10Comment,HV_Q11Comment,HV_Q12Comment
					)
	) as unpvt
--Select * from #HV_Comment
------------------</HV>------------------------

insert into OHAInformation
(BranchName,QtrYear,FileModifiedDateTime,FileType,QuestionName,AnswerName,ScoreName,CommentName, Question,Answer,Score,Comment)
Select q.BranchName,q.QtrYear,q.FileModifiedDateTime,q.FileType,QuestionName,AnswerName,ScoreName,CommentName, Question,Answer,Score,Comment
from #CR_Question  q 
left join #CR_Score s on   Replace(Replace(s.ScoreName,'Q',''),'Score','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=s.BranchName and q.QtrYear=s.QtrYear and q.FileType=s.FileType
left join #CR_Answer a on  Replace(Replace(a.AnswerName,'Q',''),'Answer','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=a.BranchName and q.QtrYear=a.QtrYear and q.FileType=a.FileType
left join #CR_Comment c on Replace(Replace(c.CommentName,'Q',''),'Comment','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=c.BranchName and q.QtrYear=c.QtrYear and q.FileType=c.FileType
union all 
Select q.BranchName,q.QtrYear,q.FileModifiedDateTime,q.FileType,QuestionName, AnswerName,ScoreName,CommentName, Question, Answer, Score,Comment
from #General_Question q
left join #General_Score s on   Replace(Replace(s.ScoreName,'Q',''),'Score','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=s.BranchName and q.QtrYear=s.QtrYear and q.FileType=s.FileType
left join #General_Answer a on  Replace(Replace(a.AnswerName,'Q',''),'Answer','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=a.BranchName and q.QtrYear=a.QtrYear and q.FileType=a.FileType
left join #General_Comment c on Replace(Replace(c.CommentName,'Q',''),'Comment','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=c.BranchName and q.QtrYear=c.QtrYear and q.FileType=c.FileType
union all 
Select q.BranchName,q.QtrYear,q.FileModifiedDateTime,q.FileType,QuestionName, AnswerName,ScoreName,CommentName, Question, Answer, Score,Comment
from #HV_Question q
left join #HV_Score s on   Replace(Replace(s.ScoreName,'Q',''),'Score','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=s.BranchName and q.QtrYear=s.QtrYear and q.FileType=s.FileType
left join #HV_Answer a on  Replace(Replace(a.AnswerName,'Q',''),'Answer','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=a.BranchName and q.QtrYear=a.QtrYear and q.FileType=a.FileType
left join #HV_Comment c on Replace(Replace(c.CommentName,'Q',''),'Comment','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=c.BranchName and q.QtrYear=c.QtrYear and q.FileType=c.FileType



End


