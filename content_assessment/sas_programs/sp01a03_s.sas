************************************************************;
*  Activity 1.03 - Solution                                *;
*    1) Submit the program.                                *;
*    2) Use the LOG tab to identify any warnings or errors.*;
*    3) Fix the code and rerun the program.                *;
************************************************************;
PROC SQL;
	CREATE TABLE UseWarm AS 
		SELECT Area, Name, Latitude, Longitude, Farenheit 
		FROM SASHELP.SPRINGS 
		WHERE Farenheit between 85 and 101 AND
			  Area in ('Adel', 'Albuquerque', 'Boise', 'Bozeman', 'Brigham City', 'Cedar City', 'Clifton', 
			  		   'Death Valley', 'Driggs', 'Fresno', 'Klamath Falls', 'Las Vegas', 'Millett', 'Pocatello', 
			  		   'Preston', 'Reno', 'Salt Lake City', 'Salton Sea', 'San Jose', 'Santa Rosa', 'Susanville', 
			  		   'Tonopah', 'Tooele', 'Tucson', 'Wells', 'Winnemucca')
		ORDER BY Farenheit DESC;

	CREATE TABLE UseCool AS 
		SELECT Area, Name, Latitude, Longitude, Farenheit 
		FROM SASHELP.SPRINGS 
		WHERE Farenheit between 85 and 101 AND
			  Area in ('Adel', 'Albuquerque', 'Boise', 'Bozeman', 'Brigham City', 'Cedar City', 'Clifton', 
			  		   'Death Valley', 'Driggs', 'Fresno', 'Klamath Falls', 'Las Vegas', 'Millett', 'Pocatello', 
			  		   'Preston', 'Reno', 'Salt Lake City', 'Salton Sea', 'San Jose', 'Santa Rosa', 'Susanville', 
			  		   'Tonopah', 'Tooele', 'Tucson', 'Wells', 'Winnemucca')
		ORDER BY Farenheit;

QUIT;

PROC RANK DATA=UseWarm DESCENDING 
	OUT=RankUseWarm(LABEL="Rank Analysis for UseWarm");
	VAR Farenheit;
	RANKS Rank_Farenheit;
RUN;

PROC RANK DATA=UseCool 
	OUT=RankUseCool(LABEL="Rank Analysis for UseCool");
	VAR Farenheit;
	RANKS Rank_Farenheit;
RUN;

DATA VisitSprings;
	SET RankUseWarm(WHERE=(Rank_Farenheit LE 10) IN=Warm) 
		RankUseCool(WHERE=(Rank_Farenheit LE 10) IN=Cool);
	IF Warm THEN Type='Warmest';
	ELSE Type='Coolest';
RUN;

PROC SORT DATA=VisitSprings;
	BY Type Area Farenheit Name;
RUN;

TITLE;
TITLE1 "Visit these Hotsprings";
FOOTNOTE;

PROC PRINT DATA=VisitSprings NOOBS LABEL;
	VAR Type Area Name Farenheit;
RUN;

ods graphics / reset width=7in height=5in;

proc sgmap plotdata=WORK.VISITSPRINGS;
	openstreetmap;
	bubble x=Longitude y=Latitude size=Farenheit / 
 		   group=Type transparency=0.25 name="springsPlot";
	keylegend "bubblePlot" / title='Type';
run;

ods graphics / reset;