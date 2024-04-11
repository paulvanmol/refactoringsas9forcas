/* ----------------------------------------
Code exported from SAS Enterprise Guide
DATE: 8. november 2021     TIME: 15:31:05
PROJECT: Hadoop - Right Click and Open in EG7_1
PROJECT PATH: U:\Platform\Hadoop\programs\Hadoop - Right Click and Open in EG7_1.egp
---------------------------------------- */

/* Unable to determine code to assign library Hadoop Source Data_HDPSRC on SASApp */

/* ---------------------------------- */
/* MACRO: enterpriseguide             */
/* PURPOSE: define a macro variable   */
/*   that contains the file system    */
/*   path of the WORK library on the  */
/*   server.  Note that different     */
/*   logic is needed depending on the */
/*   server type.                     */
/* ---------------------------------- */
%macro enterpriseguide;
%global sasworklocation;
%local tempdsn unique_dsn path;

%if &sysscp=OS %then %do; /* MVS Server */
	%if %sysfunc(getoption(filesystem))=MVS %then %do;
        /* By default, physical file name will be considered a classic MVS data set. */
	    /* Construct dsn that will be unique for each concurrent session under a particular account: */
		filename egtemp '&egtemp' disp=(new,delete); /* create a temporary data set */
 		%let tempdsn=%sysfunc(pathname(egtemp)); /* get dsn */
		filename egtemp clear; /* get rid of data set - we only wanted its name */
		%let unique_dsn=".EGTEMP.%substr(&tempdsn, 1, 16).PDSE"; 
		filename egtmpdir &unique_dsn
			disp=(new,delete,delete) space=(cyl,(5,5,50))
			dsorg=po dsntype=library recfm=vb
			lrecl=8000 blksize=8004 ;
		options fileext=ignore ;
	%end; 
 	%else %do; 
        /* 
		By default, physical file name will be considered an HFS 
		(hierarchical file system) file. 
		*/
		%if "%sysfunc(getoption(filetempdir))"="" %then %do;
			filename egtmpdir '/tmp';
		%end;
		%else %do;
			filename egtmpdir "%sysfunc(getoption(filetempdir))";
		%end;
	%end; 
	%let path=%sysfunc(pathname(egtmpdir));
    %let sasworklocation=%sysfunc(quote(&path));  
%end; /* MVS Server */
%else %do;
	%let sasworklocation = "%sysfunc(getoption(work))/";
%end;
%if &sysscp=VMS_AXP %then %do; /* Alpha VMS server */
	%let sasworklocation = "%sysfunc(getoption(work))";                         
%end;
%if &sysscp=CMS %then %do; 
	%let path = %sysfunc(getoption(work));                         
	%let sasworklocation = "%substr(&path, %index(&path,%str( )))";
%end;
%mend enterpriseguide;

%enterpriseguide


/* Conditionally delete set of tables or views, if they exists          */
/* If the member does not exist, then no action is performed   */
%macro _eg_conditional_dropds /parmbuff;
	
   	%local num;
   	%local stepneeded;
   	%local stepstarted;
   	%local dsname;
	%local name;

   	%let num=1;
	/* flags to determine whether a PROC SQL step is needed */
	/* or even started yet                                  */
	%let stepneeded=0;
	%let stepstarted=0;
   	%let dsname= %qscan(&syspbuff,&num,',()');
	%do %while(&dsname ne);	
		%let name = %sysfunc(left(&dsname));
		%if %qsysfunc(exist(&name)) %then %do;
			%let stepneeded=1;
			%if (&stepstarted eq 0) %then %do;
				proc sql;
				%let stepstarted=1;

			%end;
				drop table &name;
		%end;

		%if %sysfunc(exist(&name,view)) %then %do;
			%let stepneeded=1;
			%if (&stepstarted eq 0) %then %do;
				proc sql;
				%let stepstarted=1;
			%end;
				drop view &name;
		%end;
		%let num=%eval(&num+1);
      	%let dsname=%qscan(&syspbuff,&num,',()');
	%end;
	%if &stepstarted %then %do;
		quit;
	%end;
%mend _eg_conditional_dropds;


/* save the current settings of XPIXELS and YPIXELS */
/* so that they can be restored later               */
%macro _sas_pushchartsize(new_xsize, new_ysize);
	%global _savedxpixels _savedypixels;
	options nonotes;
	proc sql noprint;
	select setting into :_savedxpixels
	from sashelp.vgopt
	where optname eq "XPIXELS";
	select setting into :_savedypixels
	from sashelp.vgopt
	where optname eq "YPIXELS";
	quit;
	options notes;
	GOPTIONS XPIXELS=&new_xsize YPIXELS=&new_ysize;
%mend _sas_pushchartsize;

/* restore the previous values for XPIXELS and YPIXELS */
%macro _sas_popchartsize;
	%if %symexist(_savedxpixels) %then %do;
		GOPTIONS XPIXELS=&_savedxpixels YPIXELS=&_savedypixels;
		%symdel _savedxpixels / nowarn;
		%symdel _savedypixels / nowarn;
	%end;
%mend _sas_popchartsize;


ODS PROCTITLE;
OPTIONS DEV=SVG;
GOPTIONS XPIXELS=0 YPIXELS=0;
%macro HTML5AccessibleGraphSupported;
    %if %_SAS_VERCOMP_FV(9,4,4, 0,0,0) >= 0 %then ACCESSIBLE_GRAPH;
%mend;
FILENAME EGHTMLX TEMP;
ODS HTML5(ID=EGHTMLX) FILE=EGHTMLX
    OPTIONS(BITMAP_MODE='INLINE')
    %HTML5AccessibleGraphSupported
    ENCODING='utf-8'
    STYLE=HTMLBlue
    NOGTITLE
    NOGFOOTNOTE
    GPATH=&sasworklocation
;

/*   START OF NODE: 1_Using the SAS Access Libname Engine   */
%LET _CLIENTTASKLABEL='1_Using the SAS Access Libname Engine';
%LET _CLIENTPROCESSFLOWNAME='Process Flow';
%LET _CLIENTPROJECTPATH='U:\Platform\Hadoop\programs\Hadoop - Right Click and Open in EG7_1.egp';
%LET _CLIENTPROJECTPATHHOST='kochow10';
%LET _CLIENTPROJECTNAME='Hadoop - Right Click and Open in EG7_1.egp';
%LET _SASPROGRAMFILE='';
%LET _SASPROGRAMFILEHOST='';

options sastrace=',,d,d' sastraceloc=saslog nostsuffix;
LIBNAME sassrc BASE "d:/sasfolders/orion_datawarehouse/Data/01_Source_Data" ; 
libname hivedb hadoop server="192.168.77.129" port=10000 schema=default user=hdfs password="Orion123";

proc delete data=hivedb.customer;
data hivedb.customer;
  set sassrc.customer;
run;

data hivedb.customer;
  set sassrc.customer;
run;

data hivedb.order_fact;
  set oristar.order_fact;
run;

data hivedb.staff;
  set sassrc.staff;
run;



%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROCESSFLOWNAME=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTPATHHOST=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;
%LET _SASPROGRAMFILEHOST=;


/*   START OF NODE: Data Set Attributes for Class   */
%LET _CLIENTTASKLABEL='Data Set Attributes for Class';
%LET _CLIENTPROCESSFLOWNAME='Process Flow';
%LET _CLIENTPROJECTPATH='U:\Platform\Hadoop\programs\Hadoop - Right Click and Open in EG7_1.egp';
%LET _CLIENTPROJECTPATHHOST='kochow10';
%LET _CLIENTPROJECTNAME='Hadoop - Right Click and Open in EG7_1.egp';

/* -------------------------------------------------------------------
   Code generated by SAS Task

   Generated on: Tuesday, November 18, 2014 at 18:33:38
   By task: Data Set Attributes

   Input Data: SASApp:Hadoop Source Data_HDPSRC.CLASS
   Server:  SASApp
   ------------------------------------------------------------------- */

%_eg_conditional_dropds(WORK.CONTContentsForCLASS);
TITLE;
FOOTNOTE;
FOOTNOTE1 "Generated by the SAS System (&_SASSERVERNAME, &SYSSCPL) on %TRIM(%QSYSFUNC(DATE(), NLDATE20.)) at %TRIM(%SYSFUNC(TIME(), TIMEAMPM12.))";
PROC FORMAT;
   VALUE _EG_VARTYPE 1="Numeric" 2="Character" OTHER="unknown";
RUN;

PROC DATASETS NOLIST NODETAILS; 
   CONTENTS DATA=HDPSRC.CLASS OUT=WORK.SUCOUT1;

RUN;

DATA WORK.CONTContentsForCLASS(LABEL="Contents Details for CLASS");
   SET WORK.SUCOUT1;
RUN;

PROC DELETE DATA=WORK.SUCOUT1;
RUN;

%LET _LINESIZE=%SYSFUNC(GETOPTION(LINESIZE));

PROC SQL;
CREATE VIEW WORK.SCVIEW AS 
	SELECT DISTINCT memname LABEL="Table Name", 
			memlabel LABEL="Label", 
			memtype LABEL="Type", 
			crdate LABEL="Date Created", 
			modate LABEL="Date Modified", 
			nobs LABEL="Number of Obs.", 
			charset LABEL="Char. Set", 
			protect LABEL="Password Protected", 
			typemem LABEL="Data Set Type" FROM WORK.CONTContentsForCLASS
	ORDER BY memname ; 

CREATE TABLE WORK.SCTABLE AS
	SELECT * FROM WORK.SCVIEW
		WHERE memname='CLASS';
QUIT;

TITLE "Tables on &_SASSERVERNAME"; 
PROC REPORT DATA=WORK.SCTABLE; 
   DEFINE  MEMLABEL / DISPLAY WIDTH=&_LINESIZE; 
   COLUMN memname memlabel memtype crdate modate nobs charset protect typemem; 
RUN;QUIT;

PROC SORT DATA=WORK.CONTContentsForCLASS OUT=WORK.CONTContentsForCLASS;
   BY memname name;
RUN;

OPTIONS NOBYLINE;
TITLE 'Variables in Table: #BYVAL(memname)'; 

PROC SQL;
DROP TABLE WORK.SCTABLE;
CREATE TABLE WORK.SCTABLE AS
	SELECT * FROM WORK.CONTContentsForCLASS
		WHERE memname='CLASS';
QUIT;

PROC REPORT DATA=WORK.SCTABLE NOWINDOWS; 
   FORMAT TYPE _EG_VARTYPE.; 
   DEFINE LABEL / DISPLAY WIDTH=&_LINESIZE; 
   LABEL NAME="Name" LABEL="Label" TYPE="Type" LENGTH="Length" INFORMAT="Informat" FORMAT="Format"; 
   BY memname NOTSORTED;  
   COLUMN name varnum type format label length;  
 QUIT;  

PROC SQL;
	DROP TABLE WORK.SCTABLE;
	DROP VIEW WORK.SCVIEW;
QUIT;

PROC CATALOG CATALOG=WORK.FORMATS;
   DELETE _EG_VARTYPE / ENTRYTYPE=FORMAT;
RUN;
OPTIONS BYLINE;
/* -------------------------------------------------------------------
   End of task code
   ------------------------------------------------------------------- */
RUN; QUIT;
TITLE; FOOTNOTE;


%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTNAME=;


/*   START OF NODE: Upload to LASR   */
%LET _CLIENTTASKLABEL='Upload to LASR';
%LET _CLIENTPROCESSFLOWNAME='Process Flow';
%LET _CLIENTPROJECTPATH='U:\Platform\Hadoop\programs\Hadoop - Right Click and Open in EG7_1.egp';
%LET _CLIENTPROJECTPATHHOST='kochow10';
%LET _CLIENTPROJECTNAME='Hadoop - Right Click and Open in EG7_1.egp';

/* Status Checkpoint Macro */
%macro statuscheckpoint(maxokstatus=4, varstocheck=SYSERR SYSLIBRC );
   %GLOBAL LASTSTEPRC;
   %LET pos=1;
   %let var=notset;
   %let var=%SCAN(&varstocheck.,&pos.);
   %DO %WHILE ("&VAR." ne ""); 
      /* Retrieve the next return code to check */ 
	  %if (%symexist(&VAR.)) %then %do; 
	     %let val=&&&VAR..; 
	     %if (("&VAL." ne "") and %eval(&VAL. > &maxokstatus.)) %then %do;  
		    %put FAIL = &VAR.=&VAL. / SYSCC=&SYSCC.; 
           %let LASTSTEPRC=&VAL.; 
		 %end; 
	  %end;  
	  %let pos = %eval(&pos.+1); 
      %let var=%SCAN(&varstocheck.,&pos.);   
   %END; 
%mend; 


%macro codeBody;
    %GLOBAL LASTSTEPRC;
    LIBNAME LASR0_1 META Library="Enterprise LASR Data_LASR0_1" METAOUT=DATA;

    /* Remove existing table from LASR if loaded already */ 
    %macro deletedsifexists(lib,name);
        %if %sysfunc(exist(&lib..&name.)) %then %do;
        proc datasets library=&lib. nolist;
            delete &name.;
        quit;
    %end;
    %mend deletedsifexists;

    FOOTNOTE;
    FOOTNOTE1 "Generated by the SAS System (&_SASSERVERNAME, &SYSSCPL) on %TRIM(%QSYSFUNC(DATE(), NLDATE20.)) at %TRIM(%SYSFUNC(TIME(), TIMEAMPM12.))";
    %deletedsifexists(LASR0_1, CLASS);
    /* Loading through the SASIOLA Engine */ 
    data LASR0_1.CLASS (  );
        set HDPSRC.CLASS (  );
    run;

    LIBNAME LASR0_1 META Library="Enterprise LASR Data_LASR0_1"; 

%mend;

%codeBody;
%statuscheckpoint;
/* Register Table Macro */
%macro registertable( REPOSITORY=Foundation, REPOSID=, LIBRARY=, TABLE=, FOLDER=, TABLEID=, PREFIX= );
   %if %symexist(LASTSTEPRC) %then %do;
      %if %eval(&LASTSTEPRC. <= 4) %then %do;
         /* Mask special characters */
            %let REPOSITORY=%superq(REPOSITORY);
            %let LIBRARY   =%superq(LIBRARY);
            %let FOLDER    =%superq(FOLDER);
            %let TABLE     =%superq(TABLE);
            %let REPOSARG=%str(REPNAME="&REPOSITORY.");
            %if ("&REPOSID." ne "") %THEN %LET REPOSARG=%str(REPID="&REPOSID.");
            %if ("&TABLEID." ne "") %THEN %LET SELECTOBJ=%str(&TABLEID.);
            %else                         %LET SELECTOBJ=&TABLE.;
            %if ("&FOLDER." ne "") %THEN
               %PUT INFO: Registering &FOLDER./&SELECTOBJ. to &LIBRARY. library.;
            %else
               %PUT INFO: Registering &SELECTOBJ. to &LIBRARY. library.;
            proc metalib;
               omr (
                  library="&LIBRARY." 
                  %str(&REPOSARG.) 
                   ); 
               %if ("&TABLEID." eq "") %THEN %DO;
                  %if ("&FOLDER." ne "") %THEN %DO;
                     folder="&FOLDER.";
                  %end;
               %end;
               %if ("&PREFIX." ne "") %THEN %DO;
                  prefix="&PREFIX.";
               %end;
               select ('CLASS'); 
            run; 
            quit;
      %end;
   %end;
%mend;
%registerTable(
     LIBRARY=%nrstr(/Enterprise/Enterprise LASR Data_LASR0_1)
   , FOLDER=%nrstr(/Enterprise)
   );

%macro smpReport();
	%GLOBAL LASTSTEPRC; 
	%if %symexist(LASTSTEPRC) %then %do;    
		%if %eval(&LASTSTEPRC. <= 4) %then %do; 
			title 'The Upload to LASR Task';
			data upload_lasr_smp_report;
			length lbl $200. val $200.;
				lbl = 'Host Node';
				val = 'sdkstudent.sdk.sas.com';
				output;
				lbl = 'Execution Mode';
				val = 'Symmetric Multi-Processing';
				output;
				lbl = 'Number of Compute Nodes';
				val = '1';
				output;
				lbl = 'Data';
				val = 'Enterprise LASR Data_LASR0_1.CLASS';
				output;
				lbl = 'Metadata Location';
				val = '/Enterprise';
				output;
			run;
			proc report data=upload_lasr_smp_report nowd ;
				columns ('Performance Information' (lbl val));
				define val / display '';
				define lbl / display '' style(column)=rowheader;
			run;
			quit;
			title;
		%end;
	%end;
%mend;
%statuscheckpoint;
%smpReport;

%LET LASTSTEPRC=0;
FOOTNOTE;


%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTNAME=;


/*   START OF NODE: 2_Using the SQL Pass-Through Facility   */
%LET _CLIENTTASKLABEL='2_Using the SQL Pass-Through Facility';
%LET _CLIENTPROCESSFLOWNAME='Process Flow';
%LET _CLIENTPROJECTPATH='U:\Platform\Hadoop\programs\Hadoop - Right Click and Open in EG7_1.egp';
%LET _CLIENTPROJECTPATHHOST='kochow10';
%LET _CLIENTPROJECTNAME='Hadoop - Right Click and Open in EG7_1.egp';
%LET _SASPROGRAMFILE='';
%LET _SASPROGRAMFILEHOST='';

title "Customers Born in 1980 and later - Birth Year Promotion List";

proc sql; 
 connect to hadoop(server="192.168.77.129"

					 subprotocol=hive2
					 port=10000 
					 schema=default 
					 user=hdfs
					 password=Orion123
				
                     );
 select *
     from connection to hadoop
        (select Customer_Name,
              Birth_Date, Customer_Address, 
                Country, Gender
         from customer 
         where Birth_Date >=  '1980-01-01' 
                  
          );
     disconnect from hadoop;
quit;
title;


%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROCESSFLOWNAME=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTPATHHOST=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;
%LET _SASPROGRAMFILEHOST=;


/*   START OF NODE: 3_Performance Considerations ( creating Hadoop partitiions)   */
%LET _CLIENTTASKLABEL='3_Performance Considerations ( creating Hadoop partitiions)';
%LET _CLIENTPROCESSFLOWNAME='Process Flow';
%LET _CLIENTPROJECTPATH='U:\Platform\Hadoop\programs\Hadoop - Right Click and Open in EG7_1.egp';
%LET _CLIENTPROJECTPATHHOST='kochow10';
%LET _CLIENTPROJECTNAME='Hadoop - Right Click and Open in EG7_1.egp';
%LET _SASPROGRAMFILE='';
%LET _SASPROGRAMFILEHOST='';

options sastrace=',,d,d' sastraceloc=saslog nostsuffix;

proc sql;
	connect to hadoop(server="192.168.77.129"
		subprotocol=hive2
		port=10000 
		schema=default 
		user=hdfs
		password=Orion123
		);
execute (insert overwrite table classopdelt partition(sex = 'F') select name, age, height, weight from class where sex = 'F') by hadoop;
execute (insert overwrite table classopdelt partition(sex = 'M') select name, age, height, weight from class where sex = 'M') by hadoop;
disconnect from hadoop;
quit;




%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROCESSFLOWNAME=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTPATHHOST=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;
%LET _SASPROGRAMFILEHOST=;


/*   START OF NODE: 4_Base SAS In-Database Processing   */
%LET _CLIENTTASKLABEL='4_Base SAS In-Database Processing';
%LET _CLIENTPROCESSFLOWNAME='Process Flow';
%LET _CLIENTPROJECTPATH='U:\Platform\Hadoop\programs\Hadoop - Right Click and Open in EG7_1.egp';
%LET _CLIENTPROJECTPATHHOST='kochow10';
%LET _CLIENTPROJECTNAME='Hadoop - Right Click and Open in EG7_1.egp';
%LET _SASPROGRAMFILE='';
%LET _SASPROGRAMFILEHOST='';

libname hivedb hadoop server="192.168.77.129" port=10000 
        schema=default user=hdfs password="Orion123";

options sqlgeneration=(none DBMS='HADOOP TERADATA DB2 ORACLE 
				    NETEZZA ASTER GREENPLM');
proc options option=sqlgeneration;
run;

options sastrace=',,,d' sastraceloc=saslog
        nostsuffix;

proc format;
   value $genfmt 'F'='Female'
 			    'M'='Male';
run;
proc tabulate data=hivedb.staff;
   where upcase(job_title) contains ('FINANCIAL');
  * where scan(job_title, -1, ' ')='III' ;
   class gender job_title;
   var salary;
   table job_title=' ' all, 
		 gender=' '*salary*(mean min max) all*salary*(mean min max)
		 / box=job_title;
   format gender $genfmt.;
run;

proc freq data=hivedb.order_fact;
   table order_type ;
run;

libname hivedb clear;
options sastrace=off;


%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROCESSFLOWNAME=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTPATHHOST=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;
%LET _SASPROGRAMFILEHOST=;

;*';*";*/;quit;run;
ODS _ALL_ CLOSE;
