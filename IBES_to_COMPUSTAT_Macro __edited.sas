/* 
	This macro creates a dataset based on Funda and adds permno, cusip and ibes_ticker
*/

/**************************************************************************************************************************************************/
/*	Note: V2 only pulls companies that have CURCD='USD' and it left joins quarterly data to annual in getf_1 as opposed to the previous version */

%macro IbesQ(dsout=, Qvars=, Avars=, year1=, year2=);

	/* get funda -- used for datadate_annual */
	data getf_0A (keep = gvkey fyear datadate_annual Tic Conm Fic Sich &Avars);
	set comp.funda;
	if indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C' and curcd='USD';
	if &year1 <= fyear <= &year2;
	rename datadate=datadate_annual;
	run;
	proc sort data=getf_0A nodupkey; by gvkey fyear; run;

	/* FundQ data */
	/* does not have CURCD */
	data getf_0Q (keep = gvkey fyearq fyear fqtr datadate fyr ibq rdq &QVars);
	set comp.fundq;
	if &year1 <= fyearq <= &year2;
	if indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C' ;
	rename fyearQ=fyear;
	run;

	/* bring in Quarterly to Annual Data */
	proc sql; 	create table getf_1 as
				select a.gvkey, a.datadate_annual a.fyear, b.fqtr, b.fyr, b.datadate, a.tic, a.conm, a.*, b.rdq, b.ibq, %do_over(values=&QVars, between=comma, phrase=b.?) 
				from getf_0A as a left join getf_0Q as b
				on a.gvkey eq b.gvkey and a.fyear eq b.fyear
				order by a.gvkey, a.fyear, a.fqtr
				;
				quit;

	/* Permno as of datadate (datadate is Quarterly)*/
	proc sql; 
	  create table getf_2 as 
	  select a.*, b.lpermno as permno
	  from getf_1 a left join crsp.ccmxpf_linktable b 
	    on a.gvkey eq b.gvkey 
	    and b.lpermno ne . 
	    and b.linktype in ("LC" "LN" "LU" "LX" "LD" "LS") 
	    and b.linkprim IN ("C", "P")  
	    and ((a.datadate >= b.LINKDT) or b.LINKDT eq .B) and  
	       ((a.datadate <= b.LINKENDDT) or b.LINKENDDT eq .E)   
		order by gvkey, fyear, fqtr
		; 
		quit; 

	/* retrieve historic cusip */
	proc sql;
	  create table getf_3 as
	  select a.*, b.ncusip
	  from getf_2 a left join crsp.dsenames b
	  on 
	        a.permno = b.PERMNO
	    and b.namedt <= a.datadate <= b.nameendt
	    and b.ncusip ne ""
		order by gvkey, fyear, fqtr
		;
	  quit;
 
	/* get ibes ticker */
	proc sql;
	  create table getf_4 as
	  select distinct a.*, b.ticker as ibes_ticker
	  from getf_3 a left join ibes.idsum b
	  on 	a.NCUSIP = b.CUSIP
	    and a.datadate > b.SDATES 
		order by gvkey, fyear, fqtr
		;
	quit;

	/* take care of Duplicates */	
	proc sort data=getf_4; by gvkey fyear descending datadate fqtr; run;							
	proc sort data=getf_4 out=getf_4test dupout=getf_dup nodupkey; by gvkey fyear fqtr; run;					
	
	data getf_dup; set getf_dup; FYQ=gvkey||fyear||fqtr; keep gvkey fyear fqtr FYQ ; run;

	%array(FYQ , data=getf_dup, var=FYQ); run;

	data getf_5; 	set getf_4; 
					FYQ=gvkey||fyear||fqtr;
					run;
	data getf_6; 	set getf_5; 
					%do_over(FYQ ,between=; , phrase= if FYQ eq "?" and month(datadate_annual) eq fyr then keep=1; );	
					run;

	/*¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯*/
	/*	The Logic Behind Dups Deletion:
	/*
	/* 	Give preference to Observations where annual datadate is the most recent, 
	/*	then give preference to when keep=1 --> when the fiscal year end month (FYR) is the same as the datadate_annual month --> this gives preferences to OBS with non-missing FYR and where the fiscal year matches the FQTR,
	/*	then when IBQ is the highest (to favor non-missing observations),		
	/*	then when RDQ is the most recent 
	/*__________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________*/

	proc sort 	data=getf_6; by gvkey fyear fqtr descending datadate_annual descending keep descending ibq descending rdq ; run;		
	proc sort 	data=getf_6 out=getf_7 nodupkey; by gvkey fyear fqtr; run; 									

	/* Rename */
	data getf_8; set getf_7;
			rename datadate_annual=Datadate_1 datadate=Datadate_Q;	/* old = new name */
			run;
	data getf_8; set getf_8;
			rename Datadate_1=Datadate;
			run;

	/* Save final */
	data &dsout; set getf_8; drop keep FYQ; run;

	/*cleanup *//* delete all datasets with the prefix getf_ */
	proc datasets library=work; delete getf_: ; quit;

%mend;

/*****************************************************************************************************************/

%macro	cleanprefix(prefix= );
	/*cleanup datasets with prefix */
	proc datasets library=work nolist; delete &prefix: ; quit;
%mend;

/*****************************************************************************************************************/


/*****************************************************************************************************************/

%macro IbesA(dsout=, AVars=, year1=2010, year2=2015);

/* Funda data */
data getf_1 (keep = key gvkey fyear datadate sich &AVars);
set comp.funda;
if &year1 <= fyear <= &year2;
if indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C' ;
key = gvkey || fyear;
run;

/* if sich is missing, use the one of last year (note sorted descending by fyear) */
data getf_1 (drop = sich_prev);
set getf_1;
retain sich_prev;
by gvkey;
if first.gvkey then sich_prev = .;
if missing(sich) then sich = sich_prev;
sich_prev = sich;
run;

/* Permno as of datadate*/
proc sql; 
  create table getf_2 as 
  select a.*, b.lpermno as permno
  from getf_1 a left join crsp.ccmxpf_linktable b 
    on a.gvkey eq b.gvkey 
    and b.lpermno ne . 
    and b.linktype in ("LC" "LN" "LU" "LX" "LD" "LS") 
    and b.linkprim IN ("C", "P")  
    and ((a.datadate >= b.LINKDT) or b.LINKDT eq .B) and  
       ((a.datadate <= b.LINKENDDT) or b.LINKENDDT eq .E)   ; 
quit; 

/* retrieve historic cusip */
proc sql;
  create table getf_3 as
  select a.*, b.ncusip
  from getf_2 a left join crsp.dsenames b
  on 
        a.permno = b.PERMNO
    and b.namedt <= a.datadate <= b.nameendt
    and b.ncusip ne "";
  quit;
 
/* force unique records */
proc sort data=getf_3 nodupkey; by key;run;
 
/* get ibes ticker */
proc sql;
  create table getf_4 as
  select distinct a.*, b.ticker as ibes_ticker
  from getf_3 a left join ibes.idsum b
  on 
        a.NCUSIP = b.CUSIP
    and a.datadate > b.SDATES ;
quit;

/* force unique records */
proc sort data=getf_4 out=&dsout nodupkey; by key;run;

/*cleanup *//* delete all datasets with the prefix getf_ */
proc datasets library=work; delete getf_: ; quit;

%mend;

/*****************************************************************************************************************/

%macro getFundaWithIBESTicker(dsout=, fundaVars=, year1=2010, year2=2015);

/* Funda data */
data getf_1 (keep = key gvkey fyear datadate sich &fundaVars);
set comp.funda;
if &year1 <= fyear <= &year2;
if indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C' ;
key = gvkey || fyear;
run;

/* if sich is missing, use the one of last year (note sorted descending by fyear) */
data getf_1 (drop = sich_prev);
set getf_1;
retain sich_prev;
by gvkey;
if first.gvkey then sich_prev = .;
if missing(sich) then sich = sich_prev;
sich_prev = sich;
run;

/* Permno as of datadate*/
proc sql; 
  create table getf_2 as 
  select a.*, b.lpermno as permno
  from getf_1 a left join crsp.ccmxpf_linktable b 
    on a.gvkey eq b.gvkey 
    and b.lpermno ne . 
    and b.linktype in ("LC" "LN" "LU" "LX" "LD" "LS") 
    and b.linkprim IN ("C", "P")  
    and ((a.datadate >= b.LINKDT) or b.LINKDT eq .B) and  
       ((a.datadate <= b.LINKENDDT) or b.LINKENDDT eq .E)   ; 
quit; 

/* retrieve historic cusip */
proc sql;
  create table getf_3 as
  select a.*, b.ncusip
  from getf_2 a left join crsp.dsenames b
  on 
        a.permno = b.PERMNO
    and b.namedt <= a.datadate <= b.nameendt
    and b.ncusip ne "";
  quit;
 
/* force unique records */
proc sort data=getf_3 nodupkey; by key;run;
 
/* get ibes ticker */
proc sql;
  create table getf_4 as
  select distinct a.*, b.ticker as ibes_ticker
  from getf_3 a left join ibes.idsum b
  on 
        a.NCUSIP = b.CUSIP
    and a.datadate > b.SDATES ;
quit;

/* force unique records */
proc sort data=getf_4 out=&dsout nodupkey; by key;run;

/*cleanup *//* delete all datasets with the prefix getf_ */
proc datasets library=work; delete getf_: ; quit;

%mend;

/*****************************************************************************************************************/


%macro runquit;
; run; quit;
%if &syserr. ne 0 %then %do;
%abort cancel ;
%end;
%mend runquit;
