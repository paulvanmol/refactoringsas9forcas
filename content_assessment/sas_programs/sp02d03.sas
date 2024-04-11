ODS noproctitle;
title "Fish Count by Species";
proc freq data=sashelp.fish order=freq;
	tables species / plots=(freqplot);
run;

