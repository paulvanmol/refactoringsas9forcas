data mylib.mpg_forecast;
	set sashelp.cars;
	keep make model type year MPG:;
	start=year(today());

	do year=start+1 to start+10;

		if type in("Truck", "SUV") then
			do;
				mpg_city=mpg_city*1.1;
				mpg_highway=mpg_highway*1.1;
			end;
		else
			do;
				mpg_city=mpg_city*1.05;
				mpg_highway=mpg_highway*1.05;
			end;
		output;
	end;
run;

proc means data=mylib.mpg_forecast min max mean maxdec=2;
	var MPG_City MPG_Highway;
	class year type;
run;