***********************************************************;
*  LESSON 1, PRACTICE 2                                   *;
*    a) Submit the program and view the generated output. *;
*       Notice all data cells have a yellow background.   *;
*    b) Use the CATALOG procedure to determine the name   *;
*       of the format that will apply custom colors to    *;
*       the values of the Inventory column.               *;
*    c) In the TABULATE step, replace LightYellow with    *;
*       the discovered format name.                       *;
***********************************************************;

* Add a PROC CATALOG step to list the contents;
* of the MYLIB.FORMATS catalog                ;




proc sort data=sashelp.shoes out=work.shoeProducts;
	by Region Product;
run;

* Replace LightYellow with the format name;
proc tabulate data=work.shoeproducts format=comma10.;
	var Inventory;
	class Region / order=formatted missing;
	class Product / order=formatted missing;;
	table Region, Product*Inventory*sum={label=' '} 
			* {style={background=LightYellow}};
	title1 'Product Inventory by Region';
run;
