/* 1 */
* macrovar location path;
*%let loc_path=/phoenix-export/ssemonthly/homes/Cecily.Hoffritz@sas.com/; 

*libname mysas base "&loc_path.";

proc casutil;
	load data=mysas.customer_info outcaslib="casuser"
	casout="customer_info";
run;


/* 2 */
/*
data mysas.CouponMailer;
   set mysas.customer_info end=eof ;
   if quantity in (1,2) then Discount=.15;
      else if quantity in (3,4) then discount=.20;
      else discount=.30;
   MailDate=mdy(12,day(Order_Date),year(today()));
   if eof then put _threadid_=   _N_=; 
   format MailDate worddate.;
 keep Customer_Name City Postal_Code State_Province discount MailDate; 
run;
*/
data casuser.CouponMailer;
   set casuser.customer_info end=eof ;
   if quantity in (1,2) then Discount=.15;
      else if quantity in (3,4) then discount=.20;
      else discount=.30;
   MailDate=mdy(12,day(Order_Date),year(today()));
   if eof then put _threadid_=   _N_=; 
   format MailDate nldate.;
 keep Customer_Name City Postal_Code State_Province discount MailDate; 
run;

/* 3 */
data mysas.CustomerCounts;
   set mysas.customer_info end=eof;
   if scan(customer_Group,3) ='Gold' then GoldCount+1;
   else if scan(customer_Group,1,' ') ='Internet/Catalog' 
   then ICCount+1;
   else OtherCount+1;
   if eof then output;
   keep GoldCount ICCount OtherCount;
   label GoldCount='Total # Gold Memberships'
         ICCount='Total # Internet/Catalog Memberships'
         OtherCount='Total # Other Memberships';
run;

data casuser.CustomerCounts /single=yes ;
   set casuser.customer_info end=eof;
   if scan(customer_Group,3) ='Gold' then GoldCount+1;
   else if scan(customer_Group,1,' ') ='Internet/Catalog' 
   then ICCount+1;
   else OtherCount+1;
   if eof then output;
   keep GoldCount ICCount OtherCount;
   label GoldCount='Total # Gold Memberships'
         ICCount='Total # Internet/Catalog Memberships'
         OtherCount='Total # Other Memberships';
run;


/* 4 */
/*
proc sql;
select Customer_Name
     , quantity
     , Customer_BirthDate
   from mysas.customer_info
   where customer_group contains 'Gold' 
     and Continent eq 'Africa'   
;
quit;
*/
proc fedsql sessref=casauto;
select Customer_Name
     , quantity
     , Customer_BirthDate
   from casuser.customer_info
   where customer_group like '%Gold%' 
     and Continent = 'Africa'   
;
quit;

/* 5 */
/*
proc sort data=mysas.customer_info out=customer_info;
   by Continent City;
run;

data work.CityTotals(where=(city ne ' '));
   set customer_info;
   by Continent City;
   if first.City then
      do;
  TotalCost=0;
         numOrders=0;
      end;
   TotalCost+Cost;
   numOrders+1;
   if last.City then
      output;
   keep Continent City TotalCost numOrders;
run;
*/

data casuser.CityTotals;
   set casuser.customer_info;
   by Continent City;
   if first.City then
      do;
  TotalCost=0;
         numOrders=0;
      end;
   TotalCost+Cost;
   numOrders+1;
   if city ne ' ' and last.City then
      output;
   keep Continent City TotalCost numOrders;
run;
