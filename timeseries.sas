/*
	Generates a dataset containing the specified variables.
	Datetimes are rounded to the minute.
	- abt_name : the name of the dataset to be produced. Ex WORK.test
	- min_date : the minimal date to take into account. Included. Must be a datetime. Ex: "01JAN2017 00:00:00"dt
	- max_date : the maximal date to take into account. Excluded. Must be a datetime. Ex: "01JAN2017 00:00:00"dt
	- variables: the variables to extract. Ex: "Y01P205_FI51" "Y01P205_FI52"

*/
%macro extract_abt(abt_name, min_date, max_date, variables);

	proc sql;
		create table &abt_name as
			select f.measure_dttm, f.loc_measure_value, t.tag_column_nm, t.tag_desc
			from wrnamart.pam_asset_loc_measure_fact as f
			INNER
			join wrnamart.pam_tag_dim as t
			on f.pam_tag_dim_rk=t.pam_tag_dim_rk
			where (
				(t.tag_column_nm in ( 
					%do i=1 %to %sysfunc(countw(&variables));
						%scan(&variables, &i) 
					%end;
				)) and
				(f.measure_dttm >= &min_date) and
				(f.measure_dttm < &max_date)
			)
	;
	quit;

	/* Rounds all the dates to the closest minute. */
	data &abt_name;
		set &abt_name;
		measure_dttm = measure_dttm - mod(measure_dttm,60);
	run;

	proc sort data=&abt_name nodupkey;
	by measure_dttm tag_column_nm;
	run;


	proc transpose data=&abt_name out=&abt_name(drop=_name_ _label_);
	id tag_column_nm;
	idlabel tag_desc;
	var loc_measure_value;
	by measure_dttm;
	run;

	/* Rename datetime column */
	data &abt_name;
		set &abt_name (rename=(MEASURE_DTTM=datetime));
	run;

%mend extract_abt;