
%macro partial_reduce(in, out, nrows, col, f, result_col);
	data &out;
		set &in;

		array tmp_lag {&nrows};
		retain tmp_lag1-tmp_lag&nrows;

		do lag_step=&nrows to 2 by -1;
   			tmp_lag{lag_step} = &f(tmp_lag{lag_step-1} , &col);
		end;

		tmp_lag1 = &col;

		drop lag_step tmp_lag1-tmp_lag%eval(&nrows - 1);

		rename tmp_lag&nrows=&result_col;
	run;
%mend partial_reduce;



data empty_df;  
	infile datalines missover;  
    input metric $10. ;  
    datalines;
	pearson
	kendall
	hoeffding
	; 
run; 


/*
	Performs a correlation test between two variables: a target and a candidate.
	If the test is not relevant, it retrieves an empty table.

	param df: The dataframe into which test results must be put.
	param target: The target variable.
	param empty: An empty result dataframe. This is mandatory because SAS does not support datalines (or CARD) into a macro, 
                 which introduces additional side effects.
    param min_date: the minimal date to take into account in the correlation test
	param max_date: the maximal date to take into account in the correlation test.
	param candidate: the other variable to integrate in the correlation test.
*/
%macro correlation_test(df, target, empty, min_date, max_date, candidate);
	
	%extract_abt(corr_data, &min_date, &max_date, "&candidate" "&target");
	
	data corr_data;
		set corr_data;
		if &target = . then &target = .;
		if &candidate = . then &candidate = .;
	run;

	proc sql noprint;
		select count(*) into : nobs 
		from corr_data
		where &target and &candidate ;
		;
	quit;

	%put "no. of observations =" &nobs; 

	%if &nobs > 0 %then %do;
		data corr_data;
			set corr_data;
			drop datetime;
		run;

		proc corr NOMISS NOSIMPLE data=corr_data outp=corr_p outk=corr_k outh=corr_h;
		run;

		data corr_p;
			set corr_p;
			where _type_ = "CORR" and _name_ = "&target";
			metric = "pearson";
			keep metric &candidate;
		run;

		data corr_k;
			set corr_k;
			where _type_ = "CORR" and _name_ = "&target";
			metric = "kendall";
			keep metric &candidate;
		run;

		data corr_h;
			set corr_h;
			where _type_ = "CORR" and _name_ = "&target";
			metric = "hoeffding";
			keep metric &candidate;
		run;

		/* Appends correlation tables */

		proc sql;
			CREATE TABLE &df AS 
			SELECT * FROM corr_p
			 OUTER UNION CORR 
			SELECT * FROM corr_h
			 OUTER UNION CORR 
			SELECT * FROM corr_k

			order by metric
			;
		quit;

		data &df;
			set &df;
			if &candidate = . then &candidate = .;
		run;

		/* Cleans that mess */
		proc datasets library=WORK;
	   	*	delete corr_data;
			delete corr_p;
			delete corr_h;
			delete corr_k;
		run;

	%end;
	%else %do;
		data &df;
			set &empty;
			&candidate = .;
		run;

	%end;

	
%mend correlation_test;	

%correlation_test(df, variable_a, empty_df, "01JAN2018 00:00:00"dt, "01APR2018 00:00:00"dt, variable_b);






%macro multi_correlation_test(df, target, min_date, max_date, candidates);
	proc datasets library=WORK;
   		delete &df;
		delete corr_var;
	run;
	
	%do loop_index=1 %to %sysfunc(countw(&candidates));
		%put START LOOP &loop_index / %sysfunc(countw(&candidates));
		%let candidate = %scan(&candidates, &loop_index);

		%put testing correlation for &candidate;

		%correlation_test(corr_var, &target, empty_df, &min_date, &max_date, &candidate);

		proc sort data=corr_var out=corr_var;
			by metric;
		run;

		%if &loop_index=1 %then %do;
			data &df;
				set corr_var;
			run;
		%end;
		%else %do;
			data &df;
				merge &df corr_var;
				by metric;
			run;
		%end;	
	%end;

	
%mend multi_correlation_test;


%multi_correlation_test(corr_vars, variable_a, "01JAN2018 00:00:00"dt, "01APR2018 00:00:00"dt, variable_b variable_c variable_d );