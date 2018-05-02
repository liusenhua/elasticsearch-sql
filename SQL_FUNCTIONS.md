ES Plugin SQL functions
=======================

The plugin support below SQL functions, basically, the syntax follows the oracle's sql function style.

### The math functions
    1. These functions has the same syntax as Java Number & Math class:
        "exp", "sqrt", "cbrt", "ceil", "floor", "rint", "abs" 

    2. The log function
        - double log(double d) and double ln(double d)
            Returns the natural logarithm (base e) of a double value.
            
        - double log(double base, double d)
            Returns the natural logarithm (base: the first argument) of a double value.
            
        - double log10(double d)
            Returns the base 10 logarithm of a double value.

    3. The round function
        - Long round(double o)
            Returns the closest long to the argument
            
        - double round(double o, int decimals)
            Returns n rounded to integer places to the right of the decimal point.
    
    4. double pow(double base, double exponent)
        Returns the value of the first argument raised to the power of the second argument
     
### The string functions

    1. String split(String field, String pattern, int index)
        Return the nth(`The index` argument) component of splits by using the `pattern` to split the field
        
    2. String concat(String str1, String str2, ..., String strN)
        Concat the `str1`, `str2`, ... `strN` string.
        eg. concat('hello', ' ',  'world', '!') return "hello world!"
        
    3. String concat_ws(String seprator, String str1, String str2, ..., String strN)
        Using the string `seprator` as seprator to concat the `str1`, `str2`, ... `strN` string.
        eg. concat_ws(":", 'hello', 'world', '!') return "hello:world:!"
    
    4. The substr / substring
        - substr(str, pos), substring(str, pos)
            return a substring from string str starting at position pos
        - substr(str, pos, len), substring(str, pos, len)
            return a substring len characters long from string str, starting at position pos
            
        NOTE: 
            1. The position of the **first** character in the string from which the substring is to be extracted is reckoned as **1**.
            2. The pos can be a negative value. In this case, the beginning of the substring is pos characters from the end of the string, rather than the beginning.
            
        eg.
            select substring('Quadratically',5) ->  'ratically'
            select substring('Quadratically',5, 6) ->  'ratica'
            select substring('Sakila',-3) ->  'ila'
            select substring('Sakila', -5, 3) ->  'aki'
            
    5. String trim(str)
        Return the str with prefixes or suffixes spaces removed
    
    6. The instr
        Return the index of the first occurrence of substring. It has below varations:
        
        - Integer instr(String src, String target)
            Return the index of the first occurrence of `target` substring.
            
        - Integer instr(String src, String target, Integer from_index)
            Return the index of the first occurrence of `target` substring from the position `from_index`
            
        - Integer instr(String src, String target, Integer from_index, Integer nth_appearance)
            Return the index of the nth occurrence `nth_appearance` of `target` substring from the position `from_index`
            
    7. String replace(String src, String search_str, String replace_str)
        Return a new string replaced occurrences of a specified string
    
### The date functions
    1. Long year(Date o)
        Returns the year for date, in the range 1000 to 9999
        
    2. Long month(Date o)
        Returns the month for date, in the range 1 to 12 for January to December
        
    3. Long day(Object o)
        Returns the day of the month for date, in the range 1 to 31.
        
    4. Integer quarter(Date o)
        Returns the quarter of the year for date, in the range 1 to 4
        
    5. Long now()
        Returns the current time as a value in UTC timestamp.
        The value is expressed in the current time zone.
        
    6. Long today()
        Returns the the begin of the current day in UTC timestamp.
        
    7. Long date_add(String unit, int interval, Date o)
        Add a `interval` time unit specified by `unit` to the date `o`.
        The unit support(createTime="2016-03-17 13:27:33.953"):
        -   year
            eg. to_char(date_add('year', -1, createTime)) -> 2017-03-17 13:27:33.953
        -   month
            eg. to_char(date_add('month', 1, createTime)) -> 2016-04-17 13:27:33.953
        -   quarter
            eg. to_char(date_add('quarter', -1, createTime)) -> 2015-12-17 13:27:33.953
        -   day
            eg. to_char(date_add('day', -1, createTime)) -> 2016-03-16 13:27:33.953
        -   week
            eg. to_char(date_add('week', 1, createTime)) -> 2016-03-24 13:27:33.953
        
    8. Long date_diff(String unit, Date o_1, Date o_2)
        Calculate the difference between two dates, return the value with specified `unit`.
        The unit support(createTime="2016-03-17 13:27:33.953"):
            -   year
                eg. date_diff('year', to_date('2015-03-17 13:27:33.953'), createTime) -> 1
            -   month
                eg. date_diff('monnth', to_date('2015-03-17 13:27:33.953'), createTime) -> 12
            -   quarter
                eg. date_diff('quarter', to_date('2015-03-17 13:27:33.953'), createTime) -> 4
            -   day
                eg. date_diff('day', to_date('2015-03-17 13:27:33.953'), createTime) -> 366
            -   week
                eg. date_diff('week', to_date('2015-03-17 13:27:33.953'), createTime) -> 52
            
    9. Long date_trunc(String unit, Date o)
        Truncate the date time. The unit support (createTime="2016-03-17 13:27:33.953"):
            -   year
                eg. to_char(date_trunc('year',  createTime)) -> 2016-01-01 00:00:00.000
            -   month
                eg. to_char(date_trunc('month',  createTime)) -> 2016-03-01 00:00:00.000
            -   day
                eg. to_char(date_trunc('day',  createTime)) -> 2016-03-17 00:00:00.000
            -   hour
                eg. to_char(date_trunc('hour',  createTime)) -> 2016-03-17 13:00:00.000
            -   minute
                eg. to_char(date_trunc('minute',  createTime)) -> 2016-03-17 13:27:00.000
            -   second
                eg. to_char(date_trunc('second',  createTime)) -> 2016-03-17 13:27:33.000
            
    9. Long date_part(String unit, Date o)
        Get the component of date. The unit support:
           -   year
            eg. date_part('year', '2016-03-17 13:27:33.953' ) -> 2016
           -   month
            eg. date_part('month', '2016-03-17 13:27:33.953' ) -> 3
           -   day
            eg. date_part('day', '2016-03-17 13:27:33.953' ) -> 27
           -   hour
            eg. date_part('hour', '2016-03-17 13:27:33.953' ) -> 13
           -   minute
            eg. date_part('minute', '2016-03-17 13:27:33.953' ) -> 27
           -   second
            eg. date_part('second', '2016-03-17 13:27:33.953' ) -> 33
           -   millisecond
            eg. date_part('millisecond', '2016-03-17 13:27:33.953' ) -> 953
                       
    
### The convert functions
    1. String to_char(Date o, String format)
        Return the formatted string from a date.
        eg. to_char(createTime, 'yyyy-MM-dd')
            
    2. Long to_date(String o, String format)
        Parse a date(Respent as a long integer in ES) from a string.
        eg. to_date('2015_03_17', 'yyyy_MM_dd'), to_date('2018/01/01', 'yyyy/MM/dd')
        
    3. Double to_number(String o)
        Parse a numetric from a tring.
        eg. to_number('255')
    
### The aggregation functions
    1. date_range(alias='createTime', field='createTime', format = 'yyyy/MM/dd||yy-MM-dd', '2014/05/1','2016-05-1','now-1y','now', 'now+1y')
        Define a set of ranges for date values and bucket them.
        The parameters:
            -   alias
                The alias name to represent the returned aggegration
            -   field
                which field to compute aggegration
            -   format
                The date format to parse the date value. eg. 'yyyy/MM/dd||yy-MM-dd'
            -   date1, date2, ...
                The date value string.
   
        eg. SELECT count(age), min(age), max(age), avg(age) FROM elasticsearch-sql_test_index/account_with_null GROUP BY date_range(alias='createTime', field='createTime', format = 'yyyy/MM/dd||yy-MM-dd', '2014/05/1','2016-05-1','now-1y','now', 'now+1y')
        ->
        ```
            createTime	            MAX(age)	COUNT(age)	MIN(age)	AVG(age)	
            2014/05/01-2016/05/01,  36.0,       5.0,        28.0,       32.8
            2016/05/01-2017/04/20,  39.0,       1.0,        39.0,       39.0
            2017/04/20-2018/04/20,  34.0,       1.0,        34.0,       34.0
            2018/04/20-2019/04/20,  -Infinity,  0.0,I       nfinity,    NaN
        ```
    2. 