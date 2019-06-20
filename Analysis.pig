-- .jar contiene las UDF

REGISTER '/data/2019/uhadoop/fplana/cc5212_proy.jar'

raw_table = LOAD 'hdfs://cm:9000/uhadoop2019/fplana/GlobalLandTemperaturesByCity.csv.gz' USING PigStorage(',') AS (date:charArray,avgTemp:double,avgTempUnc:double,City:charArray,Country:charArray,Lat:charArray,Longit:charArray);

-- filtramos primera fila con los headers

table = FILTER raw_table by NOT (date matches 'dt');
cities_data = GROUP table BY City;

-- Proyectamos solo las fechas en la segunda columna

dates_each_city = FOREACH cities_data GENERATE $0, $1.$0;

-- Para obtener conjuntos maximales de fechas adyacentes 

DEFINE search_avail_dates mis_udfs.search_avail_dates();
max_av_dates = FOREACH dates_each_city GENERATE $0, search_avail_dates($1);

-- Para obtener fecha (DUMP date) desde la cual están todos los registros para todas las ciudades

DEFINE search_f_date mis_udfs.search_first_date();
first_dates = FOREACH dates_each_city GENERATE $0, search_f_date($1);
only_dates = FOREACH first_dates GENERATE $1 AS fdate;
dates = GROUP only_dates ALL;
date = FOREACH dates GENERATE MAX($1);

-- Para contar la cantidad total de registros de la tabla

group_all = GROUP table ALL;  
total = FOREACH group_all GENERATE COUNT_STAR(table.date);  
DUMP total;


