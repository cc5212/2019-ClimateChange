raw_table = LOAD 'hdfs://cm:9000/uhadoop2019/fplana/GlobalLandTemperaturesByCity.csv.gz' USING PigStorage(',') AS (date:charArray,avgTemp:long,avgTempUnc:long,City:charArray,Country:charArray,Lat:charArray,Longit:charArray);


-- Quitar los nulls 

full_table = FILTER raw_table by ($0 is not null and SIZE($0)  == 10 and  $1 is not null and $2 is not null and $3 is not null and $4 is not null and $5 is not null and $6 is not null);

--Normalizar las fechas
toDate_data = foreach full_table generate ToDate(date,'yyyy-MM-dd', '+00:00') as (date:DateTime ) ,$1,$2,$3,$4,$5,$6;
data_only_years = foreach toDate_data generate GetYear($0) as year ,$1,$2,$3,$4,$5,$6;



--Convertir los datos

boolean_table= FOREACH data_only_years GENERATE $0, $1, $2, $3, $4, $5 as Lat, ENDSWITH ($5, 'N') as Cord;


clean_table= FOREACH boolean_table GENERATE $0, $1, $2, $3, $4, (Cord? (Double) SUBSTRING (Lat, 0, (int)SIZE(Lat)-1): ((Double) SUBSTRING (Lat, 0, (int) SIZE (Lat)-1) *-1.00)) as Lat;


--obtener  años en el deta
clean_table_y = FILTER  clean_table BY year%5 ==0; 

---------------------------------------------------------
--             CALCULAR PROMEDIO POR ZONA
---------------------------------------------------------
--Separar por zona de acuerdo a coordenadas geográfica:

zona_polar_norte= FILTER clean_table_y BY (Lat < 90.00 and Lat > 66.01);

zona_templada_norte= FILTER clean_table_y BY (Lat < 66.00 and Lat > 23.01);

zona_tropical= FILTER clean_table_y BY (Lat < 23.00 and Lat > -23.01);

zona_templada_sur= FILTER clean_table_y BY (Lat < -23.00 and Lat > -66.01);

zona_polar_sur = FILTER clean_table_y BY (Lat < -66.00 and Lat > -90.00);

--agruparlos por año
groupYears_zpn= GROUP zona_polar_norte  BY  year;
groupYears_ztn= GROUP zona_templada_norte  BY  year;
groupYears_ztr= GROUP zona_tropical  BY  year;
groupYears_zts= GROUP zona_templada_sur  BY  year;
groupYears_zps= GROUP zona_polar_sur  BY  year;
--calcular el promedio

promedio_zpn= FOREACH groupYears_zpn GENERATE flatten(zona_polar_norte.year),  AVG(zona_polar_norte.avgTemp), MIN(zona_polar_norte.avgTemp), MAX(zona_polar_norte.avgTemp);
promedio_ztn= FOREACH groupYears_ztn GENERATE flatten(zona_templada_norte.year),  AVG(zona_templada_norte.avgTemp), MIN(zona_templada_norte.avgTemp), MAX(zona_templada_norte.avgTemp);
promedio_ztr= FOREACH groupYears_ztr GENERATE flatten(zona_tropical.year),  AVG(zona_tropical.avgTemp), MIN(zona_tropical.avgTemp), MAX(zona_tropical.avgTemp);
promedio_zts= FOREACH groupYears_zts GENERATE flatten(zona_templada_sur.year),  AVG(zona_templada_sur.avgTemp), MIN(zona_templada_sur.avgTemp), MAX(zona_templada_sur.avgTemp);
promedio_zps= FOREACH groupYears_zps GENERATE flatten(zona_polar_sur.year),  AVG(zona_polar_sur.avgTemp), MIN(zona_polar_sur.avgTemp), MAX(zona_polar_sur.avgTemp);
-- borrar repetidos
unique_promedio_zpn= DISTINCT promedio_zpn;
unique_promedio_ztn= DISTINCT promedio_ztn;
unique_promedio_ztr= DISTINCT promedio_ztr;
unique_promedio_zts= DISTINCT promedio_zts;
unique_promedio_zps= DISTINCT promedio_zps;

final_zpn= unique_promedio_zpn;
final_ztn= unique_promedio_ztn;
final_ztr= unique_promedio_ztr;
final_zts= unique_promedio_zts;
final_zps= unique_promedio_zps;

delta_zpn= foreach final_zpn GENERATE $0, ABS($3-$2);
delta_ztn= foreach final_ztn GENERATE $0, ABS($3-$2);
delta_ztr= foreach final_ztr GENERATE $0, ABS($3-$2);
delta_zts= foreach final_zts GENERATE $0, ABS($3-$2);
delta_zps= foreach final_zps GENERATE $0, ABS($3-$2);


---------------------------------------------------------------
--                    CHILE
---------------------------------------------------------------
--dejamos solo chile en la tabla
chile= Filter clean_table_y BY Country == 'Chile';

norte_grande= FILTER chile BY (Lat < -17.80 and Lat > -27.99);

norte_chico= FILTER chile BY (Lat < -28.00 and Lat > -31.99);

zona_central= FILTER chile BY (Lat < -32.00 and Lat > -36.99);

zona_sur= FILTER chile BY (Lat < -37.00 and Lat > -43.49);

zona_austral = FILTER chile BY (Lat < -43.50 and Lat > -56.00);

--agruparlos por año
groupYears_ng= GROUP norte_grande  BY  year;
groupYears_nc= GROUP norte_chico  BY  year;
groupYears_zc= GROUP zona_central  BY  year;
groupYears_zs= GROUP zona_sur  BY  year;
groupYears_za= GROUP zona_austral  BY  year;

promedio_ng= FOREACH groupYears_ng GENERATE flatten(norte_grande.year),  AVG(norte_grande.avgTemp), MIN(norte_grande.avgTemp), MAX(norte_grande.avgTemp);
promedio_nc= FOREACH groupYears_nc GENERATE flatten(norte_chico.year),  AVG(norte_chico.avgTemp), MIN(norte_chico.avgTemp), MAX(norte_chico.avgTemp);
promedio_zc= FOREACH groupYears_zc GENERATE flatten(zona_central.year),  AVG(zona_central.avgTemp), MIN(zona_central.avgTemp), MAX(zona_central.avgTemp);
promedio_zs= FOREACH groupYears_zs GENERATE flatten(zona_sur.year),  AVG(zona_sur.avgTemp), MIN(zona_sur.avgTemp), MAX(zona_sur.avgTemp);
promedio_za= FOREACH groupYears_za GENERATE flatten(zona_austral.year),  AVG(zona_austral.avgTemp), MIN(zona_austral.avgTemp), MAX(zona_austral.avgTemp);

-- borrar repetidos
unique_promedio_ng= DISTINCT promedio_ng;
unique_promedio_nc= DISTINCT promedio_nc;
unique_promedio_zc= DISTINCT promedio_zc;
unique_promedio_zs= DISTINCT promedio_zs;
unique_promedio_za= DISTINCT promedio_za;

--Calcular delta
delta_ng= foreach unique_promedio_ng GENERATE $0, ABS($3-$2);
delta_nc= foreach unique_promedio_nc GENERATE $0, ABS($3-$2);
delta_zc= foreach unique_promedio_zc GENERATE $0, ABS($3-$2);
delta_zs= foreach unique_promedio_zs GENERATE $0, ABS($3-$2);
delta_za= foreach unique_promedio_za GENERATE $0, ABS($3-$2);


--Guardar
STORE final_zpn INTO '/uhadoop2019/grupock/proyecto5/zpn5';
STORE final_ztn INTO '/uhadoop2019/grupock/proyecto5/ztn5';
STORE final_ztr INTO '/uhadoop2019/grupock/proyecto5/ztr5';
STORE final_zts INTO '/uhadoop2019/grupock/proyecto5/zts5';
STORE final_zps INTO '/uhadoop2019/grupock/proyecto5/zps5';

STORE delta_zpn INTO '/uhadoop2019/grupock/proyecto5/deltazpn5';
STORE delta_ztn INTO '/uhadoop2019/grupock/proyecto5/deltaztn5';
STORE delta_ztr INTO '/uhadoop2019/grupock/proyecto5/deltaztr5';
STORE delta_zts INTO '/uhadoop2019/grupock/proyecto5/deltazts5';
STORE delta_zps INTO '/uhadoop2019/grupock/proyecto5/deltazps5';

STORE unique_promedio_ng INTO '/uhadoop2019/grupock5/proyecto/ng5';
STORE unique_promedio_nc INTO '/uhadoop2019/grupock5/proyecto/nc5';
STORE unique_promedio_zc INTO '/uhadoop2019/grupock5/proyecto/zc5';
STORE unique_promedio_zs INTO '/uhadoop2019/grupock5/proyecto/zs5';
STORE unique_promedio_za INTO '/uhadoop2019/grupock5/proyecto/za5';

STORE delta_ng INTO '/uhadoop2019/grupock/proyecto5/deltang5';
STORE delta_nc INTO '/uhadoop2019/grupock/proyecto5/deltanc5';
STORE delta_zc INTO '/uhadoop2019/grupock/proyecto5/deltazc5';
STORE delta_zs INTO '/uhadoop2019/grupock/proyecto5/deltazs5';
STORE delta_za INTO '/uhadoop2019/grupock/proyecto5/deltaza5';







