/*
Pregunta
===========================================================================

Para el archivo `data.tsv` compute la cantidad de registros por letra de la 
columna 2 y clave de al columna 3; esto es, por ejemplo, la cantidad de 
registros en tienen la letra `b` en la columna 2 y la clave `jjj` en la 
columna 3 es:

  ((b,jjj), 216)

Escriba el resultado a la carpeta `output` del directorio actual. Para la 
evaluación, pig sera eejcutado ejecutado en modo local:

$ pig -x local -f pregunta.pig

        >>> Escriba su respuesta a partir de este punto <<<
*/

data = LOAD 'data.tsv' 
    AS (
        letter:chararray, 
        lettersbag:BAG{tup:TUPLE(chararray)},
        lettersmap:MAP[]
       );

tuples_count = FOREACH data  GENERATE FLATTEN(lettersbag) AS lb, FLATTEN(lettersmap) AS lm;
grouped_letters = GROUP tuples_count BY (lb, lm);
counted_letters = FOREACH grouped_letters GENERATE group, COUNT(tuples_count);

STORE counted_letters INTO 'output' USING PigStorage(',');