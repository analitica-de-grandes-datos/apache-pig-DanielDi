/*
Pregunta
===========================================================================

Para responder la pregunta use el archivo `data.csv`.

Escriba el código equivalente a la siguiente consulta en SQL.

   SELECT
       color
   FROM 
       u 
   WHERE 
       color 
   LIKE 'b%';

Escriba el resultado a la carpeta `output` del directorio actual. Para la 
evaluación, pig sera eejcutado ejecutado en modo local:

$ pig -x local -f pregunta.pig

        >>> Escriba su respuesta a partir de este punto <<<
*/
data = LOAD 'data.csv' USING PigStorage(',') 
    AS (
        id:int, 
        name:chararray, 
        lastName:chararray,
        date,
        color:chararray
        );

filtered_colors = FILTER data BY (color MATCHES '.*^[b].*');
colors = FOREACH filtered_colors GENERATE color;

STORE colors INTO 'output' USING PigStorage(',');
