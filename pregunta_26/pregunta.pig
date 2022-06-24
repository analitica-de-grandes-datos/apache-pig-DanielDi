/*
Pregunta
===========================================================================

Para responder la pregunta use el archivo `data.csv`.

Escriba el código equivalente a la siguiente consulta SQL.

   SELECT 
       firstname 
   FROM 
       u 
   WHERE 
       SUBSTRING(firstname, 0, 1) >= 'm';

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

filtered_data = FILTER data BY SUBSTRING(name, 0, 1) >= 'M';
names = FOREACH filtered_data GENERATE name;

STORE names INTO 'output' USING PigStorage(',');