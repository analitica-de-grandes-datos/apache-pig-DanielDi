/*
Pregunta
===========================================================================

Para responder la pregunta use el archivo `data.csv`.

Cuente la cantidad de personas nacidas por año.

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
        date:chararray,
        color:chararray
        );

B = FOREACH data GENERATE ToDate(date, 'yyyy-MM-dd') AS date2;
C = GROUP B BY ToString(date2, 'yyyy');
D = FOREACH C GENERATE group, COUNT(B);

STORE D INTO 'output' USING PigStorage(',');