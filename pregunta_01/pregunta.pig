/* 
Pregunta
===========================================================================

Para el archivo `data.tsv` compute la cantidad de registros por letra.
Almacene los resultados separados por comas. 

Escriba el resultado a la carpeta `output` del directorio actual. Para la 
evaluación, pig sera eejcutado ejecutado en modo local:

$ pig -x local -f pregunta.pig

        >>> Escriba su respuesta a partir de este punto <<<
*/

data = LOAD 'data.tsv' USING PigStorage('\t')
    AS (
            letter:chararray,
            date:chararray,
            num:int
    );

groups = GROUP data BY letter;
count = FOREACH groups GENERATE group, COUNT(data);
STORE count INTO 'output' USING PigStorage(',');