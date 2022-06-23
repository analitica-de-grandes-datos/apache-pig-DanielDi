/*
Pregunta
===========================================================================

Para el archivo `data.tsv` genere una tabla que contenga la primera columna,
la cantidad de elementos en la columna 2 y la cantidad de elementos en la 
columna 3 separados por coma. La tabla debe estar ordenada por las 
columnas 1, 2 y 3.

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

tuples_count = FOREACH data  GENERATE letter, COUNT(lettersbag)AS counttups, SIZE(lettersmap) AS countmap;
ordered = ORDER tuples_count BY letter ASC, counttups ASC, countmap ASC;
STORE ordered INTO 'output' USING PigStorage(',');