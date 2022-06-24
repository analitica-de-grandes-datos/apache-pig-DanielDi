/*
Pregunta
===========================================================================

Para el archivo `data.tsv` compute Calcule la cantidad de registros en que 
aparece cada letra minúscula en la columna 2.

Escriba el resultado a la carpeta `output` del directorio actual. Para la 
evaluación, pig sera eejcutado ejecutado en modo local:

$ pig -x local -f pregunta.pig

        >>> Escriba su respuesta a partir de este punto <<<
*/
data = LOAD 'data.tsv' 
    AS (
        letter:chararray, 
        lettersbag:BAG{tup:TUPLE(chararray)}
       );

bags = FOREACH data GENERATE FLATTEN(lettersbag) as letters;
grouped_letters = GROUP bags BY letters;
counted_letters = FOREACH grouped_letters GENERATE group, COUNT(bags);

STORE counted_letters INTO 'output' USING PigStorage(',');