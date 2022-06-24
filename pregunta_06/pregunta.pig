/*
Pregunta
===========================================================================

Para el archivo `data.tsv` Calcule la cantidad de registros por clave de la 
columna 3. En otras palabras, cuántos registros hay que tengan la clave 
`aaa`?

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

letters = FOREACH data GENERATE FLATTEN(KEYSET(lettersmap)) as tripleletters;
grouped_letters = GROUP letters BY tripleletters;
counted_letters = FOREACH grouped_letters GENERATE group, COUNT(letters);

STORE counted_letters INTO 'output' USING PigStorage(',');
