DROP TABLE IF EXISTS csv_to_json_test;

CREATE TABLE
    csv_to_json_test (entry Int32, csv String) ENGINE = Memory;

INSERT INTO
    csv_to_json_test
VALUES
    (1, 'Key1,42,John'),
    (2, 'Key2,true,Bob'),
    (3, 'Key3,false,Alice'),
    (4, 'Key4,something,Charlie'),
    (5, 'Key5|17|Rudolf'),
    (6, '"The,key,with,quotes","The value with,quotes",",;''"');

SELECT csvToJSONString ('id,property,name', csv) AS json FROM csv_to_json_test WHERE entry < 5 ORDER BY entry;
SELECT csvToJSONString ('id|property|name', csv, 'delimiter="|"') AS json FROM csv_to_json_test WHERE entry == 5;
SELECT csvToJSONString ('id|property|name', csv, 'delimiter="|",autodetect_types="false"') AS json FROM csv_to_json_test WHERE entry == 5;
SELECT csvToJSONString (',property,name', csv) AS json FROM csv_to_json_test WHERE entry < 3 ORDER BY entry;
SELECT csvToJSONString ('id,,name', csv) AS json FROM csv_to_json_test WHERE entry < 3 ORDER BY entry;
SELECT csvToJSONString ('id,property', csv) AS json FROM csv_to_json_test WHERE entry < 3 ORDER BY entry;
SELECT csvToJSONString ('id,property,name', csv) AS json FROM csv_to_json_test WHERE entry == 6;

DROP TABLE csv_to_json_test;