-- In python terminal: 
-- sudo scp -P 14226 taskC.pig ds503@localhost:~/pig
-- sudo scp -P 14226 CircleNetPage.txt ds503@localhost:~/
-- In docker: 
-- pig 
-- pig -x local taskC.pig


A = load '/home/ds503/CircleNetPage.txt' using PigStorage(',') as (id:int, nickname:chararray, job:chararray, regionCode:int, hobby:chararray);

B = FILTER A by hobby == 'Hockey';

STORE B INTO '/home/ds503/shared_folder/project2/part1/taskC/output' USING PigStorage(',');
