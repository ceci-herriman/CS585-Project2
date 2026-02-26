

// In python terminal: 
// sudo scp -P 14226 [pathto]/TaskA.pig ds503@localhost:~/pig
// sudo scp -P 14226 [pathto]/CircleNetPage.txt ds503@localhost:~/
// In docker: 
// hadoop/bin/hdfs dfs -put CircleNetPage.txt /user/ds503/input
// cd pig
// pig taskA.pig

A = load 'input/CircleNetPage.txt' using PigStorage('\t') as (id:int, nickname:chararray, job:chararray, regionCode:int, hobby:chararray);

B = foreach A generate id, hobby;

C = group B by hobby;

D = foreach C generate group as hobby, COUNT(B) as count;

STORE D into 'output/TaskA_output' using PigStorage('\t');




