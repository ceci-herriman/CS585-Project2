// In python terminal: 
// sudo scp -P 14226 [pathto]/TaskA.pig ds503@localhost:~/pig
// sudo scp -P 14226 [pathto]/CircleNetPage.txt ds503@localhost:~/
// In docker: 
// hadoop/bin/hdfs dfs -put CircleNetPage.txt /user/ds503/input
// cd pig
// pig taskA.pig

Pages = LOAD '/home/ds503/CircleNetPage.txt'
USING PigStorage(',')
AS (
    id:int,
    userNickname:chararray,
    userJob:chararray,
    userRegionCode:int,
    userHobby:chararray
);

GroupedPages = GROUP Pages BY userHobby;

PageAccessCounts = FOREACH GroupedPages
    GENERATE
        group,
        COUNT(Pages) AS accessCount;

STORE FinalResult
INTO '/home/ds503/shared_folder/project1/taskA/output'
USING PigStorage(',');;