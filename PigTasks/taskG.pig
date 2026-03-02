-- scp -P 14226 C:/Users/op902/CS585-Project1/taskG.pig ds503@localhost:~/pig

-- cd ~
-- rm -rf ~/shared_folder/project1/taskG/output
-- pig
-- pig -x local taskG.pig    

-- View output
-- cd ~
-- cat ~/shared_folder/project1/taskG/output/part-r-00000

-- ======
-- Load ActivityLog.txt
Activities = LOAD '/home/ds503/ActivityLog.txt'
USING PigStorage(',')
AS (
    logId:int,
    byWho:int,
    whatPage:int,
    actionType:chararray,
    time:int
);

-- Load CircleNetPage.txt
Pages = LOAD '/home/ds503/CircleNetPage.txt'
USING PigStorage(',')
AS (
    id:int,
    userNickname:chararray,
    userJob:chararray,
    userRegionCode:int,
    userHobby:chararray
);

-- Group ActivityLog by user and get most recent action time
GroupedActivities = GROUP Activities BY byWho;

MaxActivityTime = FOREACH GroupedActivities GENERATE
    group AS userId,
    MAX(Activities.time) AS maxTime;

-- LEFT OUTER JOIN to keep users with no activity
Joined = JOIN Pages BY id LEFT OUTER, MaxActivityTime BY userId;

-- Filter inactivty, null for no activity
InactiveUsers = FILTER Joined BY
    MaxActivityTime::maxTime IS NULL
    OR MaxActivityTime::maxTime <= (1000000 - (24 * 90));

-- Final output
FinalResult = FOREACH InactiveUsers GENERATE
    Pages::id,
    Pages::userNickname;

-- Store results
STORE FinalResult
INTO '/home/ds503/shared_folder/project1/taskG/output'
USING PigStorage(',');