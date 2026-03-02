-- scp -P 14226 C:/Users/op902/CS585-Project1/taskB.pig ds503@localhost:~/pig

-- cd ~
-- rm -rf ~/shared_folder/project1/taskB/output
-- rm -rf ~/shared_folder/project1/taskB/temp
-- pig
-- pig -x local taskB.pig    

-- View output
-- cd ~
-- cat ~/shared_folder/project1/taskB/output/part-r-00000

-- ======
-- Load ActivityLog.txt 
Activities = LOAD '/home/ds503/ActivityLog.txt'
USING PigStorage(',')
AS (
    byWho:int,
    whatPage:int,
    actionType:chararray,
    time:int
);

-- Load CircleNetPage
Pages = LOAD '/home/ds503/CircleNetPage.txt'
USING PigStorage(',')
AS (
    id:int,
    userNickname:chararray,
    userJob:chararray,
    userRegionCode:int,
    userHobby:chararray
);

-- Group by page
GroupedPages = GROUP Activities BY whatPage;

-- Count accesses
PageAccessCounts = FOREACH GroupedPages
    GENERATE
        group,
        COUNT(Activities) AS accessCount;

Joined = JOIN PageAccessCounts BY $0,
         Pages BY id;

-- Order by access count
Ordered = ORDER Joined BY accessCount DESC;
Top10 = LIMIT Ordered 10;

FinalResult = FOREACH Top10
    GENERATE
        id AS Id,
        userNickname AS NickName,
        userJob AS JobTitle;

-- Store results
STORE FinalResult
INTO '/home/ds503/shared_folder/project1/taskB/output'
USING PigStorage(',');;