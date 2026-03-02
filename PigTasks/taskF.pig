-- Copy files to hadoop
-- scp -P 14226 C:/Users/op902/CS585-Project1/Follows.txt ds503@localhost:~/
-- scp -P 14226 C:/Users/op902/CS585-Project1/CircleNetPage.txt ds503@localhost:~/

-- scp -P 14226 C:/Users/op902/CS585-Project1/taskF.pig ds503@localhost:~/pig

-- Remove output folder, run .pig file
-- cd ~
-- rm -rf ~/shared_folder/project1/taskF/output
-- pig
-- pig -x local taskF.pig    

-- View output
-- cat ~/shared_folder/project1/taskF/output/part-r-00000

-- ======

-- Load Follows.txt
Follows = LOAD '/home/ds503/Follows.txt'
USING PigStorage(',')
AS (
    colRel:int,
    id1:int,
    id2:int,
    date:int,
    desc:chararray
);

-- Count followers per id2 page owner
OwnerGroups = GROUP Follows BY id2;

FollowerCounts = FOREACH OwnerGroups
GENERATE
    group AS ownerId,
    COUNT(Follows) AS followerCount;

-- Compute average follower count for all pages
AllOwners = GROUP FollowerCounts ALL;

Average = FOREACH AllOwners
GENERATE
    AVG(FollowerCounts.followerCount) AS avgFollowers;

-- Attach average to each owner
WithAverage = CROSS FollowerCounts, Average;

-- Keep only owners above average
AboveAverage = FILTER WithAverage BY followerCount > avgFollowers;

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

-- Join owner info
Joined = JOIN AboveAverage BY ownerId, Pages BY id;

FinalResult = FOREACH Joined
GENERATE
    AboveAverage::FollowerCounts::ownerId AS ownerId,
    Pages::userNickname AS userNickname,
    Pages::userJob AS userJob,
    AboveAverage::FollowerCounts::followerCount AS followerCount;

-- Store results
STORE FinalResult
INTO '/home/ds503/shared_folder/project1/taskF/output'
USING PigStorage(',');
