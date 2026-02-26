
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
Joined = JOIN FollowerCounts BY ownerId, Pages BY id;


-- Store results
STORE Joined
INTO '/home/ds503/shared_folder/project1/taskD/output'
USING PigStorage(',');