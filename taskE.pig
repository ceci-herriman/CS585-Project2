


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

-- Count followers per id1 follower
FollowerGroups = GROUP Follows BY id1;

FollowsCounts = FOREACH FollowerGroups
GENERATE
    group AS ownerId,
    COUNT(Follows) AS followsCount;

UniqueFollowers = GROUP FollowerGroups BY id2;

UniqueFollowsCounts = ForEach FollowerGroups
GENERATE 
    group AS ownerId,
    COUNT(UniqueFollowers) AS Unique;

Result = JOIN FollowerGroups by (ownerID), UniqueFollowersCounts by (ownerID)

STORE Result INTO '/home/ds503/shared_folder/project2/part1/taskE/output' USING PigStorage(',');
