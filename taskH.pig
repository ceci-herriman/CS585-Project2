-- In python terminal: 
-- sudo scp -P 14226 taskH.pig ds503@localhost:~/pig
-- In docker: 
-- pig 
-- pig -x local taskH.pig

-- A follows B, B doesn't follow A, A in the same region as B
-- use left join to filter out A's where A -> B and B -> A

--Attach regions to remaining tuples and then filter those follows with same region

circleNet = load '/home/ds503/CircleNetPage.txt' using PigStorage(',') as (id:int, nickname:chararray, job:chararray, regionCode:int, hobby:chararray);
A = load '/home/ds503/Follows.txt' using PigStorage(',') as (id:int, followerID:int, followeeID:int, time:int, desc:chararray);
--make a copy of A
B = FOREACH A GENERATE
              followerID AS followerID,
              followeeID AS followeeID;

mutualJoin = JOIN A by (followerID, followeeID) LEFT OUTER, B by (followeeID, followerID);
nonMutuals = FILTER mutualJoin BY B::followerID IS NULL;

nonMutualsWithARegion = JOIN nonMutuals by A::followerID, circleNet by id;
nonMutualsWithRegions = JOIN nonMutualsWithARegion by A::followeeID, circleNet by id;
nonMutualsSameRegion = FILTER nonMutualsWithRegions BY nonMutualsWithARegion::circleNet::regionCode == circleNet::regionCode;

result = FOREACH nonMutualsSameRegion generate nonMutualsWithARegion::circleNet::id, nonMutualsWithARegion::circleNet::nickname;
distinctResult = distinct result;

DUMP distinctResult;

STORE distinctResult INTO '/home/ds503/shared_folder/project2/part1/taskH/output' USING PigStorage(',');
