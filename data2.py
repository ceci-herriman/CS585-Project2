
import random 
import sys

numOfPoints = 6000

# generate data for the K initial seed points
if(len(sys.argv) != 2):
    print("incorrect number of arguments")
    exit(1)


# generate data for the points --> comma delimited with the format w,x,y,z

with open("data.txt", "w") as f:
    for i in range (1, numOfPoints + 1):
        w = random.randint(0,10000)
        x = random.randint(20000,1000000)
        y = random.randint(0,20000)
        z = random.randint(0,30000)

        f.write(f'{w},{x},{y},{z}\n')

k = int(sys.argv[1])

with open("kseeds.txt", "w") as f:
    for i in range (1, k + 1):
        w = random.randint(0,10000)
        x = random.randint(20000,1000000)
        y = random.randint(0,20000)
        z = random.randint(0,30000)

        f.write(f'{w},{x},{y},{z}\n')