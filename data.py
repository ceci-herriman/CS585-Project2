
import random 
import string

# helper function
def generate_random_string(length):
    random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    return random_string

numOfUserEntries = 100000 #100
numOfFollowEntries = 40000#40
numOfActivities = 20000000 #100

# generate data for CircleNetPage

userNicknames = ["CeciHerriman1", "CharlesH123", "JimmyJim99", "LaurieMango99", "KyraBrown30", "MountLandyn", "PatriceBergy", "BradMarch63", "IsabelSmailey", "CliffordRed", "GabyNags11", "100Charlie249er", "Spidey1010x"]
userJobs = ["Software Engineer", "Data Scientist", "Senior Manager", "Director of Software", "Test Engineer"]
userHobbies = ["Sewing", "Hockey", "Reading", "Cooking", "Painting", "Skydiving", "Soccer", "Lifting", "Working out"]

with open("CircleNetPage.txt", "w") as f:
    for i in range (1, numOfUserEntries + 1):
        id = i

        ran = random.choice([0, 1])
        if(ran):
            userNickname = random.choice(userNicknames)
            userJob = random.choice(userJobs)
            userHobby = random.choice(userHobbies)
        else:
            userNickname = generate_random_string(15)
            userJob = generate_random_string(15)
            userHobby = generate_random_string(15)

        userRegionCode = random.randint(1, 50)

        f.write(f'{id},{userNickname},{userJob},{userRegionCode},{userHobby}\n')


# generate data for Follows 

followDescs = ["Best friends in college", "Best friends in high school", "Close family friend", "Professional celebrity interest", "We met during our sophomore year study group", "Close friend from college engineering classes", "Roommate from my university years", "My cousin who I grew up with"]

with open("Follows.txt", "w") as f:
    for i in range (1, numOfFollowEntries + 1):
        colRel = i
        id1 = random.randint(1, numOfUserEntries)
        id2 = random.randint(1, numOfUserEntries)

        while id2 == id1:
            id2 = random.randint(1, numOfUserEntries)

        date = random.randint(1, 1000001)

        ran = random.choice([0, 1])
        if(ran):
            desc = random.choice(followDescs)
        else: 
            desc = generate_random_string(40)

        f.write(f'{colRel},{id1},{id2},{date},{desc}\n')

# generate data for AcivityLog

actionTypes = ["Just viewing the page", "Viewed page - left a note", "Viewed page - followed user", "Viewed page - liked and commented on post"]

with open("ActivityLog.txt", "w") as f:
    for i in range (1, numOfActivities + 1):
        id = i
        byWho = random.randint(1, numOfUserEntries)
        whatPage = random.randint(1, numOfUserEntries)

        while byWho == whatPage: 
            whatPage = random.randint(1, numOfUserEntries)


        ran = random.choice([0, 1])
        if(ran):
            actionType = random.choice(actionTypes)
        else: 
            actionType = generate_random_string(40)


        time = date = random.randint(1, 1000001)

        f.write(f'{id},{byWho},{whatPage},{actionType},{time}\n')
