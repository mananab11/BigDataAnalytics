import numpy as np

from random import randrange
from datetime import timedelta, datetime

def random_date(start, end):
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)


lines = 1000000
static_sigma = 20
d1 = datetime.strptime('1/1/2016 1:30 PM', '%m/%d/%Y %I:%M %p')
d2 = datetime.strptime('2/1/2020 4:50 AM', '%m/%d/%Y %I:%M %p')

mus = dict()
for l in range(lines):
    d = random_date(d1, d2)
    uid = int(np.random.uniform(0,np.sqrt(lines)+400)) + 10000
    try:
        mu = mus[uid]
    except KeyError:
        mu = np.random.normal(30, 10)
        mus[uid] = mu
    amnt = int(np.random.normal(mu, static_sigma)*100)/100.0
    print(','.join([str(a) for a in [l, d, uid, amnt]]))
    
