# Calculate Distance As The Crow Flies Between Two Points on Earth via Haversine formula
from math import radians, cos, sin, asin, sqrt


def distancebetween(lat1, lat2, lon1, lon2):

    lon1 = radians(lon1)
    lon2 = radians(lon2)
    lat1 = radians(lat1)
    lat2 = radians(lat2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2

    c = 2 * asin(sqrt(a))

    # Radius of earth in miles.
    r = 3956

    # calculate the result
    return c * r


# driver code
# KC = 39.1238 -94.5541
# ATL = 33.7628 -84.422
# lat1 = 39.1238
# lat2 = 33.7628
# lon1 = -94.5541
# lon2 = -84.422
# print(distancebetween(lat1, lat2, lon1, lon2), "Miles")
