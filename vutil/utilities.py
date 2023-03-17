import hashlib


def createHash(myString):
    return int(hashlib.sha256(myString.encode('utf-8')).hexdigest(), 16) % 10**8
