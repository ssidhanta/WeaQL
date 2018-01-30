__author__ = 'dnlopes'


baseUUID='1b7fb86c-831b-11e5-8ae5-0022192ff24'
generatedUUIDCounter=0

def generateNextUUID():
    global generatedUUIDCounter
    generatedUUIDCounter +=1
    nextUUID = baseUUID + '{}'.format(generatedUUIDCounter)
    return nextUUID


