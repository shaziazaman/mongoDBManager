import pymongo
from datetime import datetime

def getMonthDigit(monthStr):
    month = 1
    if monthStr.lower().startswith("jan"):
        month = 1
    elif monthStr.lower().startswith("feb"):
        month = 2
    elif monthStr.lower().startswith("mar"):
        month = 3
    elif monthStr.lower().startswith("apr"):
        month = 4
    elif monthStr.lower().startswith("may"):
        month = 5
    elif monthStr.lower().startswith("jun"):
        month = 6
    elif monthStr.lower().startswith("jul"):
        month = 7
    elif monthStr.lower().startswith("aug"):
        month = 8
    elif monthStr.lower().startswith("sep"):
        month = 9
    elif monthStr.lower().startswith("oct"):
        month = 10
    elif monthStr.lower().startswith("nov"):
        month = 11
    elif monthStr.lower().startswith("dec"):
        month = 12
    return month

def formatISODate(dateToFormat):
    tmpDateStr = dateToFormat.replace(",","") 
    dateParts = tmpDateStr.split(' ');
    monthDigit= getMonthDigit(dateParts[0])
    day = (int)(dateParts[1])
    year = (int)(dateParts[2])
    isoDate = datetime(year, monthDigit, day, 0, 0, 0)
    return isoDate

def findTravelType(content):
    if ( (content.lower().find('leisure') >= 0)
    or (content.lower().find('family') >= 0) 
    or (content.lower().find('honeymoon') >= 0)):
        return 'Leisure'
    elif ( (content.lower().find('business') >= 0)
    or (content.lower().find('seminar') >= 0)
    or (content.lower().find('training') >= 0)):
        return 'Business'
    else:
        return 'Other'
try:
    mongoClient=pymongo.MongoClient('localhost',27017)
    print "connected successfully!!!"
except pymongo.errors.ConnectionFailure, e:
    print "Could not connect to MongoDB: %s" % e
db =mongoClient.analytics
hotelReviews=db.HotelReviews
hotelReview = hotelReviews.find_one()

index = 1
cursor = hotelReviews.find()
for document in cursor:
    reviews = document.get('Reviews')
    for document2 in reviews:
        dateToChangeItem = document2.get('Date')
        if not isinstance(dateToChangeItem, datetime):
            dateToChange = dateToChangeItem.encode('ascii','ignore')
            isoDateToSave = formatISODate(dateToChange)
            document2.update({'Date':isoDateToSave})
        ratingsToChange = document2.get('Ratings')
        ratingsKeys = ratingsToChange.keys()
        for ratingKey in ratingsKeys:
            valueToChange = ratingsToChange.get(ratingKey)
            badIndex = ratingKey.find('(')
            if badIndex >= 0:
                ratingKeyNew = ratingKey[0:badIndex-1]
                del ratingsToChange[ratingKey]
                ratingKey = ratingKeyNew
            if not isinstance(valueToChange, float):
                valueToSave = float(valueToChange)
                ratingsToChange.update({ ratingKey: valueToSave })
        contentToScan = document2.get('Content').encode('ascii','ignore')
        travelTypeToChange = document2.get('TravelType')
        travelType = findTravelType(contentToScan.encode('ascii','ignore'))
        if travelTypeToChange is None:
            document2.update({'TravelType':travelType})
        elif travelTypeToChange == 'Other' or travelTypeToChange == 'null':
            document2.update({'TravelType':travelType})
    hotelInfo = document.get('HotelInfo')
    address = hotelInfo.get('Address')
    if address is not None:
        addressParts = address.split('<span')
        for addressPart in addressParts:
            if addressPart.find('property') >= 0:
                keyIndexStart = addressPart.find(':') + 1
                keyIndexEnd = addressPart.find('"',keyIndexStart) 
                key = addressPart[keyIndexStart:keyIndexEnd]
                addressValueIndexStart=addressPart.find('>',keyIndexEnd) + 1
                addressValueIndexEnd=addressPart.find('<',addressValueIndexStart)
                addressValue=addressPart[addressValueIndexStart:addressValueIndexEnd]
                keyToSave = hotelInfo.get(key)
                if keyToSave is None:
                    hotelInfo.update({ key: addressValue})
    hotelReviews.save(document)
    print 'updated document number ' + str(index)
    index += 1
    