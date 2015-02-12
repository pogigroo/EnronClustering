import os
import collections
import datetime

import pymongo
from textblob import TextBlob
from dateutil import parser as dateparser


def getBuckets():
    file_buckets_name = 'LIWC_BUCKET_NAMES.txt'
    file_words_name = 'LIWC2007_English080730.txt'
    pathToFiles = r"C:\Users\Eric\PycharmProjects\EnronClustering"

    # load bucket names
    fin = open(os.path.join(pathToFiles, file_buckets_name), 'rb')
    bucketdiction = {}
    for eachLine in fin:
        bucketNumber, bucketName = eachLine.split('\n')[0].split('\t')
        bucketdiction[bucketNumber] = "%s_%s" % (bucketName.strip(), bucketNumber)

    # load words to buckets
    fin = open(os.path.join(pathToFiles, file_words_name), 'rb')
    worddiction = collections.defaultdict(list)
    stemdiction = collections.defaultdict(list)

    for eachLine in fin:
        newLine = eachLine.split('\r\n')[0].split('\t')
        word = newLine[0].lower()

        if '*' in word:
            word = word.split('*')[0]
            workingDiction = stemdiction
        else:
            workingDiction = worddiction

        for eachBucketNumber in newLine[1:]:
            workingDiction[bucketdiction[eachBucketNumber]].append(word)

    return {'stem': stemdiction, 'words': worddiction}


def splitOffIndividualComms(inBody):  # split email up between individual's email and email chain

    if 'From:' in inBody:
        return inBody.split('From:')[0]

    return inBody[:10000]


def tokenize(inbody):  # nltk tokenizer punkt

    textbody = TextBlob(inbody)
    storagediction = collections.defaultdict(int)
    for word in textbody.words:
        storagediction[word] += 1
    return storagediction


def parseDocuments(buckets):
    mc = pymongo.MongoClient()
    db = mc.enron_mail

    # BAD practice: DB and Collection names are 'enron_mail' and 'messages'
    # for eachRecord in db.messages.find({'headers.From':'phillip.allen@enron.com'}):

    finalDiction = {}
    for eachRecord in db.messages.find():
        body = eachRecord['body']
        fromPerson = eachRecord['headers']['From']
        dt = dateparser.parse(eachRecord['headers']['Date']).date()

        if fromPerson not in finalDiction:
            finalDiction[fromPerson] = {dt: collections.defaultdict(int)}
        elif dt not in finalDiction[fromPerson]:
            finalDiction[fromPerson][dt] = collections.defaultdict(int)

        workingDiction = finalDiction[fromPerson][dt]

        tokens = tokenize(splitOffIndividualComms(body))

        totalWords = 0

        for eachDocToken, eachCount in tokens.iteritems():  # iterating through tokens from a document
            totalWords += eachCount
            for eachStemWord, stemBuckets in buckets['stem'].iteritems():  # Stem processing
                if eachDocToken.startswith(eachStemWord):
                    for eachBucket in stemBuckets:
                        workingDiction[eachBucket] += eachCount

            if eachDocToken in buckets['words']:  # Full word processing
                for eachBucket in buckets['words'][eachDocToken]:
                    workingDiction[eachBucket] += eachCount

        workingDiction['totalWords'] += totalWords

    return finalDiction


def breakUpByTimeFrames(bucketCounts):
    timeFrames = [datetime.timedelta(days=90), datetime.timedelta(days=7)]

    personLastDate = {}

    for eachPerson, eachSubDiction in bucketCounts.iteritems():
        personLastDate[eachPerson] = sorted(eachSubDiction.keys(), reverse=True)[0]

    timeframeSumDiction = {}

    for eachTimeFrame in timeFrames:
        timeframeSumDiction[eachTimeFrame] = {}
        for eachPerson, eachSubDiction in bucketCounts.iteritems():
            if eachPerson not in timeframeSumDiction[eachTimeFrame]:
                timeframeSumDiction[eachTimeFrame][eachPerson] = collections.defaultdict(int)

            for eachDate, eachIndividualBucketDiction in sorted(eachSubDiction.iteritems(), reverse=True):
                if personLastDate[eachPerson] - eachDate > eachTimeFrame:
                    break
                # summation across time-frame for each bucket
                for eachBucket, eachCount in eachIndividualBucketDiction.iteritems():
                    timeframeSumDiction[eachTimeFrame][eachPerson][eachBucket] += eachCount

    return timeframeSumDiction



if __name__ == '__main__':
    buckets = getBuckets()
    bucketCounts = parseDocuments(buckets)
    breakUpByTimeFrames(bucketCounts)


