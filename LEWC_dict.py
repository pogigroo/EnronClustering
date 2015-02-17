import os
import collections
import datetime

import pymongo
from textblob import TextBlob
from dateutil import parser as dateparser

import cPickle


def getBuckets():
    file_buckets_name = 'LIWC_BUCKET_NAMES.txt'
    file_words_name = 'LIWC2007_English080730.txt'
    pathToFiles = r"C:\Users\Eric\PycharmProjects\EnronClustering"

    # load bucket names
    fin = open(os.path.join(pathToFiles, file_buckets_name), 'rb')
    bucketdiction = {}
    for eachLine in fin:
        bucketNumber, bucketName = eachLine.split('\n')[0].split('\t')
        bucketdiction[bucketNumber] = bucketName.strip()

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
            workingDiction[word].append(bucketdiction[eachBucketNumber])
    return {'words': worddiction, 'stem': stemdiction}


def splitOffIndividualComms(inBody):  # split email up between individual's email and email chain

    if 'From:' in inBody:
        return inBody.split('From:')[0]

    return inBody[:10000]


def tokenize(inbody):  # nltk tokenizer punkt
    textbody = TextBlob(inbody).lower()  # textblob object blob.str() ls like str.lower()
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

        workingDiction = finalDiction[fromPerson][dt]  # Creates pointer to final dictionary for Person, day

        tokens = tokenize(splitOffIndividualComms(body))

        totalWords = 0

        for eachDocToken, eachCount in tokens.iteritems():  # iterating through tokens from a document
            totalWords += eachCount
            for eachStemWord, stemBuckets in buckets['stem'].iteritems():  # Stem processing
                if eachDocToken.startswith(eachStemWord):
                    for eachBucket in stemBuckets:
                        workingDiction[eachBucket] += eachCount
        # TODO Add code to include datetime for first and last email of the day, in addition to date, for finalDiction
            if eachDocToken in buckets['words']:  # Full word processing
                for eachBucket in buckets['words'][eachDocToken]:
                    workingDiction[eachBucket] += eachCount

        workingDiction['totalWords'] += totalWords

        #if len(finalDiction.keys()) > 30:
        #    break

    return finalDiction


def writeToMongo(finaldiction, dbname='LEWCtestDB', colname='users'):
    """


    :rtype : int
    :type dbname: basestring
    """
    mc = pymongo.MongoClient()

    db = mc.LEWCtestDB
    # db = mc.eval(dbname)

#    assert isinstance(db.eval(colname).insert, dict)
    ids = db.users.insert(mongokeyfix(finaldiction))

    return ids


def mongokeyfix(keydict):
    """
    Creates list of dictionaries, with date objects replaced with %Y-%m-%d strings
    :param keydict:
    :rtype : dict
    :return:
    """
    output_list = []

    for key, value in keydict.iteritems():
        for date, buckets in value.iteritems():
            # output_list.append({"from": key, "date": date.strftime("%Y-%m-%d"), "buckets": buckets})
            output_list.append({"from": key, "date": datetime.datetime.combine(date, datetime.time.min), "buckets": buckets})
            # [key.replace(".", "_")] = {date.strftime("%Y-%m-%d"): buckets}

    return output_list

def writeToMongo2(finaldiction, dbname='LEWCtestDB', colname='users'):
    """


    :rtype : int
    :type dbname: basestring
    """
    mc = pymongo.MongoClient()
    # db = mc.eval(dbname)
    db = mc.LEWCtestDB

#    assert isinstance(db.eval(colname).insert, dict)
    ids = db.timeframes.insert(mongokeyfix2(finaldiction))

    return ids


def mongokeyfix2(keydict):
    """
    Creates list of dictionaries, with date objects replaced with %Y-%m-%d strings
    :param keydict:
    :rtype : dict
    :return:
    """
    output_list = []

    for key, value in keydict.iteritems():
        for person, buckets in value.iteritems():
            output_list.append({"timeframe": key.days, "from": person, "buckets": buckets})
    return output_list


def breakUpByTimeFrames(bucketCounts):
    """

    :rtype : dict
    """
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
    writeToMongo(bucketCounts, dbname='LEWCEnron', colname='dailyuserbuckets')

    user_buckets_time_frames = breakUpByTimeFrames(bucketCounts)
    writeToMongo2(user_buckets_time_frames, dbname='LEWCEnron', colname='timeframes')

    cPickle.dump(bucketCounts, open(os.path.join(r"C:\Users\Eric\PycharmProjects\EnronClustering", 'bucketcounts.p'), 'rb'))
    cPickle.dump(user_buckets_time_frames, open(os.path.join(r"C:\Users\Eric\PycharmProjects\EnronClustering", 'time_frames.p'), 'rb'))



