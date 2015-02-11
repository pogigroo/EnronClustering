import os, collections, pymongo, datetime


month2Int = {'JANUARY':1,
             'FEBRUARY':2}



def getBuckets():
    file_buckets_name = 'LIWC_BUCKET_NAMES.txt'
    file_words_name = 'LIWC2007_English080730.txt'
    pathToFiles = r"C:\Users\Eric\Dropbox (Personal)\Clustering"
    
    
    
    #load bucket names
    
    fin = open (os.path.join (pathToFiles, file_buckets_name),'rb')
    bucketDiction = {}
    for eachLine in fin:
        bucketNumber, bucketName = eachLine.split ('\n')[0].split('\t')
        bucketDiction [bucketNumber] = "%s_%s"%(bucketName.strip(), bucketNumber)
        
    #load words to buckets
    
    fin = open (os.path.join (pathToFiles, file_words_name),'rb')
    wordDiction = collections.defaultdict(list)
    stemDiction = collections.defaultdict(list)
    
    for eachLine in fin:
        newLine = eachLine.split ('\r\n')[0].split('\t')
        word = newLine[0].lower()
        
        if '*' in word:
            word = word.split ('*')[0]
            workingDiction = stemDiction
        else:
            workingDiction = wordDiction    
            
        for eachBucketNumber in newLine[1:]:
            workingDiction [bucketDiction[eachBucketNumber]].append(word)
    
    return {'stem':stemDiction, 'words':wordDiction} 


def getDateTime (dtStr):# "Mon, 14 May 2001 16:39:00 -0700 (PDT)"
    
    dtSplit = dtStr.split ()
    day = int(dtSplit[1].strip())
    month = month2Int[dtSplit [2].strip().upper()]
    year = int(dtSplit[3].strip())
    
    return datetime.datetime(year, month, day)
    
    
def splitOffIndividualComms(inBody):#split email up between individual's email and email chain
    
    if 'From:' in inBody:
        return inBody.split ('From:')[0]
        
    return inBody[:10000]
    
def tokenize(inBody):#nltk tokenizer punkt 
    
    splitBySpace = inBody.split()
    storageDiction = collections.defaultdict(int)
    for each in splitBySpace:
        #delete punctuation from left
        #delete punctuation from right
        storageDiction [word] += 1
        
        
    return storageDiction
        
    
    

def parseDocuments(buckets):
    
    mc = pymongo.MongoClient()
    #Don't know the name of the db and collection name at this point. This needs to be fixed/changed 
    db = mc.ENRONDB
    
#    for eachRecord in db.ENRONCOLLECTIONNAME.find({'headers.From':'phillip.allen@enron.com'}):

    finalDiction = {}
    for eachRecord in db.ENRONCOLLECTIONNAME.find():
        body = eachRecord['body']
        fromPerson = eachRecord['headers']['From']
        dt = getDateTime (eachRecord['headers']['Date'])
        
        if fromPerson not in finalDiction:
            finalDiction [fromPerson] = {dt:collections.defaultdict(int)}
        elif dt not in finalDiction [fromPerson]:
            finalDiction [fromPerson][dt] = collections.defaultdict(int)            
        
        workingDiction = finalDiction [fromPerson][dt]
            
        tokens = tokenize(splitOffIndividualComms(body))
        
        totalWords = 0
                
        for eachDocToken, eachCount in tokens.iteritems():#iterating through tokens from a document
            totalWords += eachCount
            for eachStemWord, stemBuckets in buckets['stem']:#Stem processing
                if eachDocToken.startswith(eachStemWord):
                    for eachBucket in stemBuckets:
                        workingDiction [eachBucket] += eachCount
                    
            if eachDocToken in buckets['words']:#Full word processing
                for eachBucket in buckets['words'][eachDocToken]:
                        workingDiction [eachBucket] += eachCount
                        

        workingDiction ['totalWords'] += totalWords                                             
                        
    return finalDiction
                    
            
def breakUpByTimeFrames (bucketCounts):
    
    timeFrames = [datetime.timedelta(days=90), datetime.timedelta(days = 7)]
    
    personLastDate = {}
    
    for eachPerson, eachSubDiction in bucketCounts.iteritems():
        personLastDate [eachPerson] = sorted(eachSubDiction.keys(), reverse=True)[0]
        
    timeframeSumDiction = {}
    
        
    for eachTimeFrame in timeFrames:
        timeframeSumDiction [eachTimeFrame] = {}
        for eachPerson, eachSubDiction in bucketCounts.iteritems():
            if eachPerson not in timeframeSumDiction[eachTimeFrame]:
                timeframeSumDiction [eachTimeFrame][eachPerson] = collections.defaultdict(int)
                
            for eachDate, eachIndividualBucketDiction in sorted (eachSubDiction.iteritems(), reverse=True):
                if personLastDate[eachPerson] - eachDate > eachTimeFrame:
                    break
                #summation accross timeframe for each bucket
                for eachBucket, eachCount in eachIndividualBucketDiction.iteritems():
                    timeframeSumDiction [eachTimeFrame][eachPerson][eachBucket] += eachCount
                
    return timeframeSumDiction        
    
                            
            

if __name__ == '__main__':
    
    buckets = getBuckets()
    bucketCounts = parseDocuments(buckets)
    breakUpByTimeFrames(bucketCounts)


