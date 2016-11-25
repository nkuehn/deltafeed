#!/usr/bin/python
import json
import sys
import time
import hashlib

# ijson with native yajl backend (needs native yajl lib installation, see README)
import ijson.backends.yajl2_cffi as ijson
# pure python variant (ca. 66% slower overall):
# import ijson

# This is an example that shows that it's pretty easy to build custom JSON stream parsers
# This example takes a feed that has an array of JSONs named "markets", but the fingerprinting
# is done for "products" entries that are contained in an array per market.
# since it's custom parsing it's not using JSONPath as an abstraction.
#
# call me as  deltajson_customexample.py {youCustomFeedFile}.json

startTime = time.time()
if len(sys.argv) <= 1:
    print('new json file name param is mandatory')
    exit()

fullfile_name = sys.argv[1]

deltafile_name = fullfile_name + '.changes.json'
fingerprintsfile_new_name = fullfile_name + '.fingerprints.json'
fingerprintsfile_old_name = ""
if len(sys.argv) > 2:
    fingerprintsfile_old_name = sys.argv[2]
    if fingerprintsfile_new_name == fingerprintsfile_old_name:
        print(
            'ERROR: last fingerprints file name must differ from new name ' + fingerprintsfile_new_name)
        exit()

with open(
        fullfile_name, 'rb') as fullfile_new, open(
        deltafile_name, 'wb') as deltafile, open(
        fingerprintsfile_new_name , 'w') as fingerprintsfile_new:
    if fingerprintsfile_old_name:
        try:
            fingerprintsfile_old = open(fingerprintsfile_old_name, 'r')
            fingerprints_old = json.load(fingerprintsfile_old)
        except IOError:
            print('ERROR: could not open file ' + fingerprintsfile_old_name)
            exit()
    else:
        print('INFO: no old fingerprints file name passed, starting from scratch')
        fingerprints_old = dict()

    fingerprints_new = dict()
    idSet = set()  # to check uniqueness. Faster than using a list or dict.
    duplicateIds = list()

    # CUSTOM IMPLEMENTATION FROM HERE

    jsonObjects = ijson.items(fullfile_new, 'messages.item.markets.item')

    deltafile.write('{"markets":[\n')

    objCount = 0
    deltacount = 0
    marketcount = 0
    # Half-streaming way: parse the complete JSON of a market and iterate over products inside that.
    # (full streaming would be pretty complex concerning how to
    markets = (o for o in jsonObjects)
    for market in markets:
        prodcount = 0
        if marketcount > 0: deltafile.write('\n,')
        marketcount += 1

        marketId = str(market['wwIdent'])
        print("found market " + str(marketcount) + " : " + marketId)
        deltafile.write('{"wwwIdent": "' + marketId + '", "products": [\n')

        for obj in market['products']:
            objCount += 1
            objNr = str(obj['nan'])
            objId = marketId + '-' + objNr

            if objId in idSet:  # ignore and remember duplicate ids
                duplicateIds.append(objId)
            else:
                idSet.add(objId)
                objJsonString = json.dumps(obj)

                objDigest = hashlib.md5(objJsonString).hexdigest()

                fingerprints_new[objId] = objDigest
                # if the obj is new or the obj has changed, write delta.
                # (removes items from old fingerprints to find implicit deletions)
                if (objId not in fingerprints_old) or (fingerprints_old.pop(objId) != objDigest):
                    if prodcount > 0: deltafile.write('\n,')
                    deltacount += 1
                    prodcount += 1
                    deltafile.write(objJsonString)

        deltafile.write('\n]}')

    deltafile.write('\n]}')

    # END OF CUSTOMIZED PART

    print('DONE: processed ' + '{:,}'.format(objCount) + ' JSON objects, ' + '{:,}'.format(
        len(idSet)) + ' unique IDs, found ' + '{:,}'.format(deltacount) + ' changed and ' + '{:,}'.format(
        len(fingerprints_old)) + ' removed entries.')

    # log duplicate ids
    if len(duplicateIds) > 0:
        print('WARN: ' + '{:,}'.format(
            len(duplicateIds)) + ' duplicate IDs found. Used only first occurrences, writing to file')
        with open(fullfile_name + '.duplicateIds.json', 'w') as duplicateIds_file:
            json.dump(duplicateIds, duplicateIds_file, indent=2)

    # write deleted fingerprints if some remained:
    if len(fingerprints_old) > 0:
        print('INFO: some entries have disappeared since the last file. Writing IDs to file')
        with open(fullfile_name + '.removedIds.json', 'w') as removedIds_file:
            json.dump(fingerprints_old, removedIds_file, indent=2)

    # persist new fingerprints and deltafile:
    deltafile.flush()
    print('wrote delta file')
    json.dump(fingerprints_new, fingerprintsfile_new)
    print('wrote new fingerprints file')
    print('duration: ' + str(time.time() - startTime) + ' seconds')