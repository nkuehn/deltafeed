#!/usr/bin/python
import json
import sys
import logging
import time

# native variant (needs native yajl lib installation)
import ijson.backends.yajl2_cffi as ijson
# pure python variant (ca. 66% slower overall):
# import ijson

# for using md5:
# import hashlib
# for using MurmurHash:
import mmh3
import binascii

from jsonpath_rw import jsonpath, parse
# https://pypi.python.org/pypi/jsonpath-rw

# TODO RUN AND TEST THE JSONPATH STUFF

# TODO pass the ID path as parameter -> use a jsonpath library
# TODO dockerize the JSON variant (needs native libs)

startTime = time.time()
if len(sys.argv) <= 2:
    print('new json file name and ID jsonpath parameters are mandatory')
    exit()

fullfile_name = sys.argv[1]
entriesProperty = sys.argv[2]
idJsonPath = sys.argv[3]
idJsonParser = parse(idJsonPath)

deltafile_name = fullfile_name + '.changes.json'
fingerprintsfile_new_name = fullfile_name + '.fingerprints.json'
fingerprintsfile_old_name = ""
if len(sys.argv) > 4:
    fingerprintsfile_old_name = sys.argv[4]
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
    objCount = 0
    deltacount = 0
    duplicateIds = list()

    jsonObjects = ijson.items(fullfile_new, entriesProperty + '.item')

    deltafile.write('{"' + entriesProperty + '":[\n')

    objects = (o for o in jsonObjects)
    for obj in objects:
        objCount += 1

        try:
            # uses first match of the jsonPath as ID
            objId = str(idJsonParser.find(obj)[0].value)
        except:
            logging.exception("message")
            print(str(obj))
            exit()

        if objId in idSet:  # ignore and remember duplicate ids
            duplicateIds.append(objId)
        else:
            idSet.add(objId)
            objJsonString = json.dumps(obj)

            # mmh3 on test file: 57 secs
            # md5 on test file: 58 secs
            # -> no difference -> choose mmh3 for collision avoidance, md5 for portability
            # objDigest = hashlib.md5(objJsonString).hexdigest()
            objDigest = binascii.hexlify(mmh3.hash_bytes(objJsonString))

            fingerprints_new[objId] = objDigest
            # if the obj is new or the obj has changed, write delta.
            # (removes items from old fingerprints to find implicit deletions)
            if (objId not in fingerprints_old) or (fingerprints_old.pop(objId) != objDigest):
                if deltacount > 0: deltafile.write('\n,')
                deltacount += 1
                deltafile.write(objJsonString)

    deltafile.write('\n]}')

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