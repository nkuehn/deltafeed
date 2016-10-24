#!/usr/bin/python
import json
import csv
import hashlib
import sys

# TODO test URL as parameter
# TODO investigate why memory usage is too high. Should stream like the JSON variant. Suspect is inside the CSV writer.
# TODO allow piping in the CSV, so it can directly be preprocessed with csvkit.

if len(sys.argv) <= 2:
    print('new csv file name and ID column parameter is mandatory')
    exit()

fullfile_name = sys.argv[1]
idColumnParam = sys.argv[2]
deltafile_name = fullfile_name + '.changes.csv'
fingerprintsfile_new_name = fullfile_name + '.fingerprints.json'
fingerprintsfile_old_name = ""
if len(sys.argv) > 3:
    fingerprintsfile_old_name = sys.argv[3]
    if fingerprintsfile_new_name == fingerprintsfile_old_name:
        print(
            'ERROR: last fingerprints file name must differ from new name ' + fingerprintsfile_new_name)
        exit()

with open(
        fullfile_name, 'rb') as fullfile_new, open(
        deltafile_name, 'wb') as deltafile, open(
        fingerprintsfile_new_name, 'w') as fingerprintsfile_new:
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

    fullfilereader = csv.reader(fullfile_new)
    fingerprints_new = dict()
    idSet = set()  # to check uniqueness. Faster than using list or dict.

    deltawriter = csv.writer(deltafile)
    lineNumber = 0
    deltacount = 0
    duplicateIds = list()
    for row in fullfilereader:
        lineNumber += 1
        # handle the header row:
        if lineNumber == 1:
            if idColumnParam.isdigit():
                idColumnIndex = int(idColumnParam)
            else:
                idColumnIndex = row.index(idColumnParam)
            deltawriter.writerow(row)
            continue
        rowId = row[idColumnIndex]
        if rowId in idSet:  # ignore and remember duplicate ids
            duplicateIds.append(rowId)
        else:
            idSet.add(rowId)
            hasher = hashlib.md5()
            for col in row:
                hasher.update(col)
                hasher.update(b',')  # some separator is necessary to differentiate AB,BC from A,BBC CSV rows
            rowDigest = hasher.hexdigest()
            fingerprints_new[rowId] = rowDigest
            # if the row is new or the row has changed, write delta.
            # (removes items from old fingerprints to find implicit deletions)
            if (rowId not in fingerprints_old) or (fingerprints_old.pop(rowId) != rowDigest):
                deltacount += 1
                deltawriter.writerow(row)

    print('DONE: processed ' + '{:,}'.format(lineNumber) + ' CSV rows, ' + '{:,}'.format(
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

