import re
import time
import datetime

from fiftystates.backend import db

import nltk
import argparse
import name_tools

base_arg_parser = argparse.ArgumentParser(add_help=False)
base_arg_parser.add_argument('state', type=str,
                             help=('the two-letter abbreviation of the '
                                   'state to import'))


def get_class(name):
    parts = name.split(".")
    c = __import__(".".join(parts[:-1]))
    for part in parts[1:]:
        c = getattr(c, part)
    return c


def insert_with_id(obj):
    """
    Generates a unique ID for the supplied legislator/committee/bill
    and inserts it into the appropriate collection.
    """
    if hasattr(obj, '_id'):
        raise ValueError("object already has '_id' field")

    if obj['_type'] == 'person' or obj['_type'] == 'legislator':
        collection = db.legislators
        id_type = 'L'
    elif obj['_type'] == 'committee':
        collection = db.committees
        id_type = 'C'
    elif obj['_type'] == 'bill':
        collection = db["%s.bills" % obj['state']]
        id_type = 'B'

    id_reg = re.compile('^%s%s' % (obj['state'].upper(), id_type))

    # Find the next available _id and insert
    while True:
        cursor = collection.find({'_id': id_reg}).sort('_id', -1).limit(1)

        try:
            prev_id = cursor.next()['_id']
            obj['_id'] = "%s%06d" % (prev_id[0:3], int(prev_id[3:]) + 1)
        except StopIteration:
            obj['_id'] = "%s%s000001" % (obj['state'].upper(), id_type)

        all_ids = obj.get('_all_ids', [])
        if obj['_id'] not in all_ids:
            all_ids.append(obj['_id'])
        obj['_all_ids'] = all_ids

        if obj['_type'] in ['person', 'legislator']:
            obj['leg_id'] = obj['_id']

        try:
            collection.insert(obj, safe=True)
            return obj['_id']
            break
        except pymongo.DuplicateKeyError:
            continue


def timestamp_to_dt(timestamp):
    return datetime.datetime(*time.localtime(timestamp)[0:7])


def update(old, new, coll):
    # To prevent deleting standalone votes..
    if 'votes' in new and not new['votes']:
        del new['votes']

    changed = False
    for key, value in new.items():
        if old.get(key) != value:
            old[key] = value
            changed = True

    if changed:
        old['updated_at'] = datetime.datetime.now()
        coll.save(old)
