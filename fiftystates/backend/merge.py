#!/usr/bin/env python
import datetime

from fiftystates.backend import db
from fiftystates.backend.utils import insert_with_id


def merge_state(state):
    merge_metadata(state)
    merge_bills(state)
    merge_legislators(state)


def merge_metadata(state):
    metadata = db.metadata.temp.find_one({'_id': state})
    if not metadata:
        return

    live_metadata = db.metadata.find_one({'_id': state})
    if live_metadata:
        live_updated_at = live_metadata.pop('updated_at',
                                            datetime.datetime.now())
        live_created_at = live_metadata.pop('created_at',
                                            datetime.datetime.now())

        if metadata != live_metadata:
            metadata['updated_at'] = datetime.datetime.now()
        else:
            metadata['updated_at'] = live_updated_at

        metadata['created_at'] = live_created_at
    else:
        metadata['updated_at'] = datetime.datetime.now()
        metadata['created_at'] = metadata['updated_at']

    db.metadata.save(metadata)


def merge_bills(state):
    new_bills_coll = args.state + '.bills.current'
    old_bills_coll = args.state + '.bills.old'
    live_bills_coll = args.state + '.bills'

    if new_bills_coll not in db.collection_names():
        print "No scraper output, exiting."
        return

    if old_bills_coll not in db.collection_names():
        # If we have scraper output but no bills.old collection then this
        # must be an initial import of a session so we can just copy
        # everything over directly to the live data (after adding
        # appropriate updated_at and created_at fields).
        print "Copying bills from %s to %s" % (new_bills_coll, live_bills_coll)
        for bill in db[new_bills_coll].find({'_type': 'bill'}):
            bill['updated_at'] = datetime.datetime.now()
            bill['created_at'] = bill['updated_at']
            db[new_bills_coll].remove({'_id': bill['_id']})
            bill['_id'] = insert_with_id(bill)
            db[new_bills_coll].save(bill)
        db[new_bills_coll].rename(old_bills_coll)
        return

    for bill in db[new_bills_coll].find({'_type': 'bill'}):
        old_bill = db[old_bills_coll].find_one({'_type': 'bill',
                                                'state': bill['state'],
                                                'session': bill['session'],
                                                'chamber': bill['chamber'],
                                                'bill_id': bill['bill_id']})

        if not old_bill:
            # If a bill with corresponding state/session/chamber/bill_id
            # is not in the bills.old collection then it must have been
            # added since the last scraper run.
            bill['created_at'] = datetime.datetime.now()
            bill['updated_at'] = bill['created_at']
            id = bill.pop('_id')
            print "New bill %s" % bill['bill_id']
            bill['_id'] = insert_with_id(bill)
            db[new_bills_coll].remove({'_id': id})
            db[new_bills_coll].save(bill)
        else:
            # We have to remove the 'updated_at' and 'created_at' and
            # 'id' fields of each object or they will always be different
            updated_at = bill.pop('updated_at', None)
            created_at = bill.pop('created_at', None)
            id = bill.pop('_id')
            old_updated_at = old_bill.pop('updated_at',
                                          datetime.datetime.now())
            old_created_at = old_bill.pop('created_at',
                                          datetime.datetime.now())
            old_id = old_bill.pop('_id')

            update_live = True
            if bill != old_bill:
                bill['updated_at'] = datetime.datetime.now()
                bill['created_at'] = old_created_at
            else:
                update_live = False
                bill['updated_at'] = old_updated_at
                bill['created_at'] = old_created_at

            bill['_id'] = old_id

            db[old_bills_coll].remove({'_id': old_id})
            db[new_bills_coll].remove({'_id': id})

            if update_live:
                print "Updating %s" % bill['_id']
                db[live_bills_coll].save(bill)

            db[new_bills_coll].save(bill)

    for old_bill in db[old_bills_coll].find({'_type': 'bill'}):
        # If we didn't get rid of this doc in the last step then there
        # must not be a corresponding bill in the new data set, so
        # delete this.
        print "Deleting %s" % old_bill['_id']
        db[live_bills_coll].remove({'_id': old_bill['_id']})

    if old_bills_coll in db.collection_names():
        db.drop_collection(old_bills_coll)
    db[new_bills_coll].rename(old_bills_coll)


def merge_legislators(state):
    legs_coll = state + ".legislators"
    old_legs_coll = legs_coll + ".old"
    new_legs_coll = legs_coll + ".current"

    if not new_legs_coll in db.collection_names():
        print "No scraped legislator data, exiting."
        return

    # We'll use the state metadata to calculate adjacent sessions
    metadata = db.metadata.find_one({'_id': state})
    if not metadata:
        print "Couldn't find state metadata for %s, exiting" % state
        return

    for legislator in db[new_legs_coll].find({'_type': 'person'}):
        active_role = legislator['roles'][0]

        leg_query = {
            'first_name': legislator['first_name'],
            'last_name': legislator['last_name'],
            'roles': {'$elemMatch': {'state': active_role['state'],
                                     'session': active_role['session'],
                                     'chamber': active_role['chamber'],
                                     'district': active_role['district']}},
            }

        old_legislator = db[old_legs_coll].find_one(leg_query)

        if old_legislator:
            updated_at = legislator.pop('updated_at', datetime.datetime.now())
            created_at = legislator.pop('created_at', datetime.datetime.now())
            id = legislator.pop('_id')
            old_updated_at = old_legislator.pop('updated_at',
                                                datetime.datetime.now())
            old_created_at = old_legislator.pop('created_at',
                                                datetime.datetime.now())
            old_id = old_legislator.pop('_id')

            if legislator != old_legislator:
                legislator['updated_at'] = datetime.datetime.now()
                legislator['created_at'] = old_created_at
                legislator['_id'] = old_id
            else:
                legislator['updated_at'] = old_updated_at
                legislator['created_at'] = old_created_at

            db[new_legs_coll].remove({'_id': id})
            #db[new_legs_coll].save(legislator)
            db[old_legs_coll].remove({'_id': old_id})
        else:
            legislator['updated_at'] = datetime.datetime.now()
            legislator['created_at'] = legislator['updated_at']

        # Now that we've merged between the old and new scraping runs we need
        # to actually merge into the live data. This is much more complicated
        # than with bills because legislator objects are cross-session
        live_legislator = db[legs_coll].find_one(leg_query)

        if not live_legislator:
            # No exact match in live. Look for close matches to merge.
            sessions = [s['name'] for s in metadata['sessions']]
            s_index = sessions.index(active_role['session'])
            if s_index > 0:
                prev_session = sessions[s_index - 1]
                leg_query['roles']['$elemMatch']['session'] = prev_session
                live_legislator = db[legs_coll].find_one(leg_query)
            if not live_legislator and s_index + 1 < len(sessions):
                next_session = sessions[s_index + 1]
                leg_query['roles']['$elemMatch']['session'] = next_session
                live_legislator = db[legs_coll].find_one(leg_query)

        if live_legislator:
            # Merge
            # We pull over roles from different sessions but wipe out
            # roles from the scraped session
            scraped_role_sessions = set([r['session']
                                         for r in legislator['roles']])

            roles = list(legislator['roles'])
            legislator['roles'].extend([
                    r for r in live_legislator['roles']
                    if r['session'] not in scraped_role_session])
            legislator['_id'] = live_legislator['_id']
            db[legs_coll].save(legislator)
        else:
            # New legislator
            legislator['_id'] = insert_with_id(legislator)

        db[new_legs_coll].save(legislator)

    for old_legislator in db[old_legs_coll].find({'_type': 'person'}):
        # Removed legislator
        # TODO: add deleted flag to live legislator
        print "Deleted legislator %s" % old_legislator['full_name']

    if old_legs_coll in db.collection_names():
        db.drop_collection(old_legs_coll)
    db[new_legs_coll].rename(old_legs_coll)

if __name__ == '__main__':
    import os
    import argparse
    from fiftystates import settings
    from fiftystates.backend.utils import base_arg_parser

    parser = argparse.ArgumentParser(parents=[base_arg_parser])
    args = parser.parse_args()

    merge_state(args.state)
