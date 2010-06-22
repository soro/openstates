#!/usr/bin/env python
import datetime

from fiftystates.backend import db
from fiftystates.backend.utils import insert_with_id


def merge_state(state):
    new_bills_coll = args.state + '.bills.current'
    old_bills_coll = args.state + '.bills.old'
    live_bills_coll = args.state + '.bills'

    if new_bills_coll not in db.collection_names():
        print "No scraper output, exiting."
        return
    
    if old_bills_coll not in db.collection_names():
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
            bill['created_at'] = datetime.datetime.now()
            bill['updated_at'] = bill['created_at']
            del bill['_id']
            print "New bill %s" % bill['bill_id']
            insert_with_id(bill)
        else:
            # We have to remove the 'updated_at' and 'created_at' and
            # 'id' fields of each object or they will always be different
            updated_at = bill.pop('updated_at', None)
            created_at = bill.pop('created_at', None)
            id = bill.pop('_id')
            old_updated_at = old_bill.pop('updated_at')
            old_created_at = old_bill.pop('created_at')
            old_id = old_bill.pop('_id')
            
            if bill != old_bill:
                bill['updated_at'] = datetime.datetime.now()
                bill['created_at'] = old_created_at
            else:
                bill['updated_at'] = old_updated_at
                bill['created_at'] = old_created_at

            bill['_id'] = old_id
            
            db[old_bills_coll].remove({'_id': old_id})
            db[new_bills_coll].remove({'_id': id})

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
        
    
if __name__ == '__main__':
    import os
    import argparse
    from fiftystates import settings
    from fiftystates.backend.utils import base_arg_parser

    parser = argparse.ArgumentParser(parents=[base_arg_parser])
    args = parser.parse_args()

    merge_state(args.state)
