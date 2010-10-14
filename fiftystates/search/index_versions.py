#!/usr/bin/env python
import sys
import logging
import argparse

from pymongo.son import SON

import urllib
import urllib2_file
import urllib2

from fiftystates.backend import db, fs
from fiftystates.backend.utils import base_arg_parser


DT_FORMAT = "%Y-%m-%d %H:%M:%S"


def index_versions(state, solr_url="http://localhost:8983/solr/"):
    """
    Add the latest version of each bill for a given state to solr.
    """
    for bill in db.bills.find({'state': state}):
        if bill['versions']:
            version = bill['versions'][-1]

            if 'document_id' not in version:
                continue

            doc = fs.get(version['document_id'])

            params = []
            params.append(('literal.bill_id', doc.metadata['bill']['bill_id']))
            params.append(('literal.state', doc.metadata['bill']['state']))
            params.append(('literal.chamber', doc.metadata['bill']['chamber']))

            params.append(('literal.document_name', doc.metadata['name']))
            params.append(('literal.url', doc.metadata['url']))
            params.append(('literal.id', version['document_id']))
            params.append(('literal.created_at',
                           bill['created_at'].strftime(DT_FORMAT)))
            params.append(('literal.updated_at',
                           bill['updated_at'].strftime(DT_FORMAT)))

            # Tika will extract a 'title' field from our document that is
            # usually useless, so we ignore it by fmapping it to a
            # 'ignored_' field.
            # We want to store the actual bill title in a 'title' field
            # but because of the fmap we can't just set literal.title
            # Instead we set an 'ignored_bill_title' field and have a
            # copyField in our solr schema to copy this to 'title'
            params.append(('fmap.title', 'ignored_title'))
            params.append((
                'literal.ignored_bill_title',
                bill['title'].encode('ascii', 'replace')))

            # committing on each upload slows the process down dramatically
            params.append(('commit', 'false'))

            for type in bill.get('type', ['bill']):
                params.append(('literal.type', type))

            url = "%supdate/extract?%s" % (solr_url, urllib.urlencode(params))
            req = urllib2.Request(url, {'file': doc})
            urllib2.urlopen(req)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        parents=[base_arg_parser],
        description="Download and store copies of bill versions.")
    parser.add_argument('-u', '--url', type=str, dest='url',
                        default='http://localhost:8983/solr/',
                        help='the solr instance URL')

    args = parser.parse_args()

    verbosity = {0: logging.WARNING,
                 1: logging.INFO}.get(args.verbose, logging.DEBUG)

    logging.basicConfig(level=verbosity,
                        format=("%(asctime)s %(name)s %(levelname)s " +
                                args.state + " %(message)s"),
                        datefmt="%H:%M:%S")

    index_versions(args.state, args.url)
