#!/usr/bin/env python
import sys
import logging
import argparse

import urllib
import urllib2_file
import urllib2

from fiftystates.backend import db, fs

import pysolr

DT_FORMAT = "%Y-%m-%d %H:%M:%S"


def index_bills(state, solr_url="http://localhost:8983/solr/bills"):
    """
    Add the latest version of each bill for a given state to solr.
    """
    solr = pysolr.Solr(solr_url)

    for bill in db.bills.find({'state': state}):
        doc = {}

        for key in ('bill_id', 'state', 'chamber', 'session'):
            doc[key] = bill[key]

        doc['id'] = bill['_id']
        doc['term'] = bill['_term']
        doc['current_session'] = bill['_current_session']
        doc['current_term'] = bill['_current_term']
        doc['bill_title'] = bill['title'].encode('ascii', 'replace')

        # We store both string and date representations of created_at
        # and updated_at because the date format we want to return via the
        # api is not equivalent to solr's internal date format, but
        # we don't want to have to convert each date value at request time.
        doc['created_at'] = bill['created_at'].strftime(DT_FORMAT)
        doc['updated_at'] = bill['updated_at'].strftime(DT_FORMAT)
        doc['created_at_dt'] = bill['created_at'].isoformat() + "Z"
        doc['updated_at_dt'] = bill['updated_at'].isoformat() + "Z"

        if bill['versions'] and 'document_id' in bill['versions'][-1]:
            version = bill['versions'][-1]

            # Prepend 'literal.' for Solr Cell
            params = [("literal.%s" % key, value)
                      for (key, value) in doc.items()]

            file = fs.get(version['document_id'])

            params.append(('literal.document_name', file.metadata['name']))
            params.append(('literal.url', file.metadata['url']))

            # Tika will extract a 'title' field from our document that is
            # usually useless, so we ignore it by fmapping it to a
            # 'ignored_' field.
            # We want to store the actual bill title in a 'title' field
            # but because of the fmap we can't just set literal.title
            # Instead we set an 'ignored_bill_title' field and have a
            # copyField in our solr schema to copy this to 'title'
            params.append(('fmap.title', 'ignored_title'))

            params.append(('commit', 'false'))

            for type in bill.get('type', ['bill']):
                params.append(('literal.type', type))

            url = "%supdate/extract?%s" % (solr_url, urllib.urlencode(params))
            req = urllib2.Request(url, {'file': file})
            urllib2.urlopen(req)
        else:
            doc['type'] = bill.get('type', ['bill'])
            solr.add([doc], commit=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Download and store copies of bill versions.")
    parser.add_argument('state', type=str,
                        help=('the two-letter abbreviation of the '
                              'state to index'))
    parser.add_argument('-u', '--url', type=str, dest='url',
                        default='http://localhost:8983/solr/bills/',
                        help='the solr instance URL')

    args = parser.parse_args()

    index_bills(args.state, args.url)
