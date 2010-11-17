#!/usr/bin/env python
import sys
import logging
import argparse

import urllib2

from fiftystates.backend import db

import pysolr

DT_FORMAT = "%Y-%m-%d %H:%M:%S"


def index_legislators(state,
                      solr_url="http://localhost:8983/solr/legislators"):
    solr = pysolr.Solr(solr_url)

    for leg in db.legislators.find({'roles.state': state}):
        doc = {}

        for key in ('leg_id', 'full_name', 'first_name', 'last_name',
                    'middle_name', 'suffixes', 'chamber', 'district',
                    'party', 'votesmart_id', 'nimsp_candidate_id',
                    'active'):
            doc[key] = leg.get(key, '')

        doc['active'] = doc['active'] or False

        doc['created_at'] = leg['created_at'].strftime(DT_FORMAT)
        doc['updated_at'] = leg['updated_at'].strftime(DT_FORMAT)
        doc['created_at_dt'] = leg['created_at'].isoformat() + "Z"
        doc['updated_at_dt'] = leg['updated_at'].isoformat() + "Z"

        solr.add([doc], commit=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Index legislators in solr.")
    parser.add_argument('state', type=str,
                        help=('the two-letter abbreviation of the '
                              'state to index'))
    parser.add_argument('-u', '--url', type=str, dest='url',
                        default="http://localhost:8983/solr/legislators/",
                        help='the solr instance URL')

    args = parser.parse_args()

    index_legislators(args.state, args.url)
