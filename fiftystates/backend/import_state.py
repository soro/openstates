#!/usr/bin/env python
import os
import glob

from fiftystates.backend import db
from fiftystates.backend.logs import logger
from fiftystates.backend.utils import rotate_collections, get_class

from saucebrush import run_recipe, Recipe
from saucebrush.sources import JSONSource, MongoDBSource
from saucebrush.emitters import DebugEmitter, MongoDBEmitter, LoggingEmitter
from saucebrush.filters import (UnicodeFilter, UniqueIDValidator, FieldCopier,
                                SubrecordFilter, FieldAdder)

from fiftystates.backend.filters import (Keywordize, SplitName,
                                         LinkNIMSP, TimestampToDatetime,
                                         LinkVotesmart, RequireField,
                                         LegislatorIDValidator)


class FiftystatesRecipe(object):
    def __init__(self, state, stage, source, filters, emitters):
        self.state = state
        self.stage = stage
        self.sources = source
        self.filters = filters
        self.db_filters = []
        self.emitters = emitters

    def pull_db_filters(self):

        def instantiate(filter_spec):
            return get_class(filter_spec['name'])(*filter_spec['args'],
                                                   **filter_spec['kwargs'])

        self.db_filters = []
        doc = db.filters.find_one({'_id': self.state})
        if doc:
            for spec in doc.get('common', []):
                self.db_filters.append(instantiate(spec))

            for spec in doc.get(self.stage, []):
                self.db_filters.append(instantiate(spec))

    def run(self):
        self.pull_db_filters()

        filters = []
        filters.append(self.filters)
        filters.append(self.db_filters)
        filters.append(self.emitters)

        recipe = Recipe(*filters)

        recipe.run(self.source)


def import_metadata(state, data_dir):
    metadata_path = os.path.join(data_dir, state, 'state_metadata.json')

    run_recipe(JSONSource(metadata_path),

               FieldCopier({'_id': 'abbreviation'}),

               LoggingEmitter(logger, "Importing metadata for %(_id)s"),
               MongoDBEmitter(db, 'metadata.temp'),
               )


def import_bills(state, data_dir):
    rotate_collections(args.state + '.bills')

    bills_path = os.path.join(data_dir, state, 'bills', '*.json')

    run_recipe(JSONSource(glob.iglob(bills_path)),

               UniqueIDValidator('state', 'session', 'chamber', 'bill_id'),
               Keywordize('title', '_keywords'),
               UnicodeFilter(),

               SubrecordFilter('sources', TimestampToDatetime('retrieved')),
               SubrecordFilter('actions', TimestampToDatetime('date')),
               SubrecordFilter('votes', TimestampToDatetime('date')),

               LoggingEmitter(logger, "Importing bill %(bill_id)s"),
               MongoDBEmitter('fiftystates', "%s.bills.current" % args.state),
               )


def import_legislators(state, data_dir):
    rotate_collections(args.state + '.legislators')

    legislators_path = os.path.join(data_dir, state, 'legislators',
                                    '*.json')

    run_recipe(JSONSource(glob.iglob(legislators_path)),

               SplitName(),

               SubrecordFilter('roles', FieldAdder('state', args.state)),
               SubrecordFilter('roles', TimestampToDatetime('start_date')),
               SubrecordFilter('roles', TimestampToDatetime('end_date')),

               LinkNIMSP(),
               RequireField('nimsp_candidate_id'),
               LinkVotesmart(args.state),
               RequireField('votesmart_id'),
               LegislatorIDValidator(),

               LoggingEmitter(logger, "Importing legislator %(full_name)s"),
               MongoDBEmitter('fiftystates',
                              "%s.legislators.current" % args.state),
               )


if __name__ == '__main__':
    import logging
    import argparse
    from fiftystates import settings
    from fiftystates.backend.logs import init_mongo_logging
    from fiftystates.backend.utils import base_arg_parser

    parser = argparse.ArgumentParser(parents=[base_arg_parser])

    parser.add_argument('--data_dir', '-d', type=str,
                        help='the base Fifty State data directory')

    args = parser.parse_args()

    if args.data_dir:
        data_dir = args.data_dir
    else:
        data_dir = settings.FIFTYSTATES_DATA_DIR

    init_mongo_logging()
    logger.addHandler(logging.StreamHandler())

    import_metadata(args.state, data_dir)
    import_legislators(args.state, data_dir)
    import_bills(args.state, data_dir)
