#!/usr/bin/env python
import glob
import datetime

from fiftystates.backend import db
from fiftystates.backend.utils import timestamp_to_dt

import nltk
import name_tools
from nimsp import nimsp, NimspApiError
from votesmart import votesmart, VotesmartApiError

from saucebrush import run_recipe
from saucebrush.filters import (Filter, ConditionalFilter, UnicodeFilter,
                                UniqueIDValidator, FieldCopier)
from saucebrush.sources import JSONSource
from saucebrush.emitters import DebugEmitter, MongoDBEmitter


class Keywordize(Filter):
    def __init__(self, field_name, new_field):
        super(Keywordize, self).__init__()
        self._field_name = field_name
        self._new_field = new_field
        self._stemmer = nltk.stem.porter.PorterStemmer()
        self._stop_words = nltk.corpus.stopwords.words()

    def process_record(self, record):
        sents = nltk.tokenize.sent_tokenize(record[self._field_name])

        words = []
        for sent in sents:
            words.extend(nltk.tokenize.word_tokenize(sent))

        keywords = set([self._stemmer.stem(word.lower()) for word in words if
                        (word.isalpha() or word.isdigit()) and
                        word.lower() not in self._stop_words])

        record[self._new_field] = sorted(list(keywords))

        return record


class SplitName(Filter):
    def __init__(self, name_field='full_name'):
        super(SplitName, self).__init__()
        self._name_field = name_field

    def process_record(self, record):
        # If the record already has first_name and last_name fields
        # then don't overwrite them.
        try:
            if record['first_name'] and record['last_name']:
                return record
        except KeyError:
            pass

        full_name = record[self._name_field]

        (record['prefixes'], record['first_name'],
         record['last_name'], record['suffixes']) = name_tools.split(full_name)

        return record


class AppendStateToRoles(Filter):
    def __init__(self, state):
        self.state = state

    def process_record(self, record):
        for role in record['roles']:
            role['state'] = self.state
        return record


class LinkNIMSP(Filter):
    def __init__(self, apikey=None, election_year='2008'):
        super(LinkNIMSP, self).__init__()
        self.election_year = election_year
        if apikey:
            self._apikey = apikey
        else:
            self._apikey = settings.NIMSP_API_KEY
        nimsp.apikey = self._apikey

    def process_record(self, record):
        role = record['roles'][0]

        # NIMSP is picky about name format
        name = record['last_name']
        if 'suffix' in record and record['suffix']:
            name += " " + record['suffix'].replace(".", "")
        name += ", " + record['first_name'].replace(".", "")

        office_id = dict(upper='S00', lower='R00')[role['chamber']]

        try:
            results = nimsp.candidates.list(
                state=role['state'], office=office_id,
                candidate_name=name, year=self.election_year,
                candidate_status='WON')
        except NimspApiError as e:
            print "Failed matching %s" % name
            record['nimsp_candidate_id'] = None
            return record

        if len(results) == 1:
            record['nimsp_candidate_id'] = int(results[0].imsp_candidate_id)
        else:
            record['nimsp_candidate_id'] = None
            print "Too many results for %s" % name

        return record


class LinkVotesmart(Filter):
    def __init__(self, state, apikey=None):
        super(LinkVotesmart, self).__init__()
        if apikey:
            self._apikey = apikey
        else:
            self._apikey = settings.VOTESMART_API_KEY
        votesmart.apikey = self._apikey

        self._officials = {}

        for chamber, office in dict(upper=9, lower=8).items():
            try:
                self._officials[chamber] = votesmart.officials.getByOfficeState(
                    office, state.upper())
            except VotesmartApiError:
                self._officials[chamber] = []

    def process_record(self, record):
        role = record['roles'][0]

        for official in self._officials[role['chamber']]:
            if (official.firstName == record['first_name'] and
                official.lastName == record['last_name']):

                record['votesmart_id'] = official.candidateId
                return record

        print "VS - failed match %s" % record['full_name']
        return record


class LegislatorIDValidator(ConditionalFilter):
    validator = True

    def __init__(self):
        super(LegislatorIDValidator, self).__init__()
        self._votesmart_seen = set()
        self._nimsp_seen = set()

    def test_record(self, record):
        votesmart_id = record.get('votesmart_id')
        if votesmart_id:
            if votesmart_id in self._votesmart_seen:
                return False
            self._votesmart_seen.add(votesmart_id)

        nimsp_id = record.get('nimsp_candidate_id')
        if nimsp_id:
            if nimsp_id in self._nimsp_seen:
                return False
            self._nimsp_seen.add(nimsp_id)

        return True


class DateFixer(Filter):
    def process_record(self, record):
        for source in record.get('sources', []):
            source['retrieved'] = timestamp_to_dt(source['retrieved'])

        for action in record.get('actions', []):
            action['date'] = timestamp_to_dt(action['date'])

        for role in record.get('roles', []):
            if role['start_date']:
                role['start_date'] = timestamp_to_dt(role['start_date'])

            if role['end_date']:
                role['end_date'] = timestamp_to_dt(role['end_date'])

        for vote in record.get('votes', []):
            vote['date'] = timestamp_to_dt(vote['date'])

        if 'date' in record:
            record['date'] = timestamp_to_dt(record['date'])

        return record


def rotate_collections(base_name):
    new_coll = base_name + ".current"
    old_coll = base_name + ".old"

    if new_coll in db.collection_names():
        if old_coll in db.collection_names():
            db.drop_collection(old_coll)
        db[new_coll].rename(old_coll)


if __name__ == '__main__':
    import os
    import argparse
    import pymongo
    from fiftystates import settings
    from fiftystates.backend.utils import base_arg_parser

    parser = argparse.ArgumentParser(parents=[base_arg_parser])

    parser.add_argument('--data_dir', '-d', type=str,
                        help='the base Fifty State data directory')

    args = parser.parse_args()

    if args.data_dir:
        data_dir = args.data_dir
    else:
        data_dir = settings.FIFTYSTATES_DATA_DIR

    db = pymongo.Connection().fiftystates

    metadata_path = os.path.join(data_dir, args.state, 'state_metadata.json')

    run_recipe(JSONSource(metadata_path),

               FieldCopier({'_id': 'abbreviation'}),
               DateFixer(),

               MongoDBEmitter('fiftystates', 'metadata.temp'),
               )

    rotate_collections(args.state + '.bills')

    bills_path = os.path.join(data_dir, args.state, 'bills', '*.json')

    run_recipe(JSONSource(glob.iglob(bills_path)),

               UniqueIDValidator('state', 'session', 'chamber', 'bill_id'),
               Keywordize('title', '_keywords'),
               UnicodeFilter(),
               DateFixer(),

#               DebugEmitter(),
               MongoDBEmitter('fiftystates', "%s.bills.current" % args.state),
               )

    rotate_collections(args.state + '.legislators')

    legislators_path = os.path.join(data_dir, args.state, 'legislators',
                                    '*.json')

    run_recipe(JSONSource(glob.iglob(legislators_path)),

               SplitName(),
               AppendStateToRoles(args.state),
 #              LinkNIMSP(),
 #              LinkVotesmart(args.state),
               LegislatorIDValidator(),
               DateFixer(),

#               DebugEmitter(),
               MongoDBEmitter('fiftystates',
                              "%s.legislators.current" % args.state),
               )
