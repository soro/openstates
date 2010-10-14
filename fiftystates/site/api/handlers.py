import re
import datetime

from fiftystates.backend import db
from fiftystates.site.geo.models import District
from fiftystates.utils import keywordize

from django.http import HttpResponse

from piston.utils import rc
from piston.handler import BaseHandler, HandlerMetaClass

import pysolr


_chamber_aliases = {
    'assembly': 'lower',
    'house': 'lower',
    'senate': 'upper',
    }

solr = pysolr.Solr("http://localhost:8983/solr/")


class FiftyStateHandlerMetaClass(HandlerMetaClass):
    """
    Returns 404 if Handler result is None.
    """
    def __new__(cls, name, bases, attrs):
        new_cls = super(FiftyStateHandlerMetaClass, cls).__new__(
            cls, name, bases, attrs)

        if hasattr(new_cls, 'read'):
            old_read = new_cls.read

            def new_read(*args, **kwargs):
                obj = old_read(*args, **kwargs)
                if isinstance(obj, HttpResponse):
                    return obj

                if obj is None:
                    return rc.NOT_FOUND

                return obj

            new_cls.read = new_read

        return new_cls


class FiftyStateHandler(BaseHandler):
    """
    Base handler for the Fifty State API.
    """
    __metaclass__ = FiftyStateHandlerMetaClass
    allowed_methods = ('GET',)


class MetadataHandler(FiftyStateHandler):
    def read(self, request, state):
        """
        Get metadata about a state legislature.
        """
        return db.metadata.find_one({'_id': state.lower()})


class BillHandler(FiftyStateHandler):
    def read(self, request, state, session, bill_id, chamber=None):
        query = {'state': state.lower(), 'session': session,
                 'bill_id': bill_id}
        if chamber:
            query['chamber'] = chamber.lower()
        return db.bills.find_one(query)


class BillSearchHandler(FiftyStateHandler):
    def read(self, request):
        _filter = {}

        for key in ('state', 'chamber'):
            try:
                _filter[key] = request.GET[key]
            except KeyError:
                pass

        # process search_window
        search_window = request.GET.get('search_window', '').lower()
        if search_window:
            if search_window == 'session':
                _filter['current_session'] = True
            elif search_window == 'term':
                _filter['current_term'] = True
            elif search_window.startswith('session:'):
                _filter['session'] = search_window.split('session:')[1]
            elif search_window.startswith('term:'):
                _filter['term'] = search_window.split('term:')[1]
            elif search_window == 'all':
                pass
            else:
                resp = rc.BAD_REQUEST
                resp.write(": invalid search_window. Valid choices are "
                           "'term', 'session' or 'all'")
                return resp

        # process updated_since
        since = request.GET.get('updated_since')
        if since:
            try:
                since = datetime.datetime.strptime(since, "%Y-%m-%d %H:%M")
            except ValueError:
                try:
                    since = datetime.datetime.strptime(since, "%Y-%m-%d")
                except ValueError:
                    resp = rc.BAD_REQUEST
                    resp.write(": invalid updated_since parameter."
                    " Please supply a date in YYYY-MM-DD format.")
                    return resp

            _filter['updated_at_dt'] = "[%sZ TO *]" % since.isoformat()

        fq = " ".join(["+%s:%s" % (key, value)
                       for (key, value) in _filter.items()])

        results = solr.search(request.GET['q'], fq=fq)
        return list(results)


class LegislatorHandler(FiftyStateHandler):
    def read(self, request, id):
        return db.legislators.find_one({'_all_ids': id})


class LegislatorSearchHandler(FiftyStateHandler):
    def read(self, request):
        legislator_fields = {'sources': 0, 'roles': 0}

        _filter = _build_mongo_filter(request, ('state', 'first_name',
                                               'last_name'))
        elemMatch = _build_mongo_filter(request, ('chamber', 'term',
                                                  'district', 'party'))
        _filter['roles'] = {'$elemMatch': elemMatch}

        active = request.GET.get('active')
        if not active and 'term' not in request.GET:
            # Default to only searching active legislators if no term
            # is supplied
            _filter['active'] = True
        elif active:
            _filter['active'] = (active.lower() == 'true')

        return list(db.legislators.find(_filter, legislator_fields))


class LegislatorGeoHandler(FiftyStateHandler):
    def read(self, request):
        try:
            districts = District.lat_long(request.GET['lat'],
                                          request.GET['long'])
            filters = []
            for d in districts:
                filters.append({'state': d.state_abbrev,
                                'roles': {'$elemMatch': {'district':d.name,
                                                         'chamber':d.chamber}}}
                              )
            return list(db.legislators.find({'$or': filters}))
        except District.DoesNotExist:
            return rc.NOT_HERE
        except KeyError:
            resp = rc.BAD_REQUEST
            resp.write(": Need lat and long parameters")
            return resp


class CommitteeHandler(FiftyStateHandler):
    def read(self, request, id):
        return db.committees.find_one({'_all_ids': id})


class CommitteeSearchHandler(FiftyStateHandler):
    def read(self, request):
        committee_fields = {'members': 0, 'sources': 0}

        _filter = _build_mongo_filter(request, ('committee', 'subcommittee',
                                                'chamber', 'state'))
        return list(db.committees.find(_filter, committee_fields))


class StatsHandler(FiftyStateHandler):
    def read(self, request):
        counts = {}

        # db.counts contains the output of a m/r run that generates
        # per-state counts of bills and bill sub-objects
        for count in db.counts.find():
            val = count['value']
            state = count['_id']

            if state == 'total':
                val['legislators'] = db.legislators.count()
                val['documents'] = db.documents.files.count()
            else:
                val['legislators'] = db.legislators.find(
                    {'roles.state': state}).count()
                val['documents'] = db.documents.files.find(
                    {'metadata.bill.state': state}).count()

            counts[state] = val

        stats = db.command('dbStats')
        stats['counts'] = counts

        return stats
