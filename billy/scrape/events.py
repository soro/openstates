import os
import uuid
import json

from billy.scrape import Scraper, SourcedObject, JSONDateEncoder
from billy.scrape.utils import get_sessions


class EventScraper(Scraper):
    scraper_type = 'events'

    def scrape(self, chamber, session):
        raise NotImplementedError("EventScrapers must define a scrape method")

    def save_event(self, event):
        event['state'] = self.state

        self.log("save_event %s %s: %s" % (event['when'],
                                           event['type'],
                                           event['description']))

        self.validate_object(event)

        filename = "%s.json" % str(uuid.uuid1())
        with open(os.path.join(self.output_dir, "events", filename), 'w') as f:
            json.dump(event, f, cls=JSONDateEncoder)


class Event(SourcedObject):
    schema = json.load(open(os.path.join(os.path.split(__file__)[0],
                                         '../schemas/event.json')))

    def __init__(self, session, when, type,
                 description, location, end=None, **kwargs):
        super(Event, self).__init__('event', **kwargs)
        self['session'] = session
        self['when'] = when
        self['type'] = type
        self['description'] = description
        self['end'] = end
        self['participants'] = []
        self['location'] = location
        self.update(kwargs)

    def add_participant(self, type, participant, **kwargs):
        kwargs.update({'type': type, 'participant': participant})
        self['participants'].append(kwargs)

    def validate(self):
        super(Event, self).validate()

        if self['session'] not in get_sessions(self['state']):
            raise ValueError("bad session: %s" % self['session'])
