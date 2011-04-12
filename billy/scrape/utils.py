import subprocess

from billy.utils import memoize


def convert_pdf(filename, type='xml'):
    commands = {'text': ['pdftotext', '-layout', filename, '-'],
                'xml':  ['pdftohtml', '-xml', '-stdout', filename],
                'html': ['pdftohtml', '-stdout', filename]}
    pipe = subprocess.Popen(commands[type], stdout=subprocess.PIPE,
                            close_fds=True).stdout
    data = pipe.read()
    pipe.close()
    return data


def pdf_to_lxml(filename, type='html'):
    import lxml.html
    text = convert_pdf(filename, type)
    return lxml.html.fromstring(text)


@memoize
def get_metadata(state):
    metadata = __import__("openstates.%s" % state,
                          fromlist=['metadata']).metadata
    return metadata


@memoize
def get_sessions(state):
    metadata = get_metadata(state)
    sessions = []
    for term in metadata['terms']:
        sessions.extend(term['sessions'])
    return sessions


@memoize
def get_terms(state):
    metadata = get_metadata(state)
    return [term['name'] for term in metadata['terms']]
