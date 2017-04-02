from cStringIO import StringIO
from gzip import GzipFile
import re
import urlparse

from mrjob.protocol import RawProtocol
import warc

from mrcc import CCJob


# note that mf2 class names are case sensitive
# http://microformats.org/wiki/parsing-microformats#Parsing_class_values
#
# (also note that this will find false positives when it matches text outside an
# HTML tag.
#
# mf2 h- vocabularies extracted from:
# http://microformats.org/wiki/h-entry#Core_Properties
MF2_CLASSES = ('adr', 'card', 'entry', 'event', 'feed', 'geo', 'item', 'listing', 'product', 'recipe', 'resume', 'review', 'review-aggregate')
MF2_CLASS_RE = re.compile(r"""
class\s*=\s*["'][^"']*\bh-(%s)\b[^"']*["']
""" % '|'.join(MF2_CLASSES), re.VERBOSE | re.UNICODE)


class ExtractMf2(CCJob):
  INTERNAL_PROTOCOL = RawProtocol
  OUTPUT_PROTOCOL = RawProtocol

  # in-progress request and response record in process_record()
  request = None
  response = None

  def process_record(self, record):
    type = record['WARC-Type']

    if type == 'warcinfo':
      return
    elif type == 'request':
      self.request = record
      return
    elif type == 'response':
      self.response = record
      return

    assert type == 'metadata' and self.request and self.response

    # The HTTP response is defined by a specification: first part is headers
    # (metadata) and then following two CRLFs (newlines) has the response
    payload = self.response.payload.read()
    headers, body = payload.split('\r\n\r\n', 1)
    if 'Content-Type: text/html' in headers:
        # if MF2_CLASS_RE.search(body):
        warcstr = StringIO()
        warcfile = warc.WARCFile(fileobj=GzipFile(fileobj=warcstr, mode='w'))
        warcfile.write_record(self.request)
        warcfile.write_record(self.response)
        warcfile.write_record(record)  # metadata
        warcfile.close()

        domain = urlparse.urlparse(record['WARC-Target-URI']).netloc
        # domain = headers['Host']
        warcbuf = warcstr.getvalue()
        warcstr.close()
        yield domain, warcbuf

    self.request = self.response = None

  def combiner(self, key, values):
    for value in values:
      yield key, value

  def reducer(self, key, values):
    # print '@', `key`
    out = warc.open(u'/tmp/out.warc.gz', 'w')
    for value in values:
      if value:
        for record in warc.WARCFile(fileobj=GzipFile(fileobj=StringIO(value))):
          out.write_record(record)
    out.close()


if __name__ == '__main__':
  ExtractMf2.run()
