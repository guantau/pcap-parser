# -*- coding: utf-8 -*-
from __future__ import unicode_literals, print_function, division

from io import StringIO
import sys

from config import OutputLevel
# print http req/resp
import utils, six
import config
import threading
import urllib
from constant import Compress
from pyconnect import PyConnect


connect = PyConnect('localhost', 27017)
connect.use('http')
connect.setCollection('metadata')

printer_lock = threading.Lock()


def _get_full_url(uri, host):
    if uri.startswith(b'http://') or uri.startswith(b'https://'):
        return urllib.unquote(uri)
    else:
        return urllib.unquote(b'http://' + host + uri)


class HttpPrinter(object):
    def __init__(self, client_host, remote_host):
        self.parse_config = config.get_config()
        self.buf = StringIO()
        self.client_host = client_host
        self.remote_host = remote_host

    def on_http_req(self, req_header, req_body):
        """
        :type req_header: HttpRequestHeader
        :type req_body: bytes
        """
        mime, charset = utils.parse_content_type(req_header.content_type)
        if self.parse_config.encoding and not charset:
            charset = self.parse_config.encoding
        if req_header.compress == Compress.IDENTITY:
            # if is gzip by content magic header
            # someone missed the content-encoding header
            if utils.gzipped(req_body):
                req_header.compress = Compress.GZIP

        if self.parse_config.mongo:
            filter = {b"micro_second":req_header.micro_second,
                      b"client_ip":self.client_host[0], b"client_port":self.client_host[1],
                      b"remote_ip":self.remote_host[0], b"remote_port":self.remote_host[1]}
            doc = dict()
            doc[b"req_url"] = req_header.method + b" " + _get_full_url(req_header.uri, req_header.host)
            doc[b"req_header"] = req_header.raw_data
            content = self._decode_body(req_body, req_header.compress, mime, charset)
            if content:
                doc[b"req_body"] = content
            connect.update(filter, doc)
        else:
            if self.parse_config.level == OutputLevel.ONLY_URL:
                self._println(req_header.method + b" " + _get_full_url(req_header.uri, req_header.host))
            elif self.parse_config.level == OutputLevel.HEADER:
                self._println(req_header.raw_data)
                self._println()
            elif self.parse_config.level >= OutputLevel.TEXT_BODY:
                self._println(req_header.raw_data)
                self._println()

                # usually charset is not set in http post
                output_body = self._if_output(mime)
                if output_body:
                    self._print_body(req_body, req_header.compress, mime, charset)
                    self._println('')

    def on_http_resp(self, resp_header, resp_body):
        """
        :type resp_header: HttpResponseHeader
        :type resp_body: bytes
        """

        mime, charset = utils.parse_content_type(resp_header.content_type)
        # usually charset is not set in http post
        if self.parse_config.encoding and not charset:
            charset = self.parse_config.encoding
        if resp_header.compress == Compress.IDENTITY:
            # if is gzip by content magic header
            # someone missed the content-encoding header
            if utils.gzipped(resp_body):
                resp_header.compress = Compress.GZIP

        if self.parse_config.mongo:
            filter = {b"micro_second":resp_header.micro_second,
                      b"client_ip":self.client_host[0], b"client_port":self.client_host[1],
                      b"remote_ip":self.remote_host[0], b"remote_port":self.remote_host[1]}
            doc = dict()
            doc[b"resp_header"] = resp_header.raw_data
            content = self._decode_body(resp_body, resp_header.compress, mime, charset)
            if content:
                doc[b"resp_body"] = content
            connect.update(filter, doc)
        else:
            if self.parse_config.level == OutputLevel.ONLY_URL:
                self._println(resp_header.status_line)
            elif self.parse_config.level == OutputLevel.HEADER:
                self._println(resp_header.raw_data)
                self._println()
            elif self.parse_config.level >= OutputLevel.TEXT_BODY:
                self._println(resp_header.raw_data)
                self._println()
                output_body = self._if_output(mime)
                if output_body:
                    self._print_body(resp_body, resp_header.compress, mime, charset)
                    self._println()

            if not config.get_config().group:
                self._do_output()

    def finish(self):
        """called when this connection finished"""
        self._do_output()

    def _do_output(self):
        printer_lock.acquire()
        try:
            value = self.buf.getvalue()
            self.buf = StringIO()
            if value:
                print("[%s:%d] -- -- --> [%s:%d] " % (self.client_host[0], self.client_host[1],
                                                      self.remote_host[0], self.remote_host[1]),
                      file=config.out)
                if six.is_python2:
                    print(value.encode('utf8'), file=config.out)
                else:
                    print(value, file=config.out)
                config.out.flush()
        except IOError as e:
            if e.errno == 32:
                # may be pipe closed
                sys.exit(0)
            else:
                print(e, file=sys.stderr)
                sys.exit(-1)

        finally:
            printer_lock.release()

    def _if_output(self, mime):
        return self.parse_config.level >= OutputLevel.ALL_BODY and not utils.is_binary(mime) \
               or self.parse_config.level >= OutputLevel.TEXT_BODY and utils.is_text(mime)

    def _println(self, line=''):
        line = six.ensure_unicode(line)
        self.buf.write(line)
        self.buf.write('\n')

    def _println_if(self, level, line):
        if self.parse_config.level >= level:
            self._println(line)

    def _print_body(self, body, compress, mime, charset):
        content = self._decode_body(body, compress, mime, charset)
        if content:
            self.buf.write(content)
            self.buf.write('\n')

    def _decode_body(self, body, compress, mime, charset):
        if compress == Compress.GZIP:
            body = utils.ungzip(body)
        elif compress == Compress.DEFLATE:
            body = utils.decode_deflate(body)

        content = utils.decode_body(body, charset)
        if content:
            if not mime:
                # guess mime...
                if content.startswith('{') and content.endswith('}') or content.startswith('[') \
                        and content.endswith(']'):
                    mime = b'application/json'
            if mime is None:
                mime = b''
            if self.parse_config.pretty:
                bodybuf = StringIO()
                if b'json' in mime:
                    utils.try_print_json(content, bodybuf)
                    content = bodybuf.getvalue()
                elif b'www-form-urlencoded' in mime:
                    utils.try_decoded_print(content, bodybuf)
                    content = bodybuf.getvalue()
        return content
