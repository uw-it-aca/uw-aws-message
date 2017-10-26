from sis_provisioner.models import Enrollment
from sis_provisioner.cache import RestClientsCache
from sis_provisioner.exceptions import EventException
from restclients_core.exceptions import DataFailureException
from aws_message.crypto import aes128cbc, Signature, CryptoException
from uw_kws import KWS
from logging import getLogger
from base64 import b64decode
from time import time
from math import floor
import json
import re


class EventBase(object):
    """
    UW Course Event Handler
    """

    _header = None
    _body = None

    def __init__(self, settings, message):
        """
        UW Course Event object

        Takes a dict representing a UW Course Event Message

        Raises EventException
        """
        self._kws = KWS()
        self._settings = settings
        self._re_guid = re.compile(
            r'^[\da-f]{8}(-[\da-f]{4}){3}-[\da-f]{12}$', re.I)
        self._re_json_cruft = re.compile(r'[^{]*({.*})[^}]*')

        try:
            self._header = message['Header']
            self._body = message['Body']
        except KeyError:
            self._header = {}
            self._body = message

        if ('MessageType' in self._header and
                self._header['MessageType'] != self._eventMessageType):
            raise EventException(
                'Unknown Message Type: %s' % (self._header['MessageType']))

        self._log = getLogger(__name__)

    def validate(self):
        try:
            t = self._header['Version']
            if t != self._eventMessageVersion:
                raise EventException('Unknown Version: ' + t)

            to_sign = self._header['MessageType'] + '\n' \
                + self._header['MessageId'] + '\n' \
                + self._header['TimeStamp'] + '\n' \
                + self._body + '\n'

            sig_conf = {
                'cert': {
                    'type': 'url',
                    'reference': self._header['SigningCertURL']
                }
            }

            Signature(sig_conf).validate(to_sign.encode('ascii'),
                                         b64decode(self._header['Signature']))
        except KeyError as err:
            if len(self._header):
                raise EventException('Invalid Signature Header: %s' % (err))
        except CryptoException as err:
            raise EventException('Crypto: %s' % (err))
        except Exception as err:
            raise EventException('Invalid signature: %s' % (err))

    def extract(self):
        try:
            if 'Encoding' not in self._header:
                if isinstance(self._body, str):
                    return(json.loads(
                        self._re_json_cruft.sub(r'\g<1>', self._body)))
                elif isinstance(self._body, dict):
                    return self._body
                else:
                    raise EventException('No body encoding')

            t = self._header['Encoding']
            if str(t).lower() != 'base64':
                raise EventException('Unkown encoding: ' + t)

            t = self._header.get('Algorithm', 'aes128cbc')
            if str(t).lower() != 'aes128cbc':
                raise EventException('Unsupported algorithm: ' + t)

            key = None
            if 'KeyURL' in self._header:
                key = self._kws._key_from_json(
                    self._kws._get_resource(self._header['KeyURL']))
            elif 'KeyId' in self._header:
                key = self._kws.get_key(self._header['KeyId'])
            else:
                try:
                    key = self._kws.get_current_key(
                        self._header['MessageType'])
                    if not re.match(r'^\s*{.+}\s*$', self._body):
                        raise CryptoException()
                except (ValueError, CryptoException) as err:
                    RestClientsCache().delete_cached_kws_current_key(
                        self._header['MessageType'])
                    key = self._kws.get_current_key(
                        self._header['MessageType'])

            cipher = aes128cbc(b64decode(key.key),
                               b64decode(self._header['IV']))
            body = cipher.decrypt(b64decode(self._body))
            return(json.loads(self._re_json_cruft.sub(r'\g<1>', body)))
        except KeyError as err:
            self._log.error(
                "Key Error: %s\nHEADER: %s" % (err, self._header))
            raise
        except ValueError as err:
            self._log.error(
                "Error: %s\nHEADER: %s\nBODY: %s" % (
                    err, self._header, self._body))
            return {}
        except CryptoException as err:
            self._log.error(
                "Error: %s\nHEADER: %s\nBODY: %s" % (
                    err, self._header, self._body))
            raise EventException('Cannot decrypt: %s' % (err))
        except DataFailureException as err:
            msg = "Request failure for %s: %s (%s)" % (
                err.url, err.msg, err.status)
            self._log.error(msg)
            raise EventException(msg)
        except Exception as err:
            raise EventException('Cannot read: %s' % (err))

    def process(self):
        if self._settings.get('VALIDATE_MSG_SIGNATURE', True):
            self.validate()

        self.process_events(self.extract())

    def process_events(self, events):
        raise EventException('No event processor defined')

    def load_enrollments(self, enrollments):
        enrollment_count = len(enrollments)
        if enrollment_count:
            for enrollment in enrollments:
                try:
                    Enrollment.objects.add_enrollment(enrollment)
                except Exception as err:
                    raise EventException('Load enrollment failed: %s' % (err))

            try:
                self.record_success(enrollment_count)
            except:
                pass

    def record_success_to_log(self, log_model, event_count):
        minute = int(floor(time() / 60))
        try:
            e = log_model.objects.get(minute=minute)
            e.event_count += event_count
        except log_model.DoesNotExist:
            e = log_model(minute=minute, event_count=event_count)

        e.save()

        if e.event_count <= 5:
            limit = self._settings.get(
                'EVENT_COUNT_PRUNE_AFTER_DAY', 7) * 24 * 60
            prune = minute - limit
            log_model.objects.filter(minute__lt=prune).delete()
