from django.http import HttpResponse
from django.conf import settings
from sis_provisioner.views.rest_dispatch import RESTDispatch
from sis_provisioner.exceptions import EventException
from sis_provisioner.events.enrollment import Enrollment
from aws_message.aws import SNS, SNSException
from logging import getLogger
import json


class EnrollmentEvent(RESTDispatch):
    """
    AWS SNS delivered UW Course Registration Event handler
    """

    _topicArn = None
    _keys = []

    def __init__(self):
        self._topicArn = settings.AWS_SQS['ENROLLMENT']['TOPIC_ARN']
        self._log = getLogger(__name__)

    def POST(self, request, **kwargs):
        try:
            aws_msg = json.loads(request.body)
            self._log.info("%s on %s" % (aws_msg['Type'], aws_msg['TopicArn']))
            if aws_msg['TopicArn'] == self._topicArn:
                aws = SNS(aws_msg)

                if settings.EVENT_VALIDATE_SNS_SIGNATURE:
                    aws.validate()

                if aws_msg['Type'] == 'Notification':
                    enrollment = Enrollment(aws.extract())

                    if settings.EVENT_VALIDATE_ENROLLMENT_SIGNATURE:
                        enrollment.validate()

                    enrollment.process()
                elif aws_msg['Type'] == 'SubscriptionConfirmation':
                    self._log.info('SubscribeURL: %s' % (
                        aws_msg['SubscribeURL']))
                    aws.subscribe()
            else:
                self._log.error('Unrecognized TopicARN : %s' % (
                    aws_msg['TopicArn']))
                return self.error_response(400, "Invalid TopicARN")
        except ValueError as err:
            self._log.error('JSON : %s' % err)
            return self.error_response(400, "Invalid JSON")
        except EventException as err:
            self._log.error("ENROLLMENT: %s" % (err))
            return self.error_response(500, "Internal Server Error")
        except SNSException as err:
            self._log.error("SNS: %s" % (err))
            return self.error_response(401, "Authentication Failure")
        except Exception as err:
            self._log.error("%s" % (err))
            return self.error_response(500, "Internal Server Error")

        return HttpResponse()
