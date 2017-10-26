from sis_provisioner.events import EventBase
from sis_provisioner.models import User, PRIORITY_HIGH
from sis_provisioner.models.events import PersonLog
from sis_provisioner.exceptions import EventException
from uw_sws.models import Person as PersonModel


log_prefix = 'PERSON:'


class Person(EventBase):
    """
    Collects Person Change Event described by

    """

    # Person V1 Settings
    SETTINGS_NAME = 'PERSON_V1'
    EXCEPTION_CLASS = EventException

    # What we expect in a v1 person message
    _eventMessageType = 'uw-person-change-v1'
    _eventMessageVersion = '1'

    def process_events(self, event, user_model):
        current = event['Current']
        previous = event['Previous']
        net_id = current['UWNetID'] if current else previous['UWNetID']
        if not net_id:
            self._log.info('%s IGNORE missing uwnetid for %s' % (
                log_prefix,
                current['RegID'] if current else previous['RegID']))
            return

        # Preferred name, net_id or reg_id change?
        if (not (previous and current) or
                current['StudentName'] != previous['StudentName'] or
                current['FirstName'] != previous['FirstName'] or
                current['LastName'] != previous['LastName'] or
                current['UWNetID'] != previous['UWNetID'] or
                current['RegID'] != previous['RegID']):

            user = user_model.objects.update_priority(
                PersonModel(uwregid=current['RegID'], uwnetid=net_id),
                PRIORITY_HIGH)

            if user is not None:
                self.record_success(1)

    def record_success(self, event_count):
        self.record_success_to_log(PersonLog, event_count)
