from sis_provisioner.events import EventBase
from sis_provisioner.models.events import EnrollmentLog
from sis_provisioner.exceptions import (
    EventException, UnhandledActionCodeException)
from uw_sws.models import Term, Section
from uw_canvas.models import CanvasEnrollment
from dateutil.parser import parse as date_parse


log_prefix = 'ENROLLMENT:'


class Enrollment(EventBase):
    """
    Collects enrollment event described by
    https://wiki.cac.washington.edu/display/StudentEvents/UW+Course+Enrollment+v2
    """

    # Enrollment Version 2 settings
    SETTINGS_NAME = 'ENROLLMENT_V2'
    EXCEPTION_CLASS = EventException

    #  What we expect in a v1 enrollment message
    #  _eventMessageType = 'uw-student-registration'
    #   eventMessageVersion = '1'

    # What we expect in a v2 enrollment message
    _eventMessageType = 'uw-student-registration-v2'
    _eventMessageVersion = '2'

    def process_events(self, events):
        enrollments = []
        for event in events.get('Events', []):
            section_data = event['Section']
            course_data = section_data['Course']

            section = Section()
            section.term = Term(quarter=course_data['Quarter'],
                                year=course_data['Year'])
            section.curriculum_abbr = course_data['CurriculumAbbreviation']
            section.course_number = course_data['CourseNumber']
            section.section_id = section_data['SectionID']
            section.is_primary_section = True
            section.linked_section_urls = []

            if ('PrimarySection' in event and
                    'Course' in event['PrimarySection']):
                primary_course = event['PrimarySection']['Course']
                if primary_course:
                    section.is_primary_section = False
                    section.primary_section_curriculum_abbr = \
                        primary_course['CurriculumAbbreviation']
                    section.primary_section_course_number = \
                        primary_course['CourseNumber']
                    section.primary_section_id = \
                        event['PrimarySection']['SectionID']

            try:
                data = {
                    'Section': section,
                    'Role': CanvasEnrollment.STUDENT.replace('Enrollment', ''),
                    'UWRegID': event['Person']['UWRegID'],
                    'Status': self._enrollment_status(event, section),
                    'LastModified': date_parse(event['LastModified']),
                    'InstructorUWRegID': event['Instructor']['UWRegID'] if (
                        'Instructor' in event and event['Instructor'] and
                        'UWRegID' in event['Instructor']) else None
                }

                if 'Auditor' in event and event['Auditor']:
                    data['Role'] = 'Auditor'

                if 'RequestDate' in event:
                    data['RequestDate'] = date_parse(event['RequestDate'])

                enrollments.append(data)
            except UnhandledActionCodeException:
                self._log.warning("%s UNKNOWN %s for %s at %s" % (
                    log_prefix,
                    event['Action']['Code'],
                    event['Person']['UWRegID'],
                    event['LastModified']))
                pass

        self.load_enrollments(enrollments)

    def record_success(self, event_count):
        self.record_success_to_log(EnrollmentLog, event_count)

    def _enrollment_status(self, event, section):
        # Canvas "active" corresponds to Action codes:
        #   "A" == ADDED and
        #   "S" == STANDBY (EO only status)
        action_code = event['Action']['Code'].upper()

        if action_code == 'A':
            return CanvasEnrollment.STATUS_ACTIVE

        if action_code == 'S':
            self._log.debug("%s ADD standby %s to %s" % (
                log_prefix,
                event['Person']['UWRegID'],
                section.canvas_section_sis_id()))
            return CanvasEnrollment.STATUS_ACTIVE

        if action_code == 'D':
            return CanvasEnrollment.STATUS_DELETED

        raise UnhandledActionCodeException()
