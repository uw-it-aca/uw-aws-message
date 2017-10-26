from sis_provisioner.dao.course import is_time_schedule_construction
from sis_provisioner.dao.term import (
    get_term_by_year_and_quarter, get_all_active_terms)
from sis_provisioner.events import EventBase
from sis_provisioner.models.events import InstructorLog
from sis_provisioner.exceptions import EventException
from restclients_core.exceptions import DataFailureException
from uw_sws.models import Section
from uw_canvas.models import CanvasEnrollment
from dateutil.parser import parse as date_parse
from datetime import datetime


log_prefix = 'INSTRUCTOR:'


class InstructorEventBase(EventBase):
    def process_events(self, event):
        self._previous_instructors = self._instructors_from_section_json(
            event['Previous'])
        self._current_instructors = self._instructors_from_section_json(
            event['Current'])
        self._last_modified = date_parse(event['EventDate'])

        section_data = event['Current']
        if not section_data:
            section_data = event['Previous']

        course_data = section_data['Course']

        try:
            term = get_term_by_year_and_quarter(
                section_data['Term']['Year'], section_data['Term']['Quarter'])
            active_terms = get_all_active_terms(datetime.now())
        except DataFailureException as err:
            self._log.info('%s ERROR get term: %s' % (log_prefix, err))
            return

        if term not in active_terms:
            self._log.info(
                '%s IGNORE inactive section %s-%s-%s-%s' % (
                    log_prefix,
                    term.canvas_sis_id(),
                    course_data['CurriculumAbbreviation'],
                    course_data['CourseNumber'],
                    section_data['SectionID']))
            return

        section = Section(
            term=term,
            course_campus=section_data['CourseCampus'],
            curriculum_abbr=course_data['CurriculumAbbreviation'],
            course_number=course_data['CourseNumber'],
            section_id=section_data['SectionID'],
            is_independent_study=section_data['IndependentStudy'])

        if is_time_schedule_construction(section):
            self._log_tsc_ignore(section.canvas_section_sis_id())
            return

        sections = []
        primary_section = section_data["PrimarySection"]
        if (primary_section is not None and
                primary_section["SectionID"] != section.section_id):
            section.is_primary_section = False
            self._set_primary_section(section, primary_section)
            sections.append(section)
        else:
            if len(section_data["LinkedSectionTypes"]):
                for linked_section_type in section_data["LinkedSectionTypes"]:

                    for linked_section_data in \
                            linked_section_type["LinkedSections"]:
                        lsd_data = linked_section_data['Section']
                        section = Section(
                            term=term,
                            curriculum_abbr=lsd_data['CurriculumAbbreviation'],
                            course_number=lsd_data['CourseNumber'],
                            section_id=lsd_data['SectionID'],
                            is_primary_section=False,
                            is_independent_study=section_data[
                                'IndependentStudy'])
                        self._set_primary_section(section, primary_section)
                        sections.append(section)
            else:
                section.is_primary_section = True
                sections.append(section)

        for section in sections:
            self.load_instructors(section)

    def _set_primary_section(self, section, primary_section):
        if primary_section is not None:
            section.primary_section_curriculum_abbr = \
                primary_section['CurriculumAbbreviation']
            section.primary_section_course_number = \
                primary_section['CourseNumber']
            section.primary_section_id = primary_section['SectionID']

    def enrollments(self, reg_id_list, status, section):
        enrollments = []
        enrollment_data = {
            'Section': section,
            'Role': CanvasEnrollment.TEACHER.replace('Enrollment', ''),
            'Status': status,
            'LastModified': self._last_modified,
            'InstructorUWRegID': None
        }

        for reg_id in reg_id_list:
            enrollment_data['UWRegID'] = reg_id
            enrollment_data['InstructorUWRegID'] = reg_id \
                if section.is_independent_study else None

            enrollments.append(enrollment_data)

        return enrollments

    def load_instructors(self, section):
        raise Exception('No load_instructors method')

    def _instructors_from_section_json(self, section):
        instructors = {}
        if section:
            for meeting in section['Meetings']:
                for instructor in meeting['Instructors']:
                    if instructor['Person']['RegID']:
                        instructors[instructor['Person']['RegID']] = instructor
                    else:
                        person = []
                        for k, v in instructor['Person'].iteritems():
                            person.append('[%s] = "%s"' % (k, v))

                        course_data = section['Course']
                        self._log.info(
                            '%s IGNORE missing regid for %s-%s-%s: %s' % (
                                log_prefix,
                                course_data['CurriculumAbbreviation'],
                                course_data['CourseNumber'],
                                section['SectionID'],
                                ', '.join(person)))

        return instructors.keys()

    def record_success(self, event_count):
        self.record_success_to_log(InstructorLog, event_count)


class InstructorAdd(InstructorEventBase):
    """
    UW Course Instructor Add Event Handler
    """
    SETTINGS_NAME = 'INSTRUCTOR_ADD'
    EXCEPTION_CLASS = EventException

    # What we expect in an enrollment message
    _eventMessageType = 'uw-instructor-add'
    _eventMessageVersion = '1'

    def load_instructors(self, section):
        add = [reg_id for reg_id in self._current_instructors
               if reg_id not in self._previous_instructors]
        enrollments = self.enrollments(
            add, CanvasEnrollment.STATUS_ACTIVE, section)
        self.load_enrollments(enrollments)

    def _log_tsc_ignore(self, section_id):
        self._log.info("%s IGNORE add TSC on for %s" % (
            log_prefix, section_id))


class InstructorDrop(InstructorEventBase):
    """
    UW Course Instructor Drop Event Handler
    """
    SETTINGS_NAME = 'INSTRUCTOR_DROP'
    EXCEPTION_CLASS = EventException

    # What we expect in an enrollment message
    _eventMessageType = 'uw-instructor-drop'
    _eventMessageVersion = '1'

    def load_instructors(self, section):
        drop = [reg_id for reg_id in self._previous_instructors
                if reg_id not in self._current_instructors]
        enrollments = self.enrollments(
            drop, CanvasEnrollment.STATUS_DELETED, section)
        self.load_enrollments(enrollments)

    def _log_tsc_ignore(self, section_id):
        self._log.info("%s IGNORE drop TSC on for %s" % (
            log_prefix, section_id))
