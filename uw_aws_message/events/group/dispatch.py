from django.conf import settings
from django.utils.timezone import utc
from sis_provisioner.dao.user import valid_net_id, valid_gmail_id
from sis_provisioner.dao.group import get_effective_members, is_member
from sis_provisioner.dao.course import (
    group_section_sis_id, valid_academic_course_sis_id)
from sis_provisioner.dao.canvas import get_sis_enrollments_for_user_in_course
from sis_provisioner.exceptions import (
    UserPolicyException, GroupPolicyException, GroupNotFoundException,
    GroupUnauthorizedException, CoursePolicyException)
from sis_provisioner.models import (
    Group as GroupModel, CourseMember as CourseMemberModel, User as UserModel,
    GroupMemberGroup as GroupMemberGroupModel, Enrollment as EnrollmentModel,
    PRIORITY_NONE, PRIORITY_DEFAULT, PRIORITY_HIGH, PRIORITY_IMMEDIATE)
from sis_provisioner.events.group.extract import (
    ExtractUpdate, ExtractDelete, ExtractChange)
from restclients_core.exceptions import DataFailureException
from logging import getLogger
import datetime
import re


log_prefix = 'GROUP:'


class Dispatch(object):
    """
    Base class for dispatching on actions within a UW GWS Event
    """
    def __init__(self, config, message):
        self._log = getLogger(__name__)
        self._settings = config
        self._message = message

    def mine(self, group):
        return True

    def run(self, action, group):
        try:
            return {
                'update-members': self.update_members,
                'put-group': self.put_group,
                'delete-group': self.delete_group,
                'put-members': self.put_members,
                'change-subject-name': self.change_subject_name,
                'no-action': self.no_action
            }[action](group)
        except KeyError:
            self._log.info('%s UNKNOWN %s for %s' % (
                log_prefix, action, group))
            return 0

    def update_members(self, group_id):
        # event = ExtractUpdate(self._settings, self._message).extract()
        self._log.info('%s IGNORE update-members for %s' % (
            log_prefix, group_id))
        return 0

    def put_group(self, group_id):
        # event = ExtractPutGroup(self._settings, self._message).extract()
        self._log.info('%s IGNORE put-group %s' % (log_prefix, group_id))
        return 0

    def delete_group(self, group_id):
        # event = ExtractDelete(self._settings, self._message).extract()
        self._log.info('%s IGNORE delete-group %s' % (log_prefix, group_id))
        return 0

    def put_members(self, group_id):
        # event = ExtractPutMembers(self._settings, self._message).extract()
        self._log.info('%s IGNORE put-members for %s' % (log_prefix, group_id))
        return 0

    def change_subject_name(self, group_id):
        # event = ExtractChange(self._settings, self._message).extract()
        self._log.info('%s IGNORE change-subject-name for %s' % (
            log_prefix, group_id))
        return 0

    def no_action(self, group_id):
        return 0


class UWGroupDispatch(Dispatch):
    """
    Canvas Enrollment Group Event Dispatcher
    """
    def __init__(self, config, message):
        super(UWGroupDispatch, self).__init__(config, message)
        self._valid_members = []

    def mine(self, group):
        self._groups = GroupModel.objects.filter(group_id=group)
        self._membergroups = GroupMemberGroupModel \
            .objects.filter(group_id=group)
        return len(self._groups) > 0 or len(self._membergroups) > 0

    def update_members(self, group_id):
        # body contains list of members to be added or removed
        event = ExtractUpdate(self._settings, self._message).extract()

        self._log.info('%s UPDATE membership for %s' % (
            log_prefix, event.group_id))
        updates = [{
            'members': event.add_members,
            'is_deleted': None
        }, {
            'members': event.delete_members,
            'is_deleted': True
        }]

        for update in updates:
            for member in update['members']:
                for group in self._groups:
                    if not group.is_deleted:
                        self._update_group(group, member, update['is_deleted'])

                for member_group in self._membergroups:
                    if not member_group.is_deleted:
                        for group in GroupModel.objects.filter(
                                group_id=member_group.root_group_id,
                                is_deleted__isnull=True):
                            self._update_group(group, member,
                                               update['is_deleted'])

        return len(event.add_members) + len(event.delete_members)

    def delete_group(self, group_id):
        event = ExtractDelete(self._settings, self._message).extract()
        self._log.info('%s DELETE %s' % (log_prefix, event.group_id))

        now = datetime.datetime.utcnow().replace(tzinfo=utc)
        # mark group as delete and ready for import
        GroupModel.objects \
                  .filter(group_id=event.group_id,
                          is_deleted__isnull=True) \
                  .update(is_deleted=True,
                          deleted_date=now,
                          deleted_by='gws-event',
                          priority=PRIORITY_IMMEDIATE)

        # mark member groups
        membergroups = GroupMemberGroupModel.objects.filter(
            group_id=event.group_id, is_deleted__isnull=True)
        membergroups.update(is_deleted=True)

        # mark associated root groups for import
        for membergroup in membergroups:
            GroupModel.objects.filter(group_id=membergroup.root_group_id,
                                      is_deleted__isnull=True) \
                              .update(priority=PRIORITY_IMMEDIATE)

        return 1

    def change_subject_name(self, group_id):
        event = ExtractChange(self._settings, self._message).extract()

        self._log.info('%s UPDATE change-subject-name %s to %s' % (
            log_prefix, event.old_name, event.new_name))

        GroupModel.objects \
                  .filter(group_id=event.old_name) \
                  .update(group_id=event.new_name)
        GroupMemberGroupModel.objects \
                             .filter(group_id=event.old_name) \
                             .update(group_id=event.new_name)
        GroupMemberGroupModel.objects \
                             .filter(root_group_id=event.old_name) \
                             .update(root_group_id=event.new_name)
        return 1

    def _update_group(self, group, member, is_deleted):
        if member.is_group():
            self._update_group_member_group(group, member.name, is_deleted)
        elif member.is_uwnetid() or member.is_eppn():
            try:
                if member.name not in self._valid_members:
                    if member.is_uwnetid():
                        valid_net_id(member.name)
                    elif member.is_eppn():
                        valid_gmail_id(member.name)
                    self._valid_members.append(member.name)

                self._update_group_member(group, member, is_deleted)
            except UserPolicyException:
                self._log.info('%s IGNORE invalid user %s' % (
                    log_prefix, member.name))
        else:
            self._log.info('%s IGNORE member type %s (%s)' % (
                log_prefix, member.member_type, member.name))

    def _update_group_member_group(self, group, member_group, is_deleted):
        try:
            # validity is confirmed by act_as
            (valid, invalid, member_groups) = get_effective_members(
                member_group, act_as=group.added_by)
        except GroupNotFoundException as err:
            GroupMemberGroupModel.objects \
                                 .filter(group_id=member_group) \
                                 .update(is_deleted=True)
            self._log.info("%s REMOVED member group %s not in %s" % (
                log_prefix, member_group, group.group_id))
            return
        except (GroupPolicyException, GroupUnauthorizedException) as err:
            self._log.info('%s IGNORE %s: %s' % (
                log_prefix, group.group_id, err))
            return

        for member in valid:
            self._update_group_member(group, member, is_deleted)

        for mg in [member_group] + member_groups:
            (gmg, created) = GroupMemberGroupModel.objects.get_or_create(
                group_id=mg, root_group_id=group.group_id)
            gmg.is_deleted = is_deleted
            gmg.save()

    def _update_group_member(self, group, member, is_deleted):
        # validity is assumed if the course model exists
        if member.is_uwnetid():
            user_id = member.name
        elif member.is_eppn():
            user_id = valid_gmail_id(member.name)
        else:
            return

        try:
            (cm, created) = CourseMemberModel.objects.get_or_create(
                name=user_id, member_type=member.member_type,
                course_id=group.course_id, role=group.role)
        except CourseMemberModel.MultipleObjectsReturned:
            models = CourseMemberModel.objects.filter(
                name=user_id, member_type=member.member_type,
                course_id=group.course_id, role=group.role)
            self._log.debug('%s MULTIPLE (%s): %s in %s as %s' % (
                log_prefix, len(models), user_id, group.course_id,
                group.role))
            cm = models[0]
            created = False
            for m in models[1:]:
                m.delete()

        if is_deleted:
            # user in other member groups not deleted
            if self._user_in_member_group(group, member):
                is_deleted = None
        elif self._user_in_course(group, member):
            # official student/instructor not added via group
            is_deleted = True

        cm.is_deleted = is_deleted
        cm.priority = PRIORITY_DEFAULT if not cm.queue_id else PRIORITY_HIGH
        cm.save()

        self._log.info('%s %s %s to %s as %s' % (
            log_prefix, 'DELETED' if is_deleted else 'ACTIVE',
            user_id, group.course_id, group.role))

    def _user_in_member_group(self, group, member):
        if self._has_member_groups(group):
            return is_member(
                group.group_id, member.name, act_as=group.added_by)
        return False

    def _user_in_course(self, group, member):
        # academic course?
        try:
            valid_academic_course_sis_id(group.course_id)
        except CoursePolicyException:
            return False

        # provisioned to academic section?
        try:
            user = UserModel.objects.get(net_id=member.name)
            EnrollmentModel.objects.get(
                reg_id=user.reg_id,
                course_id__startswith=group.course_id,
                status='active')
            return True
        except UserModel.DoesNotExist:
            return False
        except EnrollmentModel.DoesNotExist:
            pass

        # inspect Canvas Enrollments
        try:
            canvas_enrollments = get_sis_enrollments_for_user_in_course(
                user.reg_id, group.course_id)
            if len(canvas_enrollments):
                return True
        except DataFailureException as err:
            if err.status == 404:
                pass  # No enrollment
            else:
                raise

        return False

    def _has_member_groups(self, group):
        return GroupMemberGroupModel.objects.filter(
            root_group_id=group.group_id,
            is_deleted__isnull=True).count() > 0


class ImportGroupDispatch(Dispatch):
    """
    Import Group Dispatcher
    """
    def mine(self, group):
        return True if group in settings.SIS_IMPORT_GROUPS else False

    def update_members(self, group):
        # body contains list of members to be added or removed
        self._log.info('%s IGNORE canvas user update: %s' % (
            log_prefix, group))
        return 0


class CourseGroupDispatch(Dispatch):
    """
    Course Group Dispatcher
    """
    def mine(self, group):
        course = ('course_' in group and re.match(
            (r'^course_(20[0-9]{2})'
             r'([a-z]{3})-([a-z\-]+)'
             r'([0-9]{3})([a-z][a-z0-9]?)$'), group))
        if course:
            self._course_sis_id = '-'.join([
                course.group(1),
                {'win': 'winter', 'spr': 'spring', 'sum': 'summer',
                    'aut': 'autumn'}[course.group(2)],
                re.sub('\-', ' ', course.group(3).upper()),
                course.group(4), course.group(5).upper()
            ])
            return True

        return False

    def update_members(self, group):
        # body contains list of members to be added or removed
        self._log.info('%s IGNORE course group update: %s' % (
            log_prefix, self._course_sis_id))
        return 0

    def put_group(self, group_id):
        self._log.info('%s IGNORE course group put-group: %s' % (
            log_prefix, group_id))
        return 0
