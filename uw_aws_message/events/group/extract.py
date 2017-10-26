from sis_provisioner.models.events import GroupEvent
from aws_message.extract import Extract, ExtractException
from uw_gws.models import GroupMember
import xml.etree.ElementTree as ET
import json
import re


class ExtractUpdate(Extract):
    def parse(self, content_type, body):
        # normalize 'update-members' event
        if content_type == 'xml':
            rx = re.compile(r'^(<.*>)[^>]*$')
            root = ET.fromstring(rx.sub(r'\g<1>', body))
            event = GroupEvent(group_id=root.findall('./name')[0].text,
                               reg_id=root.findall('./regid')[0].text)
            event.add_members = [
                GroupMember(
                    name=m.text, member_type=m.attrib['type'])
                for m in root.findall('./add-members/add-member')
            ]
            event.delete_members = [
                GroupMember(
                    name=m.text, member_type=m.attrib['type'])
                for m in root.findall('./delete-members/delete-member')
            ]
            return event
        elif content_type == 'json':
            rx = re.compile(r'[^{]*({.*})[^}]*')
            return json.loads(rx.sub(r'\g<1>', body))

        raise ExtractException('Unknown event content-type: %s' % content_type)


class ExtractDelete(Extract):
    def parse(self, content_type, body):
        # body contains group identity information
        # normalize 'delete-group' event
        if content_type == 'xml':
            rx = re.compile(r'^(<.*>)[^>]*$')
            root = ET.fromstring(rx.sub(r'\g<1>', body))
            return GroupEvent(group_id=root.findall('./name')[0].text,
                              reg_id=root.findall('./regid')[0].text)
        elif content_type == 'json':
            rx = re.compile(r'[^{]*({.*})[^}]*')
            return json.loads(rx.sub(r'\g<1>', body))

        raise ExtractException('Unknown delete event content-type: %s' % (
            content_type))


class ExtractChange(Extract):
    def parse(self, content_type, body):
        # body contains old and new subject names (id)
        # normalize 'change-subject-name' event
        if content_type == 'xml':
            rx = re.compile(r'^(<.*>)[^>]*$')
            root = ET.fromstring(rx.sub(r'\g<1>', body))
            return GroupRename(
                old_name=root.findall('./subject/old-name')[0].text,
                new_name=root.findall('./subject/new-name')[0].text)
        elif content_type == 'json':
            rx = re.compile(r'[^{]*({.*})[^}]*')
            return json.loads(rx.sub(r'\g<1>', body))

        raise ExtractException('Unknown delete event content-type: %s' % (
            content_type))
