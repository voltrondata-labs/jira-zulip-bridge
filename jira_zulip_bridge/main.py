#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import datetime
import os
import pytz
import zulip

# JIRA REST API URL to query
JIRA_URL = ("https://issues.apache.org/jira/rest/api/2/search?"
            "jql=project=%s%%20AND%%20updated%%3E=-15m&"
            "expand=changelog&fields=changelog,summary,creator,description")

EMAIL = 'arrow-jira-bot@ursalabs.zulipchat.com'
JIRA_USERNAME = os.environ['APACHE_JIRA_USERNAME']
JIRA_PASS = os.environ['APACHE_JIRA_PASSWORD']
API_KEY = os.environ['ZULIP_JIRA_API_KEY']
DESCRIPTION_LIMIT = 10000


class ZulipJiraBot:

    def __init__(self, email, api_key, jira_project, stream='test-jira'):
        self.email = email
        self.api_key = api_key
        self.jira_project = jira_project
        self.stream = stream

        self.jira_rest_url = self.JIRA_URL % self.jira_project

        self.client = zulip.Client(email=self.email, api_key=self.api_key)

        # Keep a set of hash((topic, content)) values so we don't send
        # duplicate messages to the stream
        self.content_hashes = set()

        # Change event ids we've already processed in this process
        self.processed_change_ids = set()

        self.last_new_ticket = '{}-00000'.format(self.jira_project)

        self._get_recent_messages()

    def _get_recent_messages(self):
        # Add recent messages to our content hashes so we don't send the same
        # thing multiple times if the bot is restarted
        request = {
            'anchor': 'newest',
            'num_before': 1000,
            'num_after': 0,
            'narrow': [{'operator': 'sender', 'operand': self.email},
                       {'operator': 'stream', 'operand': self.stream}],
        }
        result = self.client.get_messages(request)

        for message in result['messages']:
            self._observe_message(message['subject'], message['content'])

    def _observe_message(self, topic, content):
        key = hash((topic, content))
        self.content_hashes.add(key)

    def _already_sent_message(self, topic, content):
        key = hash((topic, content))
        return key in self.content_hashes

    def _send_message(self, topic, content):
        if self._already_sent_message(topic, content):
            return

        request = {
            "type": "stream",
            "to": self.stream,
            "topic": topic,
            "content": content
        }

        import pprint
        pprint.pprint(request)

        # result = self.client.send_message(request)
        self._observe_message(topic, content)
        # return result

    def process_latest(self):
        events = self._get_latest_jira_events()

        now = datetime.datetime.now(pytz.utc)
        for issue in events['issues']:
            for entry in issue['changelog']['histories']:
                change_id = int(entry['id'])
                if change_id in self.processed_change_ids:
                    continue

                self.processed_change_ids.add(change_id)

                when = datetime.datetime.strptime(entry['created'],
                                                  '%Y-%m-%dT%H:%M:%S.%f%z')
                CHANGE_LOOKBACK_SECONDS = 900

                if (now - when).seconds > CHANGE_LOOKBACK_SECONDS:
                    continue

                self.send_ticket_change_event(issue, entry)

            # Newly created issue?
            if (len(issue['changelog']['histories']) == 0 and
                    issue['key'] > self.last_new_ticket):
                self.last_new_ticket = issue['key']
                self.send_new_ticket(issue)

    def _get_latest_jira_events(self):
        try:
            return requests.get(self.jira_rest_url,
                                auth=(JIRA_USERNAME, JIRA_PASS)).json()
        except Exception:
            print("Could not contact JIRA, faking empty reply")
            return {
                'issues': []
            }

    def send_new_ticket(self, issue):
        author = issue['fields']['creator']['displayName']
        assignee = issue['fields']['assignee']['displayName']
        key = issue['key']
        title = issue['fields']['summary']
        desc = issue['fields'].get('description')
        if len(desc) > DESCRIPTION_LIMIT:
            desc = desc[:DESCRIPTION_LIMIT] + "..."

        priority = 'ADDME'

        topic = title
        content = """{} created {}:
* **Priority**: {}
* **Assignee**: {}

{}""".format(author, _issue_markdown_link(title, key), priority,
             assignee, desc)
        return self._send_message(topic, content)

    def send_ticket_change_event(self, entry):
        """ Parse changelog and format it for humans """
        author = entry['change']['author']['displayName']
        title = entry['summary']
        key = entry['key']

        topic = title
        content = """{} created {}:
{}"""

        change_lines = []
        for item in entry['change']['items']:
            field = item.get('field')
            from_string = item.get('fromString')
            to_string = item.get('toString')

            to_string = to_string or "Unresolved"
            if from_string is None:
                line = 'Changed {} to **{}**'.format(field, to_string)
            else:
                line = ('Changed {} from **{}** to **{}**'
                        .format(field, from_string, to_string))

            change_lines.append(line)

        formatted_content = content.format(
            author, _issue_markdown_link(title, key),
            change_lines.join('\n')
        )
        return self._send_message(topic, formatted_content)


def _issue_markdown_link(title, key):
    return ('[{}](https://issues.apache.org/jira/browse/{})'
            .format(title, key))
