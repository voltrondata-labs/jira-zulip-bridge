#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import datetime
import os
import pytz
import re
import time
import zulip

# JIRA REST API URL to query
JIRA_URL = ("https://issues.apache.org/jira/rest/api/2/search?"
            "jql=project=%s%%20AND%%20updated%%3E=-%dm&"
            "expand=changelog&fields=changelog,summary,creator,description")

JIRA_USERNAME = os.environ['APACHE_JIRA_USERNAME']
JIRA_PASS = os.environ['APACHE_JIRA_PASSWORD']
DESCRIPTION_LIMIT = 10000
OLD_MESSAGES_LOOKBACK = 100
JIRA_API_LOOKBACK_MINUTES = 15
CHANGE_IGNORED_FIELDS = {'WorklogId', 'timespent', 'RemoteIssueLink'}


class ZulipJiraBot:

    def __init__(self, site, email, api_key, jira_project, stream):
        self.site = site
        self.email = email
        self.api_key = api_key
        self.jira_project = jira_project
        self.stream = stream

        self.jira_rest_url = JIRA_URL % (self.jira_project,
                                         JIRA_API_LOOKBACK_MINUTES)

        self.client = zulip.Client(email=self.email, api_key=self.api_key,
                                   site=self.site)

        # Keep a set of hash((topic, content)) values so we don't send
        # duplicate messages to the stream
        self.prior_content = []
        self.prior_event_ids = set()

        # Change event ids we've already processed in this process
        self.processed_change_ids = set()

        self.last_new_ticket = '{}-00000'.format(self.jira_project)

        self._get_recent_messages()

    def _get_recent_messages(self):
        # Add recent messages to our content hashes so we don't send the same
        # thing multiple times if the bot is restarted
        request = {
            'anchor': 'newest',
            'num_before': OLD_MESSAGES_LOOKBACK,
            'num_after': 0,
            'narrow': [{'operator': 'sender', 'operand': self.email},
                       {'operator': 'stream', 'operand': self.stream}],
        }
        result = self.client.get_messages(request)

        for message in result['messages']:
            content = message['content']
            m = re.findall(r'event_id: ([\d]+)', content)
            if len(m) == 1:
                try:
                    self.prior_event_ids.add(int(m[0]))
                except ValueError:
                    pass

    def _send_message(self, event_id, topic, content):
        if event_id in self.prior_event_ids:
            print("Skipping message already sent to stream")
            return

        request = {
            "type": "stream",
            "to": self.stream,
            "topic": topic,
            "content": content
        }

        import pprint
        pprint.pprint(request)

        import pdb
        pdb.set_trace()

        result = self.client.send_message(request)
        self.prior_event_ids.add(event_id)
        return result

    def process_latest(self):
        events = self._get_latest_jira_events()

        now = datetime.datetime.now(pytz.utc)
        for issue in events['issues']:
            for entry in issue['changelog']['histories']:
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
        issue_id = int(issue['id'])
        if issue_id in self.prior_event_ids:
            print("Skipping new issue with id {}".format(issue_id))
            return

        fields = issue['fields']

        author = fields['creator']['displayName']

        if 'assignee' in fields:
            assignee = fields['assignee']['displayName']
        else:
            assignee = 'UNASSIGNED'

        key = issue['key']
        title = fields['summary']
        desc = fields.get('description')
        if len(desc) > DESCRIPTION_LIMIT:
            desc = desc[:DESCRIPTION_LIMIT] + "..."

        prefixed_title = '{}: {}'.format(key, title)
        topic = prefixed_title

        # TODO: Priority does not seem to be returned by the JIRA API
        # priority = fields['priority']
        # * **Priority**: {}

        content = """{} created {}:

* **Assignee**: {}

{} (event_id: {})""".format(author, _issue_markdown_link(prefixed_title, key),
                            assignee, desc, int(issue['id']))
        return self._send_message(issue_id, topic, content)

    def send_ticket_change_event(self, issue, change):
        """ Parse changelog and format it for humans """
        author = change['author']['displayName']
        title = issue['fields']['summary']
        key = issue['key']

        change_id = int(change['id'])
        if change_id in self.prior_event_ids:
            print("Skipping change with id {}".format(change_id))
            return

        prefixed_title = '{}: {}'.format(key, title)
        topic = prefixed_title

        content = """{} updated {}:

{}

(event_id: {})"""

        change_lines = []
        for item in change['items']:
            field = item.get('field')

            if field in CHANGE_IGNORED_FIELDS:
                continue

            from_string = item.get('fromString')
            to_string = item.get('toString')

            to_string = to_string or "Unresolved"
            if from_string is None:
                line = '* Changed {} to **{}**'.format(field, to_string)
            else:
                line = ('* Changed {} from **{}** to **{}**'
                        .format(field, from_string, to_string))

            change_lines.append(line)

        if len(change_lines) == 0:
            print("Ignoring change id {}".format(change_id))
            return

        formatted_content = content.format(
            author, _issue_markdown_link(prefixed_title, key),
            '\n'.join(change_lines), int(change['id'])
        )
        return self._send_message(change_id, topic, formatted_content)


def _issue_markdown_link(title, key):
    return ('[{}](https://issues.apache.org/jira/browse/{})'
            .format(title, key))


if __name__ == '__main__':
    EMAIL = 'helper-bot@zulipchat.com'
    API_KEY = os.environ['ZULIP_JIRA_API_KEY']
    PROJECT = 'ARROW'
    STREAM = 'jira'
    ZULIP_SITE = 'https://ursalabs.zulipchat.com'

    bot = ZulipJiraBot(ZULIP_SITE, EMAIL, API_KEY, PROJECT, STREAM)

    while True:
        bot.process_latest()
        time.sleep(60)
