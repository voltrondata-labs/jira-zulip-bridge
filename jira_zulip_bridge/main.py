#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import jira
import os
import pytz
import re
import requests
import sys
import time
import traceback
import zulip

# JIRA REST API URL to query
JIRA_URL = ("https://issues.apache.org/jira/rest/api/2/search?"
            "jql=project=%s%%20AND%%20updated%%3E=-%dm&"
            "expand=changelog&"
            "fields=changelog,summary,creator,description")

JIRA_API_BASE = "https://issues.apache.org/jira"
JIRA_USERNAME = os.environ['APACHE_JIRA_USERNAME']
JIRA_PASS = os.environ['APACHE_JIRA_PASSWORD']
DESCRIPTION_LIMIT = 10000
OLD_MESSAGES_LOOKBACK = 100
JIRA_API_LOOKBACK_MINUTES = 1000
CHANGE_IGNORED_FIELDS = {'WorklogId', 'timespent', 'RemoteIssueLink',
                         'timeestimate'}
CHANGE_LOOKBACK_SECONDS = JIRA_API_LOOKBACK_MINUTES * 60


class JiraPython:

    def __init__(self):
        try:
            self.jira = jira.client.JIRA(options={'server': JIRA_API_BASE},
                                        basic_auth=(JIRA_USERNAME, JIRA_PASS))
        except jira.exceptions.JIRAError as e:
            if "CAPTCHA_CHALLENGE" in e.text:
                print("")
                print("It looks like you need to answer a captcha challenge "
                      "for this account (probably due to a login attempt "
                      "with an incorrect password). Please log in at "
                      "https://issues.apache.org/jira and complete the "
                      "captcha before running this tool again.")
                print("Exiting.")
                sys.exit(1)
            raise e

    def maybe_set_in_progress(self, jira_id):
        issue = self.jira.issue(jira_id)
        assignee = issue.fields.assignee
        if assignee is None:
            # No assignee, do not set to in progress
            return

        fields = issue.fields
        cur_status = fields.status.name

        # If the status is something other than Open, do not do any transitions
        if cur_status != 'Open':
            return

        resolve = [x for x in self.jira.transitions(jira_id)
                   if x['name'] == "Start Progress"][0]
        self.jira.transition_issue(jira_id, resolve["id"])

        # Change the assignee back to whatever it was originally
        self.jira.assign_issue(issue, assignee.key)


class ZulipJiraBot:

    def __init__(self, site, email, api_key, jira_project, stream):
        self.site = site
        self.email = email
        self.api_key = api_key
        self.jira_project = jira_project
        self.stream = stream

        self.jira_rest_url = JIRA_URL % (self.jira_project,
                                         JIRA_API_LOOKBACK_MINUTES)

        self.jira_python = JiraPython()

        self.client = zulip.Client(email=self.email, api_key=self.api_key,
                                   site=self.site)

        # Keep a set of hash((topic, content)) values so we don't send
        # duplicate messages to the stream
        self.prior_content = []
        self.prior_event_ids = set()

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

    def _get_issue_comments(self, key):
        url = ("https://issues.apache.org/jira/rest/api/2/issue/"
               "{}/comment".format(key))
        return self._make_jira_request(url)

    def _send_message(self, event_id, topic, content):
        if event_id in self.prior_event_ids:
            return

        request = {
            "type": "stream",
            "to": self.stream,
            "topic": topic,
            "content": content
        }

        import pprint
        pprint.pprint(request)

        result = self.client.send_message(request)
        self.prior_event_ids.add(event_id)
        return result

    def _is_recent_event(self, event_timestamp):
        now = datetime.datetime.now(pytz.utc)
        when = _parse_jira_timestamp(event_timestamp)
        return (now - when).total_seconds() < CHANGE_LOOKBACK_SECONDS

    def process_latest(self):
        events = self._make_jira_request(self.jira_rest_url)

        for issue in events['issues']:
            issue_comments = self._get_issue_comments(issue['key'])
            for comment in issue_comments['comments']:
                if not self._is_recent_event(comment['updated']):
                    continue
                self.send_comment_event(issue, comment)

            for entry in issue['changelog']['histories']:
                if not self._is_recent_event(entry['created']):
                    continue
                self.send_ticket_change_event(issue, entry)

            # Newly created issue?
            if (len(issue['changelog']['histories']) == 0 and
                    issue['key'] > self.last_new_ticket):
                self.last_new_ticket = issue['key']
                self.send_new_ticket(issue)

    def _make_jira_request(self, request):
        try:
            return requests.get(request,
                                auth=(JIRA_USERNAME, JIRA_PASS)).json()
        except Exception:
            print("Could not contact JIRA, faking empty reply")
            return {
                'issues': []
            }

    def send_new_ticket(self, issue):
        issue_id = int(issue['id'])
        if issue_id in self.prior_event_ids:
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
            return

        prefixed_title = '{}: {}'.format(key, title)
        topic = prefixed_title

        content = """{} updated {}:

{}

(event_id: {})"""

        field_defaults = {
            'resolution': 'Unresolved',
            'labels': '(no labels)'
        }

        change_lines = []
        for item in change['items']:
            field = item.get('field')

            if field in CHANGE_IGNORED_FIELDS:
                continue

            from_string = item.get('fromString')
            to_string = item.get('toString')

            to_string = to_string or field_defaults.get(field, "(empty)")

            if field == 'labels' and 'pull-request-available' in to_string:
                # When a pull request is opened, set the status of the PR to In
                # Progress if it has an assignee
                self.jira_python.maybe_set_in_progress(key)

            if from_string is None:
                line = '* Changed {} to **{}**'.format(field, to_string)
            else:
                line = ('* Changed {} from **{}** to **{}**'
                        .format(field, from_string, to_string))

            change_lines.append(line)

        if len(change_lines) == 0:
            print("Ignoring change id {}".format(change_id))
            self.prior_event_ids.add(change_id)
            return

        formatted_content = content.format(
            author, _issue_markdown_link(prefixed_title, key),
            '\n'.join(change_lines), change_id
        )
        return self._send_message(change_id, topic, formatted_content)

    def send_comment_event(self, issue, comment):
        author = comment['author']['displayName']
        title = issue['fields']['summary']
        key = issue['key']

        comment_id = int(comment['id'])
        if comment_id in self.prior_event_ids:
            return

        prefixed_title = '{}: {}'.format(key, title)
        topic = prefixed_title

        if comment['updated'] != comment['created']:
            action = 'updated comment'
        else:
            action = 'posted new comment'

        content = """{} {} on {}:

When: {}

{}

(event_id: {})"""

        formatted_content = content.format(
            author, action, _issue_markdown_link(prefixed_title, key),
            comment['updated'], comment['body'], comment_id
        )
        return self._send_message(comment_id, topic, formatted_content)


def _parse_jira_timestamp(stamp):
    return datetime.datetime.strptime(stamp, '%Y-%m-%dT%H:%M:%S.%f%z')


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
        now = datetime.datetime.now(pytz.utc)
        print("{}: Processing latest events"
              .format(now.strftime("%Y-%m-%d %H:%M:%S %Z")))
        try:
            bot.process_latest()
        except Exception:
            traceback.print_exc()

        time.sleep(5)
