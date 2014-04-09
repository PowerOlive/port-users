from __future__ import division

from datetime import datetime
import json
import logging
import cgi

import boto
import boto.sqs
from boto.sqs.jsonmessage import JSONMessage

from google.appengine.runtime import DeadlineExceededError
from google.appengine.datastore.datastore_query import Cursor
from google.appengine.ext import ndb
from google.appengine.api import taskqueue
import webapp2


class TriggerInit(webapp2.RequestHandler):
    def get(self):
        taskqueue.add(url='/init_ranks')
        self.response.write("<html><body>OK.</body></html>")

class TriggerRank(webapp2.RequestHandler):
    def get(self):
        taskqueue.add(url='/rank_users')
        self.response.write("<html><body>OK.</body></html>")

class TriggerSend(webapp2.RequestHandler):
    def get(self):
        taskqueue.add(url='/send_request')
        self.response.write("<html><body>OK.</body></html>")

class InitRanks(webapp2.RequestHandler):
    def post(self):
        best = Ranking()
        now = datetime.now()
        for user in (LanternUser.query(LanternUser.everSignedIn == True)
                                .iter(projection=[LanternUser.bytesProxied,
                                                  LanternUser.created,
                                                  LanternUser.directBytes,
                                                  LanternUser.requestsProxied])):
            set_user_age(user, now)
            for attr in ["age",
                         "bytesProxied",
                         "directBytes",
                         "requestsProxied"]:
                user_score = getattr(user, attr, 0)
                if user_score > getattr(best, attr):
                    setattr(best, attr, user_score)
        best.put()
        self.response.write("<html><body>OK.</body></html>")

def set_user_age(user, now):
    user.age = int((now-user.created).total_seconds())
    user.age *= user.age

class RankUsers(webapp2.RequestHandler):
    def post(self):
        best, = Ranking.query().fetch(1)
        now = datetime.now()
        cursor_str = self.request.get('cursor')
        cursor = (Cursor(urlsafe=cursor_str) if cursor_str
                  else None)
        logging.info("cursor_str: %r" % cursor_str)
        try:
            while True:
                users, cursor, more = (
                        LanternUser.query(LanternUser.everSignedIn == True)
                                   .fetch_page(10,
                                               start_cursor=cursor,
                                               projection=[LanternUser.bytesProxied,
                                                           LanternUser.created,
                                                           LanternUser.directBytes,
                                                           LanternUser.requestsProxied]))
                scores = []
                for user in users:
                    set_user_age(user, now)
                    score = sum(getattr(user, attr) / getattr(best, attr)
                                for attr in ["age",
                                             "bytesProxied",
                                             "directBytes",
                                             "requestsProxied"])
                    scores.append(UserScore(id=user.key.id(), score=score))
                if scores:
                    logging.info("Committing a batch of %s" % len(scores))
                    ndb.put_multi(scores)
                if not more:
                    logging.info("Done!")
                    break
        except DeadlineExceededError:
            cursor_str = cursor.urlsafe()
            logging.info("Deadline exceeded; rescheduling to %r." % cursor_str)
            task_queue.add(url='/rank_users',
                           params={'cursor': cursor_str})
        self.response.write("<html><body>OK.</body></html>")

class SendRequest(webapp2.RequestHandler):
    def post(self):
        users_future = (UserScore.query(UserScore.ported == False)
                                 .order(-UserScore.score)
                                 .fetch_async(500))
        # Include secrets as a submodule if anything grows out of this.
        aws_creds = {'aws_access_key_id': '<REDACTED>',
                     'aws_secret_access_key': '<REDACTED>'}
        sqs = boto.sqs.connect_to_region('ap-southeast-1', **aws_creds)
        q = sqs.get_queue("notify_lanternctrl1-2")
        q.set_message_class(JSONMessage)
        msg = JSONMessage()
        users = list(users_future.get_result())
        msg.set_body({'port-users': '\n'.join(u.key.id() for u in users)})
        q.write(msg)
        logging.info("Sent request.")
        for user in users:
            user.ported = True
        ndb.put_multi(users)
        logging.info("Marked users as ported.")

class LanternUser(ndb.Model):
    bytesProxied = ndb.IntegerProperty(default=0)
    created = ndb.DateTimeProperty(auto_now_add=True)
    directBytes = ndb.IntegerProperty(default=0)
    everSignedIn = ndb.BooleanProperty(default=False)
    requestsProxied = ndb.IntegerProperty(default=0)

class UserScore(ndb.Model):
    score = ndb.FloatProperty(default=0)
    ported = ndb.BooleanProperty(default=False)

class Ranking(ndb.Model):
    age = ndb.IntegerProperty(default=0)
    bytesProxied = ndb.IntegerProperty(default=0)
    directBytes = ndb.IntegerProperty(default=0)
    requestsProxied = ndb.IntegerProperty(default=0)


app = webapp2.WSGIApplication([('/trigger_init', TriggerInit),
                               ('/trigger_rank', TriggerRank),
                               ('/trigger_send', TriggerSend),
                               ('/init_ranks', InitRanks),
                               ('/rank_users', RankUsers),
                               ('/send_request', SendRequest)],
                              debug=True)
