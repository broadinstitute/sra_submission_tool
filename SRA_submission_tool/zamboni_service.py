__author__ = 'Paul Cao, Amr Abouelleil'

import json
import os
import stomp
import logging
logging.basicConfig(level=logging.DEBUG)


class Zamboni(object):
    """ Module to interact with zamboni pipeline workflow manager. Example usage:
        zamboni = Zamboni()
        zamboni.start_workflow('EmailGAAGWorkflow', {'notificationEmailAddresses': 'bruce@broadinstitute.org'})
        zamboni.close()

        Generated json payload:
        {"Zamboni":{"workflow":"EmailGAAGWorkflow"},"workflow":{"notificationEmailAddresses":"bruce@broadinstitute.org"}}
    """

    def __init__(self, host_port=('ale1', 61613), queue='broad.zamboni.gaag'):
    # def __init__(self, host_port=('ale-staging', 61613), queue='broad.zamboni.gaagdev'):
        self.host_port = host_port
        self.queue = queue
        self.connection = stomp.Connection([host_port])
        self.connection.start()
        self.connection.connect()

    def send(self, action, payload, sender=None):
        assert action in ('start', 'stop', 'reconsider')
        if not sender:
            sender = os.getenv('USER') or 'unknown'
        self.connection.send(headers = {'action': action,
                                        'payload': payload,
                                        'sender': sender},
                             destination = '/queue/' + self.queue)

    def workflow_payload(self, workflow, argdict):
        workflow = {"Zamboni": {"workflow": workflow}, "workflow": argdict}
        return json.dumps(workflow)

    def start_workflow(self, workflow, argdict):
        self.send('start', self.workflow_payload(workflow, argdict))

    def stop_workflow(self, workflow):
        self.send('stop', self.workflow_payload(workflow, {}))

    def restart_wokflow(self):
        self.send('reconsider', self.workflow_id)

    def disconnect(self):
        self.connection.disconnect()

    def close(self):
        self.disconnect()
