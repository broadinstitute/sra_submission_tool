#!/usr/bin/env python

import json
import os
import stomp
import logging
import sys

logging.basicConfig(level=logging.DEBUG)

class Zamboni:
    """ Module to interact with zamboni pipeline workflow manager. Example usage:
        zamboni = Zamboni()
        zamboni.start_workflow('EmailGAAGWorkflow', {'notificationEmailAddresses': 'bruce@broadinstitute.org'})
        zamboni.close()

        Generated json payload:
        {"Zamboni":{"workflow":"EmailGAAGWorkflow"},"workflow":{"notificationEmailAddresses":"bruce@broadinstitute.org"}}
    """

    def __init__(self, host_port = ('ale', 61613), queue='broad.zamboni.gaagprod'):
        self.host_port = host_port
        self.queue = queue
        self.connection = stomp.Connection([host_port])
        self.connection.start()
        self.connection.connect()

    def send(self, action, payload, sender = None):
        assert action in ('start', 'stop', 'reconsider')
        if not sender:
            sender = os.getenv('USER') or 'unknown'
        self.connection.send(headers = {'action': action,
                                        'payload': payload,
                                        'sender': sender},
                             destination = '/queue/' + self.queue
			     )

    def workflow_payload(self, workflow, argdict):
    	workflow = {"Zamboni":{"workflow":workflow}, "workflow":argdict}
	return json.dumps(workflow)

    def start_workflow(self, workflow, argdict):
        self.send('start', self.workflow_payload(workflow, argdict))
    
    def stop_workflow(self, workflow):
    	self.send('stop', self.workflow_payload(workflow, {}))
    
    def restart_workflow(self, workflow, workflow_id, sender = None):
 	if not sender:
		sender = os.getenv('USER') or 'unknown'
	self.connection.send(headers = {'action': 'stop',
                                        'payload':{"workflowId":workflow_id},				                'sender': sender},			                              destination = '/queue/' + self.queue)

    def disconnect(self):
        self.connection.disconnect()

    def close(self):
        self.disconnect()

#Example usage:
zamboni = Zamboni()
workflow_id = str(sys.argv[1])
if "-" in workflow_id:
    workflow_first = int(workflow_id.split('-')[0])
    workflow_last = int(workflow_id.split('-')[-1]) + 1
    for id in range(workflow_first, workflow_last):
        zamboni.restart_workflow("VesperProkWorkflow", id)
elif ',' in workflow_id:
    id_list = workflow_id.split(',')
    for id in id_list:
        zamboni.restart_workflow("VesperProkWorkflow", int(id))
else:
    zamboni.restart_workflow("VesperProkWorkflow", int(workflow_id))
zamboni.close()

print "Email sent"
