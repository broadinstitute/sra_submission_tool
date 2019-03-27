""" jdewar 05-2013 """

import hashlib
import hmac
import json
import urllib

import shaker.config
from shaker.http.action import Action
from shaker.http.json_connector import JSONConnector


class ManhattanConnector(JSONConnector):
    """
    Support for access to Manhattan API using HMAC authentication.  
    Credit to Andrew Roberts for the auth stuff, it was mostly stolen
    from his example code.

    In addition to general get() and post() methods that support arbitrary
    requests to the Manhattan API, which we inherit from JSONConnector,
    we have some convenience methods for calling specific API endpoints.

    URL's provided to this class should be of the following form:
        initiatives/62
    """

    def __init__(self, username, api_key, base_url=None, 
                 hide_calhoun_initiative=True):
        """ Set up an instance to connect to the Manhattan API using the
        given username and api key. base_url, if provided, should specify
        a REST version.  For example:

        http://manhattan.broadinstitute.org/rest/v1/
        """
        
        config = shaker.config.load('.angosturarc', __file__)
        super(ManhattanConnector, self).__init__(
            base_url if base_url else config['DEFAULT_MANH_URL']
        )

        self.username = username
        self.api_key = api_key
        self.hide_calhoun_initiative = hide_calhoun_initiative

    def _make_url(self, base, params=None):
        url = base
        if not params:
            params = []
        if not self.hide_calhoun_initiative:
            params.append(('hide_internal', False))
        if params:
            url = "%s?%s" % (base, urllib.urlencode(params, True))
        return url

    def get_initiative(self, initiative_id):
        url = self._make_url("initiatives/%s" % initiative_id)
        return self.get(url)

    def get_specimen(self, specimen_id):
        url = self._make_url("specimens/%s" % specimen_id)
        return self.get(url)

    def get_deliverable(self, deliverable_id):
        """ Get a deliverable by its unique deliverable id. """
        url = self._make_url("deliverables/%s" % deliverable_id)
        return self.get(url)
    
    def get_specimens_for_initiative(self, initiative_id):
        """Get all specimens for initiatives.  Useful for
        assembly pipeline project management."""
        return self.search_specimens(initiative_id, None, None)

    def search_specimens(self, initiative_id=None, specimen_name=None, 
                                                ref_key=None, ref_value=None):
        """ Return a list of specimens matching the given criteria.  Any, but
        not all, of the arguments can be null. 

        -- initiative_id: Return only specimens in this initiative
        -- specimen_name: Return only specimens whose identifiers 
        (aka collaborator sample ids) match this one
        -- ref_key and ref_value: The given key and value can either be single 
        items, or lists of values.  If key and value lists are provided, they 
        should be of the same length and should represent several key/value 
        pairs.
        """

        if not initiative_id and not specimen_name and not ref_value:
            raise Exception("Inputs are not sufficient to search specimens.")

        base_url = ''
        if initiative_id:
            base_url += 'initiatives/%s' % initiative_id
        base_url += '/specimens'

        params = []
        if specimen_name:
            params.append(('specimen_id', specimen_name))
        if ref_key:
            params.append(('reference_key', ref_key))
        if ref_value:
            params.append(('reference_value', ref_value))

        url = self._make_url(base_url, params)
        return self.get(url)

    def is_specimen_editable(self, specimen_id, username):
        """ Return True if this user is authorized to edit this specimen, 
        False otherwise. 
        """
        url = self._make_url("specimens/%s/authorization/%s" % 
                                                        (specimen_id, username))
        json = self.get(url)
        return json['update']

    def get_specimen_deliverables_by_ref(self, initiative_id, specimen_id, 
                                           ref_key, ref_value=None, scope=None):
        """ Get the deliverables with the given initiative_id,
        specimen_id, scope, and reference key or reference key/value pairs.
        If passing initiative_id, then specimen_id must be passed to ensure proper
        Manhattan API url.  The given key, value, and scope can either be single 
        items, or lists of values.  If key and value lists are provided, they 
        should be of the same length and should represent several key/value 
        pairs. """

        base_url = ""
        if initiative_id and not specimen_id:
            raise Exception("Must provide a specimen_id along with initiative_id")
        
        if initiative_id:
            base_url += "initiatives/%s" % (initiative_id)
        if specimen_id:
            base_url += "/specimens/%s" % (specimen_id)
        base_url += "/deliverables"

        params = []
        if ref_key:
            params.append(('reference_key', ref_key))
        if ref_value:
            params.append(('reference_value', ref_value))
        if scope:
            params.append(('scope', scope))

        url = self._make_url(base_url, params)

        return self.get(url)

    def get_deliverables_by_ref(self, ref_key, ref_value, scope=None):
        """ Get the deliverables with the given reference key/value pair and,
        optionally, scope. The given key, value, and scope can either be single 
        items, or lists of values. If key and value lists are provided, they 
        should be of the same length and should represent several key/value 
        pairs. """
        return self.get_specimen_deliverables_by_ref(
            None, None, ref_key, ref_value, scope
        )

    def get_deliverables_by_scope(self, initiative_id, specimen_id, scope):
        """ Get the deliverables with the given scope or scopes from the given 
        specimen and initiative. 'scope' can be either a single string or a 
        list of such. """
        base_url = ""
        if initiative_id:
            base_url = "/initiatives/%s" % initiative_id
        base_url += "/specimens/%s/deliverables" % specimen_id

        params = [('scope', scope)]

        url = self._make_url(base_url, params)
        return self.get(url)

    def get_downstream_deliverables(self, specimen_id, deliverable_ids):
        """
        Given a deliverable id or a list of such, query for all downstream 
        deliverables.
        """
        params = [('downstream_of', deliverable_ids)]
        url = self._make_url("specimens/%s/deliverables" % specimen_id, params)
        return self.get(url)

    def trigger_event(self, deliverable_id, event_key, references):
        url = "deliverables/%s/events" % deliverable_id
        payload = {
            'key': event_key,
            'references': references
        }
        self.post(self._make_url(url), json.dumps(payload))

    def create_specimen(self, initiative_id, specimen_id, created_by, 
                        genus=None, species=None, strain=None, 
                        workflow_name=None):
        """ Create a new specimen record. """
        url = "initiatives/%s/specimens/create" % initiative_id
        payload = {
            'specimen_id': specimen_id,
            'created_by': created_by
        }
        if genus:
            payload['genus'] = genus
        if species:
            payload['species'] = species
        if strain:
            payload['strain'] = strain
        if workflow_name:
            payload['workflow_name'] = workflow_name
        return self.post(self._make_url(url), json.dumps(payload))

    def update_specimen(self, initiative_id, specimen_id, ref_key, ref_value=None, scope=None):

        url = self._make_url("specimens/%s" % specimen_id)
        return self._do_request(url, "Put", self.get_specimen(specimen_id))

    def _make_headers(self, url, http_method):
        """ Create an authorization header for a request to this URL. 
        Given URL should be the complete URL the request will be sent to,
        including protocol. """                     

        hmac = "%s:%s" % (
            self.username, self._create_hmac(url, http_method)
        )
        headers = { 'Authorization': hmac }
        if http_method == Action.POST:
            headers['Content-type'] = 'application/json'
        return headers

    def _create_hmac(self, url, http_method):
        digest = hmac.new(str(self.api_key), digestmod=hashlib.sha1)

        digest.update(url.lower())
        digest.update(http_method.lower())
        digest.update(self.username)

        return digest.hexdigest()

        

