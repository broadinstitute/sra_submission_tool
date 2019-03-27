import sys
import SRA_submission_tool.manhattan_connector as manhattan_connector
__author__ = 'Amr Abouelleil'


class ManhattanConnector(object):
    def connect(self, user, api_key, url):
        manhattan_connection = manhattan_connector.ManhattanConnector(user, api_key, url)
        return manhattan_connection


class UpdateService(object):
        def __init__(self, g_number):
            self.mc = ManhattanConnector().connect("gaag",
                                                   "aa42890cdfef90e807f1099b736a2a4a62ee64c9",
                                                   "http://ale:8282/rest/v1/")
            self.mm = ManhattanMapper(self.mc, g_number)

        def update_deliverable(self, initiative_id, scope, event_key, references, protocol=None, created_by=None):
            deliverables = self.mc.get_deliverables_by_scope(initiative_id,
                                                               self.mm.retrieve_manhattan_specimen_id(),
                                                               scope)
            deliverable_id = deliverables[0]['id']
            if protocol is not None:
                if created_by is None:
                    for deliverable in deliverables:
                        if deliverable['protocol'] == protocol:
                            deliverable_id = deliverable['id']
                            break
                else:
                    for deliverable in deliverables:
                        if deliverable['protocol'] == protocol \
                                and deliverable['events'][0]['created_by_id'] == created_by:
                            deliverable_id = deliverable['id']
                            break

            print("deliverable_id:" + deliverable_id)
            print("scope:" + scope)
            print("manhattan_spec_id:" + self.mm.retrieve_manhattan_specimen_id())
            print("event key:" + event_key)

            try:
                self.mc.trigger_event(deliverable_id=deliverable_id,
                                      event_key=event_key,
                                      references=references)
            except Exception as e:
                print(e.message)
            return deliverable_id


class RetrievalService(object):
        def __init__(self, g_number):
            self.mc = ManhattanConnector().connect("gaag",
                                                   "aa42890cdfef90e807f1099b736a2a4a62ee64c9",
                                                   "http://ale:8282/rest/v1/")
            self.g_number = g_number
            self.mm = ManhattanMapper(self.mc, g_number)

        def retrieve_deliverables(self, key_list):
            init_id = self.mm.retrieve_initiative_id()
            spec_id = self.mm.retrieve_manhattan_specimen_id()
            results_dict = dict()
            for key in key_list:
                value = self.mm.get_value_with_key(init_id, spec_id, key)
                results_dict[key] = value
            return results_dict

        def retrieve_initiative_data(self):
            return self.mm.retrieve_initiative_data()


class ManhattanMapper(object):
    """
    A data mapper that simplifies objects returned using manhattan connection to connect to manhattan for the domain
    objects.
    """
    def __init__(self, manhattan_connection, g_number):
        self.g_number = g_number
        self.manhattan_connection = manhattan_connection
        self.specimens = self.retrieve_specimen_record_with_g_number(self.g_number)
        self.init_id = self.retrieve_initiative_id()
        self.manhattan_specimen_id = self.specimens[0]['id']

    def retrieve_specimen_record_with_g_number(self, g_number):
        try:
            return self.manhattan_connection.search_specimens(ref_key="squid_project_number", ref_value=g_number)
        except IndexError:
            print(g_number, "does not have a specimen record.")
            sys.exit(-1)

    def retrieve_specimen_id(self):
        try:
            return self.specimens[0]['specimen_id']
        except IndexError:
            print(self.g_number, "does not have a specimen ID.")

    def retrieve_strain(self):
        try:
            return self.specimens[0]['strain']
        except IndexError:
            print(self.g_number, "does not have a strain.")
            sys.exit(-1)

    def retrieve_initiative_data(self):
        return self.manhattan_connection.get_initiative(self.retrieve_initiative_id())

    def retrieve_initiative_id(self):
        try:
            return int(self.specimens[0]['initiative_ids'][0])
        except IndexError:
            print(self.g_number, "does not have a an initiative ID.")
            sys.exit(-1)

    def retrieve_manhattan_specimen_id(self):
        try:
            return self.specimens[0]['id']
        except IndexError:
            print(self.g_number, "does not have a manhattan specimen ID.")
            sys.exit(-1)

    def findManhattanData(self, data_dict, key):
        """
        A function for finding a particular value in multi-level manhattan API nested dict/list structure.
        :param data_dict: An events dictionary to be passed to manhattan.
        :param field_name: The name of the desired field to be returned.
        :return:
        """
        for data in data_dict:
            for val in data.values():
                if type(val) == type(data.values()):
                    for subdict in val:
                        for k, v in subdict.items():
                            if key in v:
                                return subdict["value"]

    def get_value_with_key(self, init_id, manhattan_specimen_id, key):
        deliverable = self.manhattan_connection.get_specimen_deliverables_by_ref(initiative_id=init_id,
                                                                                 specimen_id=manhattan_specimen_id,
                                                                                 ref_key=key)
        try:
            return self.findManhattanData(deliverable[0]['events'], key)
        except IndexError:
            print(key, "is not a valid key.")
            sys.exit(-1)

    def fill_object_fields_request(self, init_id, manhattan_specimen_id):
        fields_dict = {"specimen_id": self.retrieve_specimen_id(),
                       "initiative_ids": self.init_id,
                       "strain": self.retrieve_strain(),
                       "ncbi_locus_tag_prefix": self.get_value_with_key(init_id,
                                                                        manhattan_specimen_id, "ncbi_locus_tag_prefix"),
                       "ncbi_bioproject_id": self.get_value_with_key(init_id,
                                                                     manhattan_specimen_id, "ncbi_bioproject_id"),
                       "ncbi_biosample_id": self.get_value_with_key(init_id,
                                                                    manhattan_specimen_id, "ncbi_biosample_id"),
                       "ncbi_taxon_id": self.get_value_with_key(init_id,
                                                                manhattan_specimen_id, "ncbi_taxon_id"),
                       "ale_assembly_id": self.get_value_with_key(init_id,
                                                                  manhattan_specimen_id, "ale_assembly_id")
                       }
        return fields_dict
