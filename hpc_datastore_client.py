#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum
import requests
from hpc_datastore_description import HPCDatastoreDescription 

class Access(Enum):
    READ=1
    WRITE=2
    READ_WRITE=3

DS_PATH= '/datasets'

class HPCDatastoreClient(object):
	"""HPC Datastore client implementation in Python 3"""
	
	def __init__(self, server_url="http://localhost:9080", dataset_path=None, access_regime = Access.READ, credentials=None):
		"""Initializes the Data Store connection

		:type dataset_path: str
		:param dataset_path: Identity of the datastore (UUID)

		:type access_regime: Access
		:param access_regime: Access regime to the datastore - R/W/RW 

		:type server_url: str
		:param server_url: DataStore server URL, defaults to localhost and port 9080

		:type credentials: object
		:param credentials: Future access credentials, set to None for now
		"""
		
		self.server_url = server_url
		self.credentials = credentials
		self.access_regime = access_regime
		self.dataset_path= dataset_path
	
	def get_base_url(self):
		"""Returns full datastore URL"""
		return self.server_url + DS_PATH + '/' + self.dataset_path 
		
	def load_description(self):
		desc = requests.get(self.get_base_url())
		if desc is not None and int(desc.status_code / 100) == 2:
			self.ds_description = HPCDatastoreDescription(json_objects=desc.json());
	
	def __str__(self):
		out_s = ""
		for k in self.__dict__: 
			out_s += "%s: %s\n" % (k,self.__dict__[k])
		return out_s
