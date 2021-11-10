#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum
from hpc_ds_types import *
from hpc_ds_desc import HPCDatastoreDescription
from dataset_client import DatasetServerClient
from regserv_client import RegisterServiceClient
from time import time
import requests

class HPCDatastoreRepository(object):
	"""HPC Datastore Repository representation"""

	data_headers = {
		"json": { "Content-Type": "application/json" },
		"string": { "Content-Type": "text/plain; charset=utf-8" }
	}

	def __init__(self, server_url, dataset_path=None, credentials=None):
		"""Set up base URL of the repository to use
		:type dataset_path: str
		:param dataset_path: Identity of the datastore (UUID)

		:type server_url: str
		:param server_url: DataStore server URL, defaults to localhost:9080

		:type credentials: object
		:param credentials: Future access credentials, set to None for now
		"""
		self.server_url = server_url
		self.dataset_path = dataset_path
		self.credentials = credentials

	def assert_dataset_ready(self, dataset_path):
		if dataset_path is None:
			raise InvalidDatasetException(
			"No dataset path has been specified, repository cannot be accessed")

	def get_base_url(self, use_dataset=True):
		"""Returns full datastore URL"""
		retval = self.server_url + DS_PATH
		if use_dataset:
			self.assert_dataset_ready(self.dataset_path)
			retval += '/' + self.dataset_path
		return retval

	def create(self, datastore_description):
		"""Create repository and record the server URL"""
#		print("Request URL: %s" % self.get_base_url(False))
		self.assert_dataset_ready(self.dataset_path)
		desc = requests.post(self.get_base_url(False),
								data=datastore_description.to_json(),
								headers=self.data_headers["json"])

		if desc is not None and int(desc.status_code / 100) == 2:
			self.dataset_path = desc.text
		else:
			raise HPCRepositoryAccessException(
			"Dataset has not been created, HTTP error %i" % (desc.status_code))
		return None

	def retrieve(self):
		"""Load repository data"""
		desc = requests.get(self.get_base_url())
		if desc is not None and int(desc.status_code / 100) == 2:
			return desc.json()
		else:
			raise HPCRepositoryAccessException(
			"Dataset %s has not been found, HTTP error %i"
				% (self.dataset_path, desc.status_code))
		return None

	def delete(self, deleted_dataset_path): # =self.dataset_path):
		# Argument forced to rather not to prevent deleting used datasets
		self.assert_dataset_ready(deleted_dataset_path)
		desc = requests.delete(self.get_base_url(False) + '/' + deleted_dataset_path)
		if desc is not None and int(desc.status_code / 100) == 2:
			if deleted_dataset_path == self.dataset_path: self.dataset_path = None
			return True
		elif desc.status_code == 404:
			if deleted_dataset_path == self.dataset_path: self.dataset_path = None
#			return False
#		else:
#			raise HPCRepositoryAccessException(
#				"Dataset %s has not been deleted, HTTP error %i"
#				% (deleted_dataset_path, desc.status_code))
		return False

	def get_common_metadata(self):
		"""Get N5 metadata from repository"""
		self.assert_dataset_ready(self.dataset_path)
		result = requests.get(self.get_base_url() + "/common-metadata")
		if result is not None and int(result.status_code / 100) == 2:
			return result.text

	def set_common_metadata(self, metadata):
		"""Set N5 metadata to repository"""
		self.assert_dataset_ready(self.dataset_path)
		result = requests.post(self.get_base_url() + "/common-metadata",
								data=str(metadata), 
								headers=self.data_headers["string"])
		return result is not None and int(result.status_code / 100) == 2

	def add_channels(self, count):
		"""Add extra channels"""
		self.assert_dataset_ready(self.dataset_path)
		result = requests.post(self.get_base_url() + "/channels",
								data=str(count),
								headers=self.data_headers["json"])
		return result is not None and int(result.status_code / 100) == 2

class HPCDatastoreClient(object):
	"""HPC Datastore client implementation in Python 3"""

	def __init__(self, server_url="http://localhost:9080", dataset_path=None,
					access_regime = DatastoreAccess.READ, credentials=None):
		"""Initializes the Data Store connection

		:type dataset_path: str
		:param dataset_path: Identity of the datastore (UUID)

		:type access_regime: DatasetAccess
		:param access_regime: Access regime to the datastore - R/W/RW

		:type server_url: str
		:param server_url: DataStore server URL, defaults to localhost and port 9080

		:type credentials: object
		:param credentials: Future access credentials, set to None for now
		"""
		
		self.access_regime = access_regime
		self.credentials = credentials
		#self.server_url = server_url
		self.repository = HPCDatastoreRepository(server_url, dataset_path,
							credentials)

		#Running Data Store servers
		self.ds_servers = dict()

	def load_description(self):
		json_objects=self.repository.retrieve()
		#print(json_objects)
		self.ds_description = HPCDatastoreDescription(json_objects=json_objects)
	
	def start_dataset_server(resolution, access_regime=None,
									version="latest", timeout=10000):
		"""Open dataset server for limited time to work with slices

		:type resolutions: Point3D
		:param server_url: (X,Y,Z) tuple representing the resolution levels
		       from HPCDatastoreDescription on individual axes

		:type access_regime: DatastoreAccess
		:param access_regime: Access regime to the datastore - R/W/RW.
				None will result in the default access regime being used.

		:type version: str
		:param version: Name of the version (may be number, latest, ...)

		:type timeout: int
		:param timeout: Timeout of Service Client instance in ms
		"""
		if access_regime is None:
			access_regime = self.access_regime

		ds_regserv = RegisterServiceClient(self.repository.get_base_url(),
					access_regime, resolution, version, timeout,
					self.credentials)

	def __str__(self):
		out_s = ""
		for k in self.__dict__:
			out_s += "%s: %s\n" % (k,self.__dict__[k])
		return out_s
