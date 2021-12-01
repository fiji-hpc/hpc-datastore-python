#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum
from hpc_ds_types import *
from hpc_ds_desc import HPCDatastoreDescription
from hpc_ds_dsclient import DatasetServerClient
from hpc_ds_reg_client import RegisterServiceClient
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

	def rebuild(self, resolutions=[Point3D(1,1,1)]):
		"""Rebuilds data for specified resolutions from base data"""
		self.assert_dataset_ready(self.dataset_path)
		res_url = Point3D(1,1,1).to_ds_url_part() # From doc, otherwise ""
		for resolution in resolutions:
			res_url += Point3D.to_ds_url_part(resolution)
		result = requests.get(self.get_base_url() + res_url + "/rebuild")
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
		self.ds_description = None
		#self.server_url = server_url
		self.repository = HPCDatastoreRepository(server_url, dataset_path,
							credentials)

		#Running Data Store servers
		self.ds_servers = dict()

	def load_description(self):
		json_objects=self.repository.retrieve()
		#print(json_objects)
		self.ds_description = HPCDatastoreDescription(json_objects=json_objects)

	def start_dataset_server(self, resolution, access_regime=None,
									version="latest", timeout=15000):
		"""Open dataset server for limited time to work with slices and
		returns object which is representing it

		:type resolutions: ``Point3D``
		:param server_url: (X,Y,Z) tuple representing the resolution levels
		       from HPCDatastoreDescription on individual axes

		:type access_regime: DatastoreAccess
		:param access_regime: Access regime to the datastore - R/W/RW.
				None will result in the default access regime being used.

		:type version: str
		:param version: Name of the version (may be number, latest, ...)

		:type timeout: int
		:param timeout: Timeout of Service Client instance in ms

		:rtype: ``DatasetServerClient``
		:return: A new or already running instance of ``DatasetServerClient``
		"""
		if access_regime is None:
			access_regime = self.access_regime

		if self.ds_description is None:
			self.load_description()

		ds_regserv = RegisterServiceClient(self.repository.get_base_url(),
					access_regime, resolution, version, timeout,
					self.credentials)

		ds_id=ds_regserv.to_url()
		if ds_id in self.ds_servers \
		  and (self.ds_servers[ds_id].expires is None \
				or self.ds_servers[ds_id].expires > time()):
			ds_regserv = self.ds_servers[ds_id]
		else:
			ds_regserv.start()
			self.ds_servers[ds_id] = ds_regserv
			ds_regserv.client.voxel_type = self.ds_description.voxelType

		return ds_regserv.client

	def __str__(self):
		out_s = ""
		for k in self.__dict__:
			out_s += "%s: %s\n" % (k,self.__dict__[k])
		return out_s
