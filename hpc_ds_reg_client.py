#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from time import time
from hpc_ds_types import DatastoreAccess, EXTRA_VERSIONS, Point3D
from hpc_ds_dsclient import DatasetServerClient
import requests

class RegisterServiceClient(object):
	def __init__(self, base_url, access_regime, resolution=Point3D(1,1,1),
				  version="latest", timeout=10000, credentials=None):
		"""Initialize the Register service to request service server with
		:type base_url: str
		:param base_url: Full url path to the datastore instance

		:type access_regime: DatastoreAccess
		:param access_regime: Access regime to the datastore - R/W/RW

		:type resolution: Point3D
		:param server_url: (X,Y,Z) tuple representing the resolution levels
		       from HPCDatastoreDescription

		:type version: str
		:param version: Name of the version (may be number, latest, ...)

		:type timeout: int
		:param timeout: Timeout of Service Client instance in ms

		:type credentials: object
		:param credentials: Future access credentials, set to None for now
		"""
		self.base_url = base_url
		if not isinstance(access_regime, DatastoreAccess):
			raise DataStoreAccessException(
			   "Invalid access regime \"%s\", use DatastoreAccess enum values"
			   % str(access_regime))
		if not isinstance(version, int) and not version in EXTRA_VERSIONS:
			raise DataStoreAccessException(
				"Invalid datastore version string \"%s\"" % str(version))

		self.access_regime = access_regime
		self.version = version
		self.resolution = resolution
		self.credentials = credentials
		if timeout is not None and int(timeout) > 0:
			self.timeout = int(timeout)
		else:
			self.timeout = None

	def to_url(self, use_timeout=True):
		retval = self.base_url + Point3D.to_ds_url_part(self.resolution) \
			+ '/' + str(self.version) + '/' + str(self.access_regime)
		if use_timeout and self.timeout is not None:
			retval += "?timeout="  + str(self.timeout)
		return retval

	def start(self):
		"""Opens connection to the server"""
		if self.timeout:
			self.expires = (int(time() * 1000) + self.timeout) / 1000
		else:
			self.expires = None
		#print(self.to_url())
		result = requests.get(self.to_url(), allow_redirects=False)
		if result is not None and int(result.status_code) == 307:
			self.client = DatasetServerClient(result.headers['Location'], self)
			#print('Result: ' + str(result.status_code) + '\n'
			#		+ 'Answer: ' +  result.text + '\n'
			#		+ 'New server: ' + result.headers['Location'])
		else:
			raise HPCRepositoryAccessException(
			"Register Service did not start a new server, HTTP error %i"
				% result.status_code)
