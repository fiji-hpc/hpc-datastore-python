#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from time import time
from hpc_ds_types import DatastoreAccess
import requests

class RegisterServiceClient(object):
	def __init__(self, base_url, access_regime, resolutions=Point3D(1,1,1), 
				  version="latest", timeout=10000, credentials=None):
		"""Initialize the Register service to request service server with
		:type base_url: str
		:param base_url: Full URL path to the datastore instance

		:type access_regime: DatastoreAccess
		:param access_regime: Access regime to the datastore - R/W/RW

		:type resolutions: Point3D
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
			   "Invalid access regime %s, use DatastoreAccess enum values",
			   str(access_regime))
		self.access_regime = access_regime
		self.version = version
		self.resolutions = resolutions
		self.credentials = credentials
		
	def start(self):
		self.expires = int(time() * 1000) + self.timeout
		
		
		
