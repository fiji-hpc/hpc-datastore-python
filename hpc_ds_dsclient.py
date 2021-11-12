#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum
from hpc_ds_types import Point3D, DatastoreAccess
from time import time

import requests

class DatasetServerClient(object):
	def __init__(self, base_url, regs_client):
		#TODO: Use credentials from regs_client
		self.base_url = base_url
		self.regs_client = regs_client
		self.info = self.fetch_info()

	def fetch_info(self):
		result = requests.get(self.base_url)
		if result is not None and int(result.status_code / 100) == 2:
			return result.json()

	def is_running(self):
		"""Check if dataset server is still running or it has timed out"""
		return (self.info['serverTimeout'] < 0 or
					time() < self.regs_client.expires)

	def can_read(self):
		"""Check if dataset server info allows reading of data"""
		return self.info['mode'].find(DatastoreAccess.READ.name) >= 0

	def can_write(self):
		"""Check if dataset server info allows writing of data"""
		return self.info['mode'].find(DatastoreAccess.WRITE.name) >= 0

	def stop(self):
		"""Stop the dataset server instance"""
		if self.is_running():
			requests.post(self.base_url + 'stop', data="")
			self.info['serverTimeout'] = 0
			self.regs_client.expires = 0
