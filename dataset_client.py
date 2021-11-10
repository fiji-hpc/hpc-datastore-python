#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum
from hpc_ds_types import Point3D
from time import time

import requests

class DatasetServerClient(object):
	def __init__(self, base_url, info, start_time):
		self.base_url = base_url
		self.info = info
		self.start_time = start_time
		self.running=True

	def is_running(self):
		"""Check if dataset server is still running or it has timed out"""
		return self.running and (self.info.serverTimeout < 0 or \
					time() < start_time + info.serverTimeout/1000.0)

	def stop(self):
		"""Stop the dataset server instance"""
		if self.is_running():
			requests.post(self.base_url + '/stop', data="")
			self.running = False
		
