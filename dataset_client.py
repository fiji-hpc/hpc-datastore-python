#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum
from hpc_ds_types import Point3D

import requests

class DatasetServerClient(object):
	def __init__(self, base_url, version=0, ):
		self.base_url = base_url
		self.version = version
		
		
