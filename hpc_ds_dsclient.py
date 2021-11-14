#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum
from hpc_ds_types import Point3D, Block5D, DatastoreAccess, VOXEL_TYPES, \
						MAX_URL_LEN

from time import time
import struct

import requests

class DatasetServerClient(object):

	header="!lll"

	binary_headers =  { "Content-Type": "application/octet-stream" }

	def __init__(self, base_url, regs_client):
		#TODO: Use credentials from regs_client
		self.base_url = base_url
		self.regs_client = regs_client
		self.info = self.fetch_info()
		self.voxel_type = None
		self.block_fmt = None

	def fetch_info(self):
		result = requests.get(self.base_url)
		if result is not None and int(result.status_code / 100) == 2:
			return result.json()

	def is_running(self):
		"""Check if dataset server is still running or it has timed out"""
		return (self.info['serverTimeout'] < 0 or
					time() < self.regs_client.expires)

	@property
	def can_read(self):
		"""Check if dataset server info allows reading of data"""
		return self.info['mode'].find(DatastoreAccess.READ.name) >= 0

	@property
	def can_write(self):
		"""Check if dataset server info allows writing of data"""
		return self.info['mode'].find(DatastoreAccess.WRITE.name) >= 0

	@property
	def datatype(self):
		"""Return default datatype for time=0, channel=0 and angle=0"""
		return self.get_datatype()

	def get_datatype(self, time=0, channel=0, angle=0):
		"""Get datatype for given time, channel and angle combination
		:type time: int
		:param time: Time dimension of data

		:type channel: int
		:param channel: Used channel (e.g. red) of data

		:type angle: int
		:param angle: Angle of data for rotating slices in 3D

		:rtype: str
        :return: Name of datatype sizes mappable with VOXEL_TYPES
		"""
		url = self.base_url + ("datatype/%i/%i/%i" % (time, channel, angle))
		result = requests.get(url)
		if result is not None and int(result.status_code / 100) == 2:
			return result.text
		return None

	def init_block_fmt(self):
		if self.voxel_type is None:
			self.voxel_type = self.datatype
		self.block_fmt = DatasetServerClient.header + "%u" \
							+ VOXEL_TYPES[self.voxel_type]

	def read_block(self, block_coords):
		"""Request a block from dataset server
		:type block_coords: Block5D
		:param block_coords: Tuple representing 5D coordinates of
			(x,y,z,time, channel, angle)

		:rtype: tuple
		:return: A tuple with data and ``Point3D`` representing its sizes
		"""

		if not self.can_read:
			raise DataStoreAccessException(
				"Collection opened from %s is not readable"
				% self.regs_client.to_url())

		if self.block_fmt is None:
			self.init_block_fmt()

		url = self.base_url + Block5D.to_ds_url_part(block_coords)
		result = requests.get(url)
		if result is not None and int(result.status_code / 100) == 2:
			data=result.content
			x,y,z = struct.unpack(DatasetServerClient.header, data[0:12])
			total_size = x * y * z
			if total_size != -1:
				return (Point3D(x, y, z),
					 struct.unpack("!12x%u%s" % (total_size, \
										VOXEL_TYPES[self.voxel_type]), \
									data))
		return None

	def read_blocks(self, block_coords_array):
		"""Request a block from dataset server
		:type block_coords_array: Block5D
		:param block_coords_array: array or list of tuples representing
		5D coordinates of (x,y,z,time, channel, angle)

		:rtype: tuple
		:return: tuple with a dictionary containing data for datapoints
		and ``Point3D`` representing its sizes
		"""

		if not self.can_read:
			raise DataStoreAccessException(
				"Collection opened from %s is not readable"
				% self.regs_client.to_url())

		if self.block_fmt is None:
			self.init_block_fmt()

		count=0
		blocks=len(block_coords_array)
		results = {}
		ids = []

		url = self.base_url
		for idx in range(blocks):
			item = block_coords_array[idx]
			url2 = url + Block5D.to_ds_url_part(item) + '/'
			if len(url2) < MAX_URL_LEN and idx != blocks - 1:
				url = url2
				ids += [item]
				count = count+1
				continue
			else:
				if idx != blocks - 1:
					url2 = self.base_url + Block5D.to_ds_url_part(item)
				else: # Last block has to be added to the URL
					#TODO: We use shorter URLs, but could we get over
					#      the maximum? Maybe rewrite with blocks+1 and
					#      different ifs?
					url = url2
					ids += [item]
					count = count+1
				#print(url, count, str(ids))
				result = requests.get(url[:-1])
				if result is not None and int(result.status_code / 100) == 2:
					all_data=result.content
					start=0
					for i in range(count):
						hdr=all_data[start:start+12]
						x,y,z = struct.unpack(DatasetServerClient.header, hdr)
						total_size = x * y * z
						expander="!%u%s" % (total_size, \
										VOXEL_TYPES[self.voxel_type])
						if total_size != -1:
							typed_size = struct.calcsize(expander)
							next_index = typed_size + start+12
							results[ids[i]] = (Point3D(x, y, z),
								 struct.unpack(expander, all_data[
								   start+12:next_index]))
							start = next_index
						else:
							start += 12
				if idx != blocks - 1:
					count = 1
					url = url2
					ids = [item]
		return results


	def write_block(self, block_coords, data, block_sizes):
		"""Request a block from dataset server
		:type block_coords: ``Block5D``
		:param block_coords: Tuple representing 5D coordinates of
			(x,y,z,time, channel, angle)
		:type data: array
		:param data: Array of datatype entries representing block data

		:type block_sizes: ``Point3D``
		:param block_sizes: Tuple representing data sizes in x, y and z
		"""
		if not self.can_write:
			raise DataStoreAccessException(
				"Collection opened from %s is not writable"
				% self.regs_client.to_url())

		if self.block_fmt is None:
			self.init_block_fmt()

		total_size = block_sizes[0] * block_sizes[1] * block_sizes[2]
		assert(len(data) == total_size)

		url = self.base_url + Block5D.to_ds_url_part(block_coords)
		post_data=struct.pack(self.block_fmt % total_size, block_sizes[0], \
								block_sizes[1], block_sizes[2], data)
		result=requests.post(url, data=post_data, headers=binary_headers)
		return result is not None and int(result.status_code / 100) == 2


	def stop(self):
		"""Stop the dataset server instance"""
		if self.is_running():
			requests.post(self.base_url + 'stop', data="")
			self.info['serverTimeout'] = 0
			self.regs_client.expires = 0
