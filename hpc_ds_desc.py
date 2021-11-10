#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum
from types import SimpleNamespace
from hpc_ds_types import Point3D, adjust_range
import json

VOXEL_TYPES = ["uint8", "uint16", "uint32", "uint64","int8", "int16", "int32", "int64", "float32", "float64"]
#VOXEL_UNITS = ["nm", "microns", "um", "mm","cm", "dm", "m", "km"]
COMPRESSIONS = ["none", "raw", "gzip"] # Is "raw" OK and how it differs from none?

class HPCDatastoreDescription(object):
	"""Datastore metadata object"""

	def __init__(self, dimensions=Point3D(1,1,1), timepoints=1, channels=1,
					angles=1, voxel_type="uint16",
					voxel_resolutions=Point3D(1,1,1),
					voxel_unit="um", time_res=1.0, time_unit ="seconds",
					channel_res=1.0, channel_unit="channel",
					angle_res=1.0, angle_unit="deg", resolution_levels=1,
					block_dimensions=Point3D(64,64,64),compression="gzip",
					transformations=None,
					json_objects=None):

		if voxel_type in VOXEL_TYPES:
			self.voxelType=voxel_type # Name has to keep JSON compatibility
		else:
			raise Exception("Voxel type %s not found" % voxel_type)

		if json_objects is None:
			self.timepoints = timepoints
			self.channels = channels
			self.angles = angles

			#if voxel_unit in VOXEL_UNITS: # No check is really needed
			self.voxelUnit=voxel_unit
			#else:
			#	raise Exception("Voxel unit %s not found" % voxel_unit)

			if compression in COMPRESSIONS:
				self.compression=compression
			else:
				raise Exception("Compression type %s not found" % compression)

			self.dimensions = Point3D.adjust_range(dimensions)

			self.voxelResolution = Point3D.adjust_range(voxel_resolutions,
				0.0,  datatype=float)

			self.timepointResolution = {
				"value" : adjust_range(time_res, 0, datatype=float),
				"unit" : time_unit
			}
			#TODO: We should really consider unifying s/seconds, um/microns in API

			self.channelResolution = {
				"value" : channel_res,
				"unit" : channel_unit
			}

			self.angleResolution = {
				"value" : angle_res,
				"unit" : angle_unit
			}

			resolution_levels = adjust_range(resolution_levels, 1)
			rl_entries = []
			for level in range(0, resolution_levels):
				level_v = 1 << level;
				rl_entries.append({
					"resolutions" : [level_v, level_v, level_v],
					"blockDimensions" : Point3D.adjust_range(block_dimensions)
				})
			self.resolutionLevels =  rl_entries
			#self.versions = [0]
			self.transformations = transformations
		else:
			self.__dict__ = json_objects

	@staticmethod
	def load_json(data):
		"""Loading of data store description data from JSON to a simple object"""
		return json.loads(data, object_hook=lambda d: SimpleNamespace(**d))

	def from_json(self, data):
		"""Importing data store description from JSON representation"""
		self.__dict__ = HPCDatastoreDescription.load_json(data).__dict__

	def to_json(self):
		"""Exporting data store description into JSON representation"""
		return json.dumps(self, default=lambda o: o.__dict__, sort_keys=False, indent=2)

	def __str__(self):
		"""String conversion of internal description to human readable text"""
		out_s = ""
		for k in self.__dict__:
			out_s += "%s: %s\n" % (k,self.__dict__[k])
		return out_s
