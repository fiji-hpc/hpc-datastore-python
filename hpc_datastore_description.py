#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum
from types import SimpleNamespace
from types import FunctionType
import json

VOXEL_TYPES = ["uint8", "uint16", "uint32", "uint64","int8", "int16", "int32", "int64", "float32", "float64"]
#VOXEL_UNITS = ["nm", "microns", "um", "mm","cm", "dm", "m", "km"]
COMPRESSIONS = ["none", "raw", "gzip"] # Is "raw" OK and how it differs from none?

def adjust_range(v, min_value=1, max_value=None, datatype=int):
	"""Helper function to adjust a variable range and convert it to datatype
    :param v: Adjusted variable

    :type min_value: float
    :param min_value: Minimum value of v

    :type max_value: float
    :param max_value: Maximum value of v

    :type datatype: object
    :param datatype: ob

    :type credentials: object
    :param credentials: Future access credentials, set to None for now
	"""

	if min_value is not None and v < min_value:
		v = min_value
	elif max_value is not None and v > max_value:
		v = max_value
	if datatype is not None and isinstance(datatype, FunctionType):
		v = datatype(v)
	return v

def xyz_value(x, y, z, min_value=1, max_value=None, datatype=int):
	"""Helper function conveting values in 3 dimensions taking into
	account the data types, minimum and maximum values
	"""

	return [ adjust_range(x, min_value, max_value, datatype),
			 adjust_range(y, min_value, max_value, datatype),
			 adjust_range(z, min_value, max_value, datatype)
			]


class HPCDatastoreDescription(object):
	"""Datastore metadata object"""

	def __init__(self, dimensions=(1,1,1), timepoints=1, channels=1, angles=1,
					voxel_type="uint16", voxel_resolutions=(1,1,1),
					voxel_unit="um", time_res=1.0, time_unit ="seconds",
					channel_res=1.0, channel_unit="channel",
					angle_res=1.0, angle_unit="deg", resolution_levels=1,
					block_dimensions=(64,64,64),compression="gzip",
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

			self.dimensions = xyz_value(dimensions[0], dimensions[1],
										dimensions[2])

			self.voxelResolution = xyz_value(voxel_resolutions[0],
				voxel_resolutions[1], voxel_resolutions[2], 0.0,
				datatype=float)

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
					"blockDimensions" : xyz_value(block_dimensions[0],
						block_dimensions[1], block_dimensions[2])
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
