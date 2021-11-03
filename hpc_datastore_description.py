#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum
from types import SimpleNamespace
from types import FunctionType
import json 

VOXEL_TYPES = ["uint8", "uint16", "uint32", "uint64","int8", "int16", "int32", "int64", "float32", "float64"]	
VOXEL_UNITS = ["nm", "um", "mm","cm", "dm", "m", "km"]	
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
	return [ adjust_range(x, min_value, max_value, datatype),
				adjust_range(y, min_value, max_value, datatype),
				adjust_range(z, min_value, max_value, datatype)]
class HPCDatastoreDescription(object):
	"""Datastore metadata object"""
	
	def __init__(self, dimensions, timepoints=1, channels=1, angles=1, 
					voxel_type="uint16", voxel_resolutions=(1,1,1), 
					voxel_unit="um", time_res=1.0, time_unit ="seconds",
					channel_res=1.0, channel_unit="channel",
					angle_res=1.0, angle_unit="deg", resolution_levels=1,
					block_dimensions=(64,64,64),compression="gzip"):
		
		self.timepoints = timepoints
		self.channels = channels
		self.angles = angles
		
		if voxel_type in VOXEL_TYPES:
			self.voxelType=voxel_type # Name has to keep JSON compatibility
		else:
			raise Exception("Voxel type %s not found" % voxel_type)
			
		if voxel_unit == "microns" : 
			self.voxelUnit = "um" 	 # Library compatibility	
		elif voxel_unit in VOXEL_UNITS:
			self.voxelUnit=voxel_unit # Name has to keep JSON compatibility
		else:
			raise Exception("Voxel unit %s not found" % voxel_unit)

		if compression in COMPRESSIONS:
			self.compression=compression
		else:
			raise Exception("Compression type %s not found" % compression)

		self.dimensions = xyz_value(dimensions[0], dimensions[1], dimensions[2])
			
		self.voxelResolution = xyz_value(voxel_resolutions[0], 
			voxel_resolutions[1], voxel_resolutions[2], 0.0, datatype=float)
			
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
				"block_dimensions" : xyz_value(block_dimensions[0], 
					block_dimensions[1], block_dimensions[2])
			})
		self.resolutionLevels =  rl_entries
		
	@staticmethod
	def load_json(data):
		return json.loads(data, object_hook=lambda d: SimpleNamespace(**d))

	def from_json(self, data):
		self.__dict__ = HPCDatastoreDescription.load_json(data).__dict__

	def to_json(self):
		return json.dumps(self, default=lambda o: o.__dict__, sort_keys=False, indent=2)
	
	def __str__(self):
		out_s = ""
		for k in self.__dict__: 
			out_s += "%s : %s\n" % (k,self.__dict__[k])
		return out_s
