from collections import namedtuple
from enum import Enum
from types import FunctionType

# Dataset path main directory on servers, it is fixed for now
DS_PATH= '/datasets'

# Common Exceptions
class InvalidDatasetException(Exception):
	pass

class HPCRepositoryAccessException(Exception):
	pass

class DataStoreAccessException(Exception):
	pass

# Enumerations
class DatastoreAccess(Enum):
	""" DataStore Access Enumeration and its string interpretations"""

	READ=1
	WRITE=2
	READ_WRITE=3

	def __str__(self):
		if self == DatastoreAccess.READ_WRITE:
			return "read-write"
		else:
			return str.lower(self.name)

# Constants
VOXEL_TYPES = ["uint8", "uint16", "uint32", "uint64","int8", "int16",
				"int32", "int64", "float32", "float64"
]

COMPRESSIONS = ["none", "raw", "gzip"] # Is "raw" OK and how it differs from none?

EXTRA_VERSIONS = ["latest", "mixedLatest" ] # Non-numeric versions

#VOXEL_UNITS = ["nm", "microns", "um", "mm","cm", "dm", "m", "km"]
# Voxel units are not checked for validity

# Helper functions
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

# Points
class Point3D(namedtuple('Point3D', ['x', 'y', 'z'])):

	@staticmethod
	def adjust_range(xyz_source, min_value=1, max_value=None, datatype=int):
		"""Helper static method conveting values in all 3 dimensions taking
		into account the data types, minimum and maximum values
		"""
		x, y, z = xyz_source
		return Point3D(adjust_range(x, min_value, max_value, datatype),
			 adjust_range(y, min_value, max_value, datatype),
			 adjust_range(z, min_value, max_value, datatype)
		)

	def adjust(self,  min_value=1, max_value=None, datatype=int):
		"""Adjust own dimentsions"""
		return Point3D.adjust_range(self, min_value, max_value, datatype)

	def to_ds_URL_component(self):
		res=''
		for x in self:
			res += '/' + str(x)
		return res
