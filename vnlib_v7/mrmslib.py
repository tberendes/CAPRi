# ---------------------------------------------------------------------------------------------
#
#  mrmslib.py
#
#  Author:  Todd Berendes, UAH ITSC, July 2021
#
#  Description: this module provides MRMS matchup class and binary file access and manipulation
#
#  Syntax: currently no input parameters
#
#  To Do: modify to accept input parameters
#
# ---------------------------------------------------------------------------------------------

# --Do all the necessary imports
import struct
import numpy as np

class binaryFile:
    def __init__(self, filename):
        self.data = None
        self.width = None  # longitude direction
        self.height = None  # latitude direction
        self.llLat = None  # lower left corner
        self.llLon = None
        self.llResolution = None
        self.header_size = 5
        # note: data is written in ascending lat, lon (i.e. lower left on map)
        # if you want to access data in image style coordinate (i.e. upper left descending)
        # you need to flip the line order of the numpy array by setting flip_flag = True
        # be sure to set flip flag to "True" if you are creating an image from the binary array (png, etc)
        self.flip_flag = False

        self.load_data(filename)

    # convert x and y coordinates to lat, lon
    def get_lat(self, y):
        # note, fixed bug here, added "not"
        if not self.flip_flag:
            lat = y * self.llResolution + self.llLat
        else:
            lat = (self.height - y - 1) * self.llResolution + self.llLat
        return lat

    def get_lon(self, x):
        return x * self.llResolution + self.llLon

    def set_flip_flag(self, trueOrFalse):
        self.flip_flag = trueOrFalse

    def get_lat_lon(self, x, y):
        return self.get_lat(y), self.get_lon(x)

    def get_lat_lon_box(self):
        south = self.llLat
        west = self.llLon
        north = self.llLat + (self.height - 1) * self.llResolution
        east = self.llLon + (self.width - 1) * self.llResolution
        return north, south, east, west

    # crops binary image from center to width and height.
    def crop_from_center(self,width,height):
        # check site of image and return if the requested crop width or height is greater than current width or height
        if width >= self.width:
            print("Cannot crop, requested width "+str(width) +" is greater than current width "+self.width)
            return
        if height >= self.height:
            print("Cannot crop, requested height "+str(height) +" is greater than current height "+self.height)
            return
        x_start = int((self.width - width)/2)
        y_start = int((self.height - height)/2)
        #print(" x_start "+str(x_start) + " y_start " + str(y_start))

        self.llLon = self.llLon + (x_start * self.llResolution)
        self.llLat = self.llLat + (y_start * self.llResolution)
        self.height = height
        self.width = width
        # print("Cropped...")
        # print("width " +str(self.width))
        # print("height " +str(self.height))
        # print("ll lat "+str(self.llLat))
        # print("ll lon "+str(self.llLon))
        # print("resolution " + str(self.llResolution))
        # not np array subsetting is exclusive of ending element (i.e. less than)
        self.data = self.data[y_start:y_start+height, x_start:x_start+width]

    def write(self,filename):
        with open(filename, 'wb') as data_file:
            header= [self.width, self.height, self.llLat, self.llLon, self.llResolution]
            size = int(self.width * self.height)
            shape = (int(self.height), int(self.width))

            header_struct = struct.Struct('>{0}f'.format(self.header_size))
            packed_header = header_struct.pack(*header)
            data_file.write(packed_header)

            data_struct = struct.Struct('>{0}f'.format(size))
            packed_data = data_struct.pack(*self.data.flatten())
            #packed_data = np.packbits(self.data, bitorder='big')
            data_file.write(packed_data)

    def __del__(self):
        # Destructor:
        pass

    # Charles code
    def load_data(self, path):
        with open(path, 'rb') as data_file:
            # note: data is written in ascending lat, lon (i.e. lower left on map)
            # if you want to access data in image style coordinate (i.e. upper left descending)
            # you need to flip the line order of the numpy array
            header = struct.unpack('>{0}f'.format(self.header_size), data_file.read(4 * self.header_size))
            self.width = int(header[0])
            self.height = int(header[1])
            self.llLat = header[2]
            self.llLon = header[3]
            self.llResolution = header[4]
            size = int(self.width * self.height)
            shape = (int(self.height), int(self.width))
            # print("width " + str(self.width))
            # print("height " + str(self.height))
            # print("ll lat " + str(self.llLat))
            # print("ll lon " + str(self.llLon))
            # print("resolution " + str(self.llResolution))

            data = struct.unpack('>{0}f'.format(size), data_file.read(4 * size))
            self.data = np.asarray(data).reshape(shape)

    def get_data(self):
        # flip line order if flip_flag is set to true
        if self.flip_flag:
            return np.flip(self.data, 0)  # data is packed in ascending latitude
        else:
            return self.data  # data is packed in ascending latitude


class MRMSToGPM:
    def __init__(self, mrms_filename):
        self.MRMS=None
        self.GPM=None
        self.GPMFootprint=None
        self.MRMSDeepLearning=None # placeholder for eventual loading of deep learning results
        self.load_data(mrms_filename)

    def __del__(self):
        # Destructor:
        pass
    def load_data(self, mrms_filename):
        # construct footprint and GPM filenames from MRMS filename
        self.MRMS = binaryFile(mrms_filename)
        gpm_filename = mrms_filename.split('.mrms.bin')[0]+'.gpm.bin'
        self.GPM = binaryFile(gpm_filename)
        fp_filename = mrms_filename.split('.mrms.bin')[0]+'.fp.bin'
        self.GPMFootprint = binaryFile(fp_filename)

    def set_flip_flag(self,value):
        self.MRMS.set_flip_flag(value)
        self.GPM.set_flip_flag(value)
        self.GPMFootprint.set_flip_flag(value)
