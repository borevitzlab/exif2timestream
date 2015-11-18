#!usr/bin/env python
"""Take somewhat structured image collections and outputs Timestream format."""
# pylint:disable=logging-format-interpolation

from __future__ import print_function

# Standard library imports
import argparse
import csv
import datetime
import inspect
import json
import logging
import multiprocessing
import os
import re
import shutil
import sys
from time import strptime, strftime, mktime, localtime, struct_time, time, sleep
import warnings
import struct
# Module imports
import pexif
import exifread
from PIL import Image

# global logger
log = logging.getLogger("exif2timestream")

# Constants
EXIF_DATE_TAG = "Image DateTime"
EXIF_DATE_FMT = "%Y:%m:%d %H:%M:%S"
DATE_MASK = "%Y%m%d_%H%M%S"
TS_V1_FMT = ("%Y/%Y_%m/%Y_%m_%d/%Y_%m_%d_%H/"
             "{tsname}_%Y_%m_%d_%H_%M_%S_{n:02d}.{ext}")
TS_V2_FMT = ("%Y/%Y_%m/%Y_%m_%d/%Y_%m_%d_%H/"
             "{tsname}_%Y_%m_%d_%H_%M_%S_{n:02d}.{ext}")
TS_DATE_FMT = "%Y_%m_%d_%H_%M_%S"
TS_FMT = TS_V1_FMT
TS_NAME_FMT = "{expt}-{loc}-c{cam}~{res}-{step}"
TS_NAME_STRUCT = "EXPT-LOCATION-CAM_NUM"
FULLRES_CONSTANTS = {"original", "orig", "fullres"}
IMAGE_TYPE_CONSTANTS = {"raw", "jpg"}
RAW_FORMATS = {"cr2", "nef", "tif", "tiff", "raw"}
IMAGE_SUBFOLDERS = {"raw", "jpg", "png", "tiff", "nef", "cr2"}
DATE_NOW_CONSTANTS = {"now", "current"}
ongoing = False

def cli_options():
    """Return CLI arguments with argparse."""
    parser = argparse.ArgumentParser()
    parser.add_argument('-V', '--version',
                        help='Print version information.')
    parser.add_argument('-t', '--threads', type=int, default=1,
                        help='Number of processes to use.')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Enable debug logging (to file).')
    parser.add_argument('-l', '--logdir',
                        help='Directory to contain log files.')
    parser.add_argument('-c', '--config', help='Path to CSV camera '
                        'config file for normal operation.')
    parser.add_argument('-g', '--generate', help='Generate a template'
                        ' camera configuration file at given path.')
    return parser.parse_args()


def date(x):
    """Converter / validator for date field."""
    if isinstance(x, struct_time):
        return x
    if x.lower() in DATE_NOW_CONSTANTS:
        return localtime()
    try:
        return strptime(x, "%Y_%m_%d")
    except:
        raise ValueError

def date_end(x):
    global ongoing
    """Converter / validator for date field."""
    if isinstance(x, struct_time):

        ongoing = False
        return x
    if x.lower() in DATE_NOW_CONSTANTS:
        ongoing = True
        return localtime()
    else:
        ongoing = False
    try:
        return strptime(x, "%Y_%m_%d")
    except:
        raise ValueError

def bool_str(x):
    """Converts a string to a boolean, even yes/no/true/false."""
    if isinstance(x, bool):
        return x
    elif isinstance(x, int):
        return bool(x)
    x = x.strip().lower()
    if x in {"t", "true", "y", "yes", "f", "false", "n", "no"}:
        return x in {"t", "true", "y", "yes"}
    return bool(int(x))


def int_time_hr_min(x):
    """Validator for time field."""
    if isinstance(x, tuple):
        return x
    return (int(x) // 100, int(x) % 100)


def path_exists(x):
    """Validator for path field."""
    if os.path.exists(x):
        return x
    raise ValueError("path '%s' doesn't exist" % x)


def resolution_str(x):
    """Validator for resolution field."""
    if not isinstance(x, str):
        raise ValueError
    res_list = []
    for res in x.strip().split('~'):
        xy = res.strip().lower().split("x")
        if res in FULLRES_CONSTANTS:
            res_list.append(res)
        elif len(xy) == 2:
            res_list.append(tuple(int(i) for i in xy))
        else:
            # either int(x-res), or raise ValueError for validator
            res_list.append((int(res), None))
    return res_list

def dataset(x):
    "If it exists, appends all neccesary items"
    if (x):
        if len(str(x)) ==1:
            return '-F0' + str(x)
        else:
            return '-F' + str(x)
    else:
        return ''

def cam_pad_str(x):
    """Pads a numeric string to two digits."""
    if len(str(x)) == 1:
        return '0' + str(x)
    return x


def image_type_str(x):
    """Validator for image type field."""
    if isinstance(x, list):
        return x
    if not isinstance(x, str):
        raise ValueError
    types = x.lower().strip().split('~')
    if not all(t in IMAGE_TYPE_CONSTANTS for t in types):
        raise ValueError
    return types


def remove_underscores(x):
    """Replaces '_' with '-'."""
    return x.replace("_", "-")

def large_json(x):
    if x in {"true", "True", "yes", "Yes", "1"}:
        return 1
    else:
        return 0


def method_list(x):
    """Ensure x is a vaild timestream method."""
    if x not in {"copy", "archive", "move", "resize", "json"}:
        raise ValueError
    return x


def mode_list(x):
    """Ensure x is a vaild timestream method."""
    if x not in {"batch", "watch"}:
        raise ValueError
    return x


class CameraFields(object):
    """Validate input and translate between exif and config.csv fields."""
    # Validation functions, then schema, then the __init__ and execution

    ts_csv_fields = (
        ('use', 'USE', bool_str),
        ('location', 'LOCATION', remove_underscores),
        ('expt', 'EXPT', remove_underscores),
        ('cam_num', 'CAM_NUM', cam_pad_str),
        ('source', 'SOURCE', path_exists),
        ('destination', 'DESTINATION', path_exists),
        ('archive_dest', 'ARCHIVE_DEST', path_exists),
        ('expt_end', 'EXPT_END', date_end),
        ('expt_start', 'EXPT_START', date),
        ('interval', 'INTERVAL', int),
        ('image_types', 'IMAGE_TYPES', image_type_str),
        ('method', 'METHOD', method_list),
        ('resolutions', 'RESOLUTIONS', resolution_str),
        ('sunrise', 'SUNRISE', int_time_hr_min),
        ('sunset', 'SUNSET', int_time_hr_min),
        ('timezone', 'CAMERA_TIMEZONE', int_time_hr_min),
        ('user', 'USER', remove_underscores),
        ('mode', 'MODE', mode_list),
        ('project_owner', 'PROJECT_OWNER', remove_underscores),
        ('ts_structure', 'TS_STRUCTURE', str),
        ('filename_date_mask', 'FILENAME_DATE_MASK', str),
        ('orientation', 'ORIENTATION', str),
        ('fn_parse', 'FN_PARSE', str),
        ('fn_structure', 'FN_STRUCTURE', str),
        ('datasetID', 'DATASETID', dataset),
        ('timeshift', 'TIMESHIFT', str),
        ('userfriendlyname', 'USERFRIENDLYNAME', str),
        ('large_json', 'LARGE_JSON', large_json),
        ('json_updates', 'JSON_UPDATES', str)
        )

    TS_CSV = dict((a, b) for a, b, c in ts_csv_fields)
    CSV_TS = {v: k for k, v in TS_CSV.items()}
    REQUIRED = {"use", "destination", "expt", "cam_num", "expt_end",
                "expt_start", "image_types", "interval", "location",
                "archive_dest", "method", "source"}
    SCHEMA = dict((a, c) for a, b, c in ts_csv_fields)

    def __init__(self, csv_config_dict):
        """Store csv settings as object attributes and validate."""
        csv_config_dict = {self.CSV_TS[k]: v for k, v in
                           csv_config_dict.items() if k in self.CSV_TS}
        # Set default properties
        if 'interval' not in csv_config_dict:
            csv_config_dict['interval'] = 1
        if 'method' not in csv_config_dict:
            csv_config_dict['method'] = 'archive'
        # Ensure required properties are included, and no unknown attributes
        if not all(key in csv_config_dict for key in self.REQUIRED):
            raise ValueError('CSV config dict lacks required key/s.')
# TODO: re-enable correctly, to catch illegal keys
#        if any(key not in self.TS_CSV for key in csv_config_dict):
#            raise ValueError('CSV config dict has unknown key/s.')
        # Converts dict keys and calls validation function on each value
        csv_config_dict = {k: self.SCHEMA[k](v)
                           for k, v in csv_config_dict.items()}
        # Set object attributes from config
        for k, v in csv_config_dict.items():
            setattr(self, self.CSV_TS[k] if k in self.CSV_TS else k, v)

        # Localise pathnames
        def local(p):
            """Ensure that pathnames are correct for this system."""
            return p.replace(r'\\', '/').replace('/', os.path.sep)
        self.source = local(self.source)
        self.archive_dest = local(self.archive_dest)
        self.destination = local(self.destination)
        log.debug("Validated camera '{}'".format(csv_config_dict))


class SkipImage(StopIteration):
    """Exception that specifically means skip this image.

    Allows try-except blocks to pass any errors on to the calling functions,
    unless they can be solved by skipping the erroring image.
    """
    pass


def d2s(date):
    """Format a date for easy printing"""
    if isinstance(date, struct_time):
        return strftime(TS_DATE_FMT, date)
    else:
        return date

def resolution_calc(camera, image):
    x=1

    for resize_resolution in camera.resolutions:
        if resize_resolution[1] is None:
            try:
                img = Image.open(image).size
                if camera.orientation in ("90", "270"):
                    new_res = (resize_resolution[0],
                                img[1] * resize_resolution[0] / img[0])
                    new_res = (new_res[1], new_res[0])
                else:
                    new_res = (resize_resolution[0],
                                img[1] * resize_resolution[0] / img[0])
                log.debug("One resolution arguments, '{0:d}'".format(new_res[0]))
                camera.resolutions[x] = new_res
            except Exception as e:
                log.debug("Wouldn't calculate resolution arguments" + str(e))
            x=x+1
    return camera

def create_small_json(res, camera, image_resolution, full_res, p_start, p_end, ts_end_text, ext, webrootaddr):
    if (res == "fullres"):
        folder = "original"
    else:
        folder = "output"

    if (ext in RAW_FORMATS):
        step = "raw"
    else:
        step = 'orig'
    if not os.path.exists(os.path.join(camera.destination, camera.ts_structure.format(folder=folder,
        res=res, step = step))):
        os.makedirs(os.path.join(camera.destination, camera.ts_structure.format(folder=folder,
        res=res, step = step)))
    small_json = open(os.path.join(camera.destination, camera.ts_structure.format(folder=folder,
        res=res, step = step), '{}-{}-C{}'.format(camera.expt, camera.location, camera.cam_num) +
                                   str(camera.datasetID) + '-ts-info.json'), 'wb+')
    jdump = {
        'expt': camera.expt,
        'owner': camera.project_owner,
        'height': image_resolution[1],
        'height_hires': full_res[camera.orientation not in ("270", "90")],
        'image_type': ext,
        'ts_name': camera.ts_structure.format(folder = folder, res = res, step = step).replace("\\","/"),
        'ts_id': '{}-{}-C{}'.format(camera.expt, camera.location, camera.cam_num) + str(camera.datasetID),
        'name':camera.userfriendlyname,
        'period_in_minutes': camera.interval,
        'posix_end': mktime(p_end),
        'posix_start': mktime(p_start),
        'timezone': camera.timezone[0],
        'ts_end': ts_end_text,
        'ts_start': strftime(TS_DATE_FMT, p_start),
        'width': image_resolution[0],
        'width_hires': full_res[camera.orientation in ("90", "270")],
        'webroot':webrootaddr.format(folder="output", res=res, step ="orig"),
        'webroot_hires':(webrootaddr.format(folder="original", res="fullres", step="orig"))
        }
    json.dump(jdump, small_json)
    small_json.close()

def parse_structures(camera):
    if not camera.userfriendlyname:
        camera.userfriendlyname = '{}-{}-C{}{}'.format(camera.expt, camera.location, camera.cam_num,camera.datasetID)
    else:
         for key, value in camera.__dict__.items():
            camera.userfriendlyname = camera.userfriendlyname.replace(key.upper(),
                                                              str(value))
         camera.userfriendlyname = camera.userfriendlyname.replace(os.path.sep, '')

    """Parse the file structure of the camera for conversion to timestream
    format."""
    if camera.ts_structure is None or len(camera.ts_structure) == 0:
        # If we dont have a ts_structure, then lets do the default one

        camera.ts_structure = os.path.join(
            camera.expt,
            (camera.location + '-C' +
            camera.cam_num + camera.datasetID),
            '{folder}',
            (camera.expt + '-' +
            camera.location + "-C" +
            camera.cam_num +
            camera.datasetID + "~{res}-{step}" )).replace("_","-")

    else:
        # Replace the ts_structure with all the other stuff
        for key, value in camera.__dict__.items():
            camera.ts_structure = camera.ts_structure.replace(key.upper(),
                                                              str(value))
        # If it starts with a /, then we need to get rid of that
        if camera.ts_structure[0] == os.path.sep:
            camera.ts_structure = camera.ts_structure[1:]
        # Split it up so we can add the "~orig~res" part
        camera.ts_structure = camera.ts_structure.replace("_", "-")
        direc, fname = os.path.split(camera.ts_structure)
        camera.ts_structure = os.path.join(
            direc,
            (fname + "~" + "{res}" + "-{step}")
            )
    if not len(camera.fn_structure) and not camera.fn_structure:
        camera.fn_structure = camera.expt.replace("_", "-") + \
            '-' + camera.location.replace("_", "-") + \
            '-C' + camera.cam_num.replace("_", "-") +\
            camera.datasetID + \
            '~{res}-{step}'
    else:
        for key, value in camera.__dict__.items():
            camera.fn_structure = camera.fn_structure.replace(key.upper(),
                                                              str(value))
        camera.fn_structure = camera.fn_structure.replace(os.path.sep, "")\
            .replace("_", "-") + '~{res}-{step}'
    return camera


def resize_function(camera, image_date, dest, img_array):
    """Create a resized image in a new location."""
    log.debug("Now checking if we have 1 or 2 resolution arguments on '{}'"
              .format(dest))
    for resize_resolution in camera.resolutions[1:]:
        new_res = resize_resolution
        log.debug("Two resolution arguments, "
                  "'{}' x '{}'".format(new_res[0], new_res[1]))
        log.info("Now getting Timestream name")
        ts_name = make_timestream_name(camera, res=new_res[camera.orientation in ("90", "270")], step="orig")
        resizing_temp_outname = get_new_file_name(image_date, ts_name)
        resized_img = os.path.join(
            camera.destination,
            camera.ts_structure.format(folder='output', res=str(new_res[camera.orientation in ("90", "270")]),
                                       cam=camera.cam_num, step='orig'),
            resizing_temp_outname)
        if os.path.isfile(resized_img):
            return
        log.debug("Full resized filename for output is '{}'".format(resized_img))
        resized_img_path = os.path.dirname(resized_img)
        if not os.path.exists(resized_img_path):
            try:
                os.makedirs(resized_img_path)
            except OSError:
                log.warn("Could not make dir '{}', skipping image '{}'"
                         .format(resized_img_path, resized_img))
                # raise SkipImage
        log.debug("Now actually resizing image to '{}'".format(resized_img))
        resize_img(dest, resized_img, new_res[0], new_res[1], img_array)


def resize_img(filename, destination, to_width, to_height, img_array):
    """Actually resizes the image."""
    img = img_array.resize((to_width, to_height))
    log.debug("Now resizing the image")
    log.debug("Saving Image")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        img.save(destination)
    # Write new exif data from old image
    try:
        exif_source = pexif.JpegFile.fromFile(filename)
        exif_dest = pexif.JpegFile.fromFile(destination)
        exif_dest.exif.primary.ExtendedEXIF.DateTimeOriginal = \
            exif_source.exif.primary.ExtendedEXIF.DateTimeOriginal
        exif_dest.exif.primary.Orientation = \
            exif_source.exif.primary.Orientation
        exif_dest.writeFile(destination)
        log.debug("Successfully copied exif data also")
    except (AttributeError, pexif.JpegFile.InvalidFile):
        log.debug("Unable to copy over some exif data")


def get_time_from_filename(filename, mask=None):
    """Replaces time placeholders with the regex equivalent to parse."""
    global DATE_MASK
    if len(mask) is 0:
        mask = DATE_MASK
    mask_r = r"\.*" + mask.replace("%Y", r"\d{4}") + r"\.*"
    for s in ('%m', '%d', '%H', '%M', '%S'):
        mask_r = mask_r.replace(s, r'\d{2}')
    date_reg_exp = re.compile(mask_r)
    for match in date_reg_exp.findall(filename):
        # Attempt to parse each match into a datetime; return first success
        try:
            datetime = strptime(match, mask)
            return datetime
        except ValueError:
            continue


def write_exif_date(filename, date_time):
    """Change an image timestamp."""
    try:
        img = pexif.JpegFile.fromFile(filename)
        img.exif.primary.ExtendedEXIF.DateTimeOriginal = strftime(
            EXIF_DATE_FMT, date_time)
        img.writeFile(filename)
        return True
    except:
        return False


def get_file_date(filename, timeshift, round_secs=1, date_mask = DATE_MASK):
    """Gets a time.struct_time from an image's EXIF, or None if not possible.
    """
    date = None
    try:
        exif_tags = pexif.JpegFile.fromFile(filename)
        str_date = exif_tags.exif.primary.ExtendedEXIF.DateTimeOriginal
        date = strptime(str_date, EXIF_DATE_FMT)
    except (AttributeError, pexif.JpegFile.InvalidFile, struct.error):
       # print ("failed pexif")
        pass
    if not date:
        with open(filename, "rb") as fh:
            exif_tags = exifread.process_file(
                fh, details=False, stop_tag=EXIF_DATE_TAG)
            try:
                str_date = exif_tags[EXIF_DATE_TAG].values
                date = strptime(str_date, EXIF_DATE_FMT)
            except KeyError:
             #   print ("failed ExifRead")
                pass
    if not date:
    # Try to get datetime from the filename, but not the directory
        log.debug("No Exif data in '{}', reading from filename".format(
            os.path.basename(filename)))
        # Try and grab the date, we can put a custom mask in here if we want
        date = get_time_from_filename(filename,date_mask)
        if date is None:
            log.debug("Unable to scrape date from '{}'".format(filename))
          #  print("Unable to read Exif Data")
            return None
        else:
            if not write_exif_date(filename, date):
                log.debug("Unable to write Exif Data")
    if round_secs > 1:
        date = round_struct_time(date, round_secs)
    if (timeshift and (int)(timeshift)):
        datetime_date = datetime.datetime.fromtimestamp(mktime(date))
        shift = datetime.timedelta(hours=(int)(timeshift))
        datetime_date = datetime_date + shift
        date = datetime_date.timetuple()
    log.debug("Date of '{}' is '{}'".format(filename, d2s(date)))
    return date


def get_new_file_name(date_tuple, ts_name, n=0, fmt=TS_FMT, ext="jpg"):
    """
    Gives the new file name for an image within a timestream, based on
    datestamp, timestream name, sub-second series count and extension.
    """
    if not (date_tuple and ts_name):
        raise SkipImage
    date_formatted_name = strftime(fmt, date_tuple)
    name = date_formatted_name.format(tsname=ts_name, n=n, ext=ext)
    log.debug("New filename is '{}'".format(name))
    return name


def round_struct_time(in_time, round_secs, tz_hrs=0, uselocal=True):
    """Round a struct_time object to any time interval in seconds."""
    # TODO:  replace use of time module with more reliable datetime
    seconds = mktime(in_time)
    rounded = int(round(seconds / float(round_secs)) * round_secs)
    if not uselocal:
        rounded -= tz_hrs * 60 * 60  # remove tz seconds, back to UTC
    rv_list = list(localtime(rounded))
    rv_list[8] = in_time.tm_isdst
    rv_list[6] = in_time.tm_wday
    retval = struct_time(tuple(rv_list))
    log.debug("time {} rounded to {:d} seconds is {}".format(
        d2s(in_time), round_secs, d2s(retval)))
    return retval


def make_timestream_name(camera, res="fullres", step="orig",
                         folder='original'):
    """Makes a timestream name given the format (module-level constant), step,
    resolution and a camera object."""
    if isinstance(res, tuple):
        res = "x".join([str(x) for x in res])
    ts_name = camera.fn_structure.format(
        expt=camera.expt,
        loc=camera.location,
        cam=camera.cam_num,
        res=str(res),
        step=step,
        folder=folder)
    return ts_name


def timestreamise_image(image, camera, subsec=0, step="orig"):
    """Process a single image, mv/cp-ing it to its new location"""
    # Edit the global variable for the date mask, used elsewhere

    global DATE_MASK
    DATE_MASK = camera.filename_date_mask
    image_date = get_file_date(image, camera.timeshift, camera.interval * 60)
    if not image_date:
        log.warn("Couldn't get date for image {}".format(image))
        raise SkipImage
    in_ext = os.path.splitext(image)[-1].lstrip(".")
    ts_name = make_timestream_name(camera, res="fullres", step=step)
    out_image = get_new_file_name(image_date, ts_name, n=subsec, ext=in_ext)
    out_image = os.path.join(
        camera.destination,
        camera.ts_structure.format(folder='original', res='fullres',
                                   cam=camera.cam_num, step=step),
        out_image)
    # make the target directory
    try:
        os.makedirs(os.path.dirname(out_image))
    except OSError:
        if not os.path.exists(os.path.dirname(out_image)):
            log.warn("Could not make dir '{}', skipping image '{}'"
                     .format(os.path.dirname(out_image), image))
            raise SkipImage
    # And do the copy
    dest = _dont_clobber(out_image, mode=SkipImage)

    try:
        shutil.copyfile(image, dest)
        log.info("Copied '{}' to '{}".format(image, dest))
    except:
        log.warn("Couldnt copy '{}' to '{}', skipping image".format(
            image, dest))
        raise SkipImage
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        if camera.orientation and camera.orientation is not 0 and step != "raw":
            img_array = rotate_image(camera.orientation, dest)
            write_exif_date(dest, image_date);
        elif (len(camera.resolutions)>1) and step != "raw":
            img_array = Image.open(dest)
    if len(camera.resolutions) > 1 and step != "raw":
        log.info("Going to resize image '{}'".format(dest))
        try:
            resize_function(camera, image_date, dest, img_array)
        except IOError:
            log.debug("Resize failed due to io error")
            raise SkipImage
        except SkipImage:
            log.debug("Faied to resize due to skipimage being reported first")
            raise SkipImage
        except Exception as e:
            print(e)
            log.debug(e)
            log.debug("Resize failed for unknown reason")
            raise SkipImage

def rotate_image(rotation, dest):
    try:
        img = Image.open(dest)
        img = img.rotate(float(rotation), expand =1)
        img.save(dest)
        return img
    except IOError:
        log.debug("Can't Rotate Non JPEG Images {}".format(dest))

def _dont_clobber(fn, mode="append"):
    """Ensure we don't overwrite things, using a variety of methods"""
    #TODO:  will clobber something different if the new filename exists
    if os.path.exists(fn):
        # Deal with SkipImage or StopIteration exceptions, even uninstantiated
        if isinstance(mode, StopIteration):
            log.debug("Path '{}' exists, raising StopIteration".format(fn))
            raise mode
        elif inspect.isclass(mode) and issubclass(mode, StopIteration):
            log.debug("Path '{0}' exists, raising an Exception".format(fn))
            raise mode()
        # Otherwise, append something '_1' to the file name
        elif mode == "append":
            log.debug("Path '{}' exists, adding '_1' to its name".format(fn))
            base, ext = os.path.splitext(fn)
            return '{}_1.{}'.format(base, ext) if ext else '{}_1'.format(base)
        raise ValueError("Bad _dont_clobber mode: %r", mode)
    log.debug("Path '{}' doesn't exist. Returning it.".format(fn))
    return fn


def process_image(args):
    """Do move and copy operations for a camera config and list of images."""
    log.debug("Starting to process image")

    image, camera, ext = args
    image_date = get_file_date(image, camera.timeshift, camera.interval * 60)
    if camera.expt_start > image_date or image_date > camera.expt_end:
        log.debug("Skipping {}. Outside of date range {} to {}".format(
            image, d2s(camera.expt_start), d2s(camera.expt_end)))
        return
    my_ext = os.path.splitext(image)[-1].lower().strip(".")
    if not (my_ext == ext) and not ((my_ext in RAW_FORMATS) and (ext == "raw")):
        return
    if camera.method == "json":
        return
    if "last_image" in image.lower() :
        log.debug ("Skipping file {}, assumed last image".format(image))
        return
    if camera.method == "resize" and (ext not in RAW_FORMATS):
        img_array = Image.open(image)
        resize_function(camera, image_date, image, img_array)
        log.debug("Rezied Image {}".format(image))
    if camera.method == "archive":
        log.debug("Will archive {}".format(image))
        ts_name = make_timestream_name(camera, res="fullres")
        out_image = get_new_file_name(image_date, ts_name)
        archive_image = os.path.join(
            camera.archive_dest,
            camera.expt,
            (camera.expt + '-' +
                camera.location + "-C" +
                camera.cam_num +
                camera.datasetID + "~fullres-" + (ext if ext in RAW_FORMATS else "orig")).replace("_","-"),
            os.path.relpath(image, camera.source))
        try:
            os.makedirs(os.path.dirname(archive_image))
            log.debug("Made archive dir {}".format(os.path.dirname(
                archive_image)))
        except OSError as exc:
            if not os.path.exists(os.path.dirname(archive_image)):
                raise exc
        archive_image = _dont_clobber(archive_image)
        shutil.copyfile(image, archive_image)
        log.debug("Copied {} to {}".format(image, archive_image))
    try:
        # deal with original image (move/copy etc)
        timestreamise_image(
            image, camera, subsec=0,
            step="raw" if ext.lower() in RAW_FORMATS else "orig")
        log.debug("Successfully timestreamed {}".format(image))
    except SkipImage:
        log.debug("Failed to timestream {} (got SkipImage)".format(image))

    if camera.method in {"move", "archive"}:
        # images have been archived above, so just delete originals
        try:
            os.unlink(image)
        except OSError:
            log.error("Could not delete '{0}'".format(image))
        log.debug("Deleted {}".format(image))


def parse_camera_config_csv(filename):

    """Parse a camera configuration, yielding localised and validated
    camera configuration objects."""
    if filename is None:
        raise StopIteration
    with open(filename) as fh:
        cam_config = csv.DictReader(fh)
        for camera in cam_config:
            try:
                camera = CameraFields(camera)
                if camera.use:
                    yield parse_structures(camera)
            except (SkipImage, ValueError):
                continue


def find_image_files(camera):
    """Scrape a directory for image files, by extension.
    Possibly, in future, use file magic numbers, but a bad idea on windows.
    """
    print("Finding Image Files in source Directory {}. ".format(camera.source))
    print("Warning, this can take a while depending on number of images in the directory")
    exts = camera.image_types
    ext_files = {}
    count_images = 0
    for ext in exts:
        src = camera.source

        lst = [x for x in os.listdir(src) if not x[0] in ('.', '_')]
        log.debug("List of src valid subdirs is {}".format(lst))
        for node in lst:
            log.debug("Found src subdir {}".format(node))
            if node.lower() == ext:
                src = os.path.join(src, node)
                break
        log.info("Walking from {} to find images".format(src))
        for cur_dir, dirs, files in os.walk(src):
            # for d in dirs:
            #     if not (d.lower() in IMAGE_SUBFOLDERS or d.startswith("_")):
            #         if not camera.method in ("resize", "json"):
            #             #log.error("Source directory has too many subdirs.")

            for fle in files:
                this_ext = os.path.splitext(fle)[-1].lower().strip(".")
                if ext in (this_ext) or (ext == "raw" and this_ext in RAW_FORMATS):
                    fle_path = os.path.join(cur_dir, fle)
                    if camera.fn_parse in fle_path:
                        count_images+=1
                        print("Found {:5d} Images".format(count_images), end='\r')
                        try:
                            ext_files[ext].append(fle_path)
                        except KeyError:
                            ext_files[ext] = []
                            ext_files[ext].append(fle_path)
            log.info("Found {0} {1} files for camera.".format(
                len(ext_files), ext))
    return ext_files


def setup_logs(logdir, debug=False):
    """Sets up logging options."""
    NOW = strftime("%Y%m%dT%H%M%S", localtime())
    fmt = logging.Formatter(
        '%(asctime)s - %(name)s.%(funcName)s - %(levelname)s - %(message)s')
    # Errors are logged to sys.stderr
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    ch.setFormatter(fmt)
    log.addHandler(ch)
    # Warnings and Info are also logged to file
    if logdir is not None or debug:
        logdir = logdir if os.path.exists(logdir) else '.'
        log_fh = logging.FileHandler(
            os.path.join(logdir, "e2t_" + NOW + ".log"))
        log_fh.setLevel(logging.INFO)
        log_fh.setFormatter(fmt)
        log.addHandler(log_fh)
    # Debug is optionally logged to another file
    if debug:
        debug_fh = logging.FileHandler(
            os.path.join(logdir, "e2t_" + NOW + ".debug"))
        debug_fh.setLevel(logging.DEBUG)
        debug_fh.setFormatter(fmt)
        log.addHandler(debug_fh)
        log.setLevel(logging.DEBUG)


def get_resolution(image, camera):
    """Return various resolution numbers for an image."""
    try:
        if "raw" in camera.image_types and os.path.splitext(image)[-1].lower().strip(".") in RAW_FORMATS:
            with open(image, "rb") as fh:
                exif_tags = exifread.process_file(
                    fh, details=False)
            try:
                width = exif_tags["Image ImageWidth"].values[0]
                height = exif_tags["Image ImageLength"].values[0]
                if ("rotated 90" in str(exif_tags["Image Orientation"]).lower()):
                    temp = height
                    height = width
                    width = temp
                image_resolution = (width, height)
            except KeyError:
                image_resolution=(0,0)
        else:
            try:
                image_resolution = Image.open(image).size
            except ValueError:
                print("Value Error?")
                image_resolution=(0,0)
    except IOError:
        image_resolution = (0, 0)
    folder, res = "original", 'fullres'
    return res, image_resolution, folder


def get_thumbnail_paths(camera, images, res, image_resolution, folder):
    """Return thumbnail paths, for the final resting place of the images."""
    webrootaddr = ""
    url = "http://phenocam.anu.edu.au/cloud/a_data"
    if "a_data" in camera.destination:
        webrootaddr = "http://phenocam.anu.edu.au/cloud/a_data{}/{}".format(
            camera.destination.split("a_data")[1],
            camera.ts_structure if camera.ts_structure else camera.location).replace("\\","/")
    thumb_image = []
    if len(images) > 4 and len(camera.resolutions) != 1:
        thumb_image = [None, None, None]
        sep = '/'
        for i in range(3):
            try:
                image_date = get_file_date(images[len(images)//2 + i], camera.timeshift,
                                           camera.interval * 60)
                ts_image = get_new_file_name(
                    image_date, make_timestream_name(camera, camera.resolutions[1], 'orig'))

                thumb_image[i] = sep.join([
                    camera.destination, os.path.dirname(camera.ts_structure).format(folder=folder),
                        os.path.basename(camera.ts_structure).format(res=res, step='orig'), ts_image]).replace("\\","/")
            except (SkipImage):
                pass
    for i in range (0, len(thumb_image)):
        if thumb_image[i]:
            if "a_data" in thumb_image[i]:
                thumb_image[i] = url + thumb_image[i].split("a_data")[1]
            if len(camera.resolutions)>1:
                thumb_image[i] = thumb_image[i].format(folder="output", res = camera.resolutions[1][0])
            else:
                thumb_image[i] = thumb_image[i].format(folder="original", res = "orig")

    return webrootaddr, thumb_image

def get_actual_start_end(camera, images, ext):
    earlier = True
    j=0
    my_ext_images = [];
    date = ''
    for image in images:
        my_ext = os.path.splitext(image)[-1].lower().strip(".")
        if (my_ext == ext):
            my_ext_images.append(image);
        elif (my_ext in RAW_FORMATS) and (ext == "raw"):
            my_ext_images.append(image);	
    while earlier and (j<= len(my_ext_images)-1):
        date = get_file_date(my_ext_images[j], camera.timeshift, camera.interval * 60)
        if (date >= camera.expt_start) and (date is not None):
            earlier = False
        j+=1
    if not (date):
        date = camera.expt_start
    p_start = date
    later = True
    j = len(my_ext_images)-1
    date = None
    while later and j>=0:
        date = get_file_date(my_ext_images[j], camera.timeshift, camera.interval * 60)
        if (date <= camera.expt_end) and (date is not None):
            later = False
        j-=1
    if date is None:
        date = camera.expt_end
    p_end = date
    return p_start, p_end

def find_empty_dirs(root_dir):
    for dirpath, dirs, files in os.walk(root_dir, topdown=False):
        if(len(files) is 1 and "thumbs.db" in files):
            os.remove(os.path.join(dirpath,"thumbs.db"))
        if (not dirs and not files) or len(os.listdir(dirpath))==0:
            print ("removing empty dir " + dirpath)
            os.rmdir(dirpath)

def process_camera(camera, ext, images, n_threads=1):
    """Process a set of images for one extension for a single camera."""

    if (camera.method == 'json'):
        new_images = []
        for image in images:
            if ("fullres" in image):
                new_images.append(image)
        images = new_images
    p_start, p_end = get_actual_start_end(camera, images, ext)
    try:
        my_image = (x for x in images if ((os.path.splitext(x)[-1].lower().strip(".") == ext) or (os.path.splitext(x)[-1].lower().strip(".") in RAW_FORMATS and ext == "raw"))).next()
        if camera.method == 'json':
            images = [my_image]
    except StopIteration:
	    return
    camera = resolution_calc(camera, my_image)
    res, image_resolution, folder = get_resolution(my_image, camera)
    webrootaddr, thumb_image = get_thumbnail_paths(camera, images, res, image_resolution, folder)
    webrootaddr = webrootaddr.replace("\\","/")

    # TODO: sort out the whole subsecond clusterfuck
    if n_threads == 1:
        log.info("Using 1 process - what is this? 1990?")
        for count, image in enumerate(images):
            print("Processed {:5d} Images".format(count), end='\r')

            process_image((image, camera, ext))
    else:
        threads = max(1, min(n_threads, multiprocessing.cpu_count() - 1))
        log.info("Using {0:d} processes".format(threads))
        # set the function's camera-wide arguments
        args = ((image, camera, ext) for image in images)
        pool = multiprocessing.Pool(threads)
        for count, _ in enumerate(pool.imap(process_image, args)):
            print("Processed {:5d} Images".format(count), end='\r')
        pool.close()
        pool.join()
    print("Processed {:5d} Images. Finished this cam!".format(count))
    if (ongoing):
        ts_end_text = "now"
    else:
        ts_end_text = strftime(TS_DATE_FMT, p_end)
    if len(camera.resolutions) >1:
        new_res = camera.resolutions[1]
    else:
        new_res = res
    jdump = {
        'access': '0',
        'expt': camera.expt,
        'height_hires': image_resolution[camera.orientation not in ("270", "90")],
        'height': new_res[camera.orientation not in ("270", "90")],
        'image_type': ext,
        'ts_id': '{}-{}-C{}'.format(camera.expt, camera.location, camera.cam_num ) + str(camera.datasetID),
        'name':camera.userfriendlyname,
        'period_in_minutes': camera.interval,
        'posix_end': mktime(p_end),
        'posix_start': mktime(p_start),
        'thumbnails': thumb_image,
        'timezone': camera.timezone[0],
        'ts_end': ts_end_text,
        'ts_start': strftime(TS_DATE_FMT, p_start),
        'ts_version': '1',
        'utc': "false",
        'webroot_hires':(webrootaddr.format(folder="original", res="fullres", step="orig")),
        'webroot':webrootaddr.format(folder="output", res=new_res[camera.orientation in ("90",
                                                                   "270")], step ="orig"),
        'width_hires': image_resolution[camera.orientation in ("90",
                                                                   "270")],
        'width': new_res[camera.orientation in ("90",
                                                                   "270")]
        }

    if (camera.json_updates) and ext != "raw" and camera.large_json:
        for key, value in jdump.items():
            if not (key.lower() in camera.json_updates.lower()):
                if not (key.lower() in ('ts_name')):
                    jdump.pop(key, None)
    create_small_json("fullres", camera,image_resolution, image_resolution, p_start, p_end, ts_end_text, ext, webrootaddr)
    if ext not in RAW_FORMATS:
        for resize_res in camera.resolutions[1:]:
            new_res = resize_res
            create_small_json(new_res[camera.orientation in ("90", "270")], camera, new_res, image_resolution, p_start, p_end, ts_end_text, ext, webrootaddr)
    if ext != 'raw' and camera.large_json:
        return {k: str(v) for k, v in jdump.items()}
    else:
        return False




def main(configfile, n_threads=1, logdir=None, debug=False):
    """The main loop of the module, do the renaming in parallel etc."""
    setup_logs(logdir, debug)
    start_time = time()
    n_images = 0
    json_dump = []
    for camera in parse_camera_config_csv(configfile):
        if (len(json_dump) is 0) and camera.large_json:
            try:
                already_json = open(os.path.join(camera.destination, 'all_cameras.json'), 'r')
                json_dump = json.load(already_json)
                already_json.close
            except IOError:
                pass
        print("Processing experiment {}, location {}".format(
            camera.expt, camera.location))
        log.info("Processing experiment {}, location {}".format(
            camera.expt, camera.location))
        print("Images are coming from {}, being put in {}".format(
            camera.source, camera.destination))
        log.info("Images are coming from {}, being put in {}".format(
            camera.source, camera.destination))
        for ext, images in find_image_files(camera).items():

            print(("Have Found {0} {1} images from this camera".format(
                len(images), ext)))
            log.info("Have Found {0} {1} images from this camera".format(
                len(images), ext))
            n_images += len(images)
            j_dump = process_camera(camera, ext, sorted(images),
                                            n_threads)
            # if (camera.large_json):
            if (j_dump):
                json_dump.append(j_dump)
            jpath = os.path.join(camera.destination) #, os.path.dirname(
                # camera.ts_structure.format(folder='', res='', cam=''))
            try:
                os.makedirs(jpath)
            except OSError:
                if not os.path.exists(jpath):
                    log.warn("Could not make dir '{}', skipping images"
                             .format(jpath))
            with open(os.path.join(jpath, 'all_cameras.json'), 'w') as fname:
                json.dump(json_dump, fname)
        #remove any empty directories in source
        if camera.method == "archive":
            empty = find_empty_dirs(camera.source)
    secs_taken = time() - start_time
    print("\nProcessed a total of {0} images in {1:.2f} seconds".format(
        n_images, secs_taken))


def gen_config(fname):
    """Write example config and exit if a filename is passed."""
    if fname is None:
        return
    with open(fname, "w") as f:
        f.write(",".join(l[1] for l in CameraFields.ts_csv_fields) + "\n")
    sys.exit()


if __name__ == "__main__":
    opts = cli_options()
    if opts.version:
        from ._version import get_versions
        print("Version {}".format(get_versions()['version']))
        sys.exit(0)
    main(opts.config, debug=opts.debug, logdir=opts.logdir, n_threads=opts.threads)
