#!usr/bin/env python
"""Take somewhat structured image collections and outputs Timestream format."""
#pylint:disable=bad-whitespace,line-too-long,logging-format-interpolation

from __future__ import print_function

# Standard library imports
import argparse
import calendar
import csv
import itertools
import inspect
import json
import logging
import multiprocessing
import os
import re
import shutil
import sys
from time import strptime, strftime, mktime, localtime, struct_time, time
import warnings

# Module imports
import pexif
import exifread
import skimage
#import skimage.io
#import skimage.novice
from voluptuous import Required, Schema, MultipleInvalid

# versioneer
from _version import get_versions
__version__ = get_versions()['version']
del get_versions
EXIF_DATE_TAG = "Image DateTime"
EXIF_DATE_FMT = "%Y:%m:%d %H:%M:%S"
EXIF_DATE_MASK = EXIF_DATE_FMT
TS_V1_FMT = ("%Y/%Y_%m/%Y_%m_%d/%Y_%m_%d_%H/"
             "{tsname:s}_%Y_%m_%d_%H_%M_%S_{n:02d}.{ext:s}")
TS_V2_FMT = ("%Y/%Y_%m/%Y_%m_%d/%Y_%m_%d_%H/"
             "{tsname:s}_%Y_%m_%d_%H_%M_%S_{n:02d}.{ext:s}")
TS_DATE_FMT = "%Y_%m_%d_%H_%M_%S"
TS_FMT = TS_V1_FMT
TS_NAME_FMT = "{expt:s}-{loc:s}-c{cam:s}~{res:s}-{step:s}"
TS_NAME_STRUCT = "EXPT-LOCATION-CAM_NUM"
FULLRES_CONSTANTS = {"original", "orig", "fullres"}
IMAGE_TYPE_CONSTANTS = {"raw", "jpg"}
RAW_FORMATS = {"cr2", "nef", "tif", "tiff"}
IMAGE_SUBFOLDERS = {"raw", "jpg", "png", "tiff", "nef", "cr2"}
DATE_NOW_CONSTANTS = {"now", "current"}

def cli_options():
    """Return CLI arguments with argparse."""
    parser = argparse.ArgumentParser()
    parser.add_argument('-V', '--version',
                        help='Print version information.')
    parser.add_argument('--one', help='Use one core')
    parser.add_argument('-t', '--threads', type=int,
                        help='Number of processes to use.')
    parser.add_argument('-d', '--debug',
                        help='Enable debug logging (to file).')
    parser.add_argument('-l', '--logdir', default='.',
                        help='Directory to contain log files.')
    parser.add_argument('-c', '--config', help='Path to CSV camera '
                        'config file for normal operation.')
    parser.add_argument('-g', '--generate',  help='Generate a template'
                        ' camera configuration file at given path.')
    return parser.parse_args()


# Set up logging objects
NOW = strftime("%Y%m%dT%H%M%S", localtime())


# Map csv fields to camera dict fields. Should be 1 to 1, but is here for
# compability.
FIELDS = {
    'use': 'USE',
    'location': 'LOCATION',
    'expt': 'EXPT',
    'cam_num': 'CAM_NUM',
    'source': 'SOURCE',
    'destination': 'DESTINATION',
    'archive_dest': 'ARCHIVE_DEST',
    'expt_end': 'EXPT_END',
    'expt_start': 'EXPT_START',
    'interval': 'INTERVAL',
    'image_types': 'IMAGE_TYPES',
    'method': 'METHOD',
    'resolutions': 'resolutions',
    'sunrise': 'sunrise',
    'sunset': 'sunset',
    'timezone': 'camera_timezone',
    'user': 'user',
    'mode': 'mode',
    'project_owner': 'PROJECT_OWNER',
    'ts_structure': 'TS_STRUCTURE',
    'filename_date_mask': 'FILENAME_DATE_MASK',
    'orientation': 'ORIENTATION',
    'fn_parse': 'FN_PARSE',
    'fn_structure': 'FN_STRUCTURE'
}

FIELD_ORDER = [
    'use',
    'location',
    'expt',
    'cam_num',
    'source',
    'destination',
    'archive_dest',
    'expt_start',
    'expt_end',
    'interval',
    'image_types',
    'method',
    'resolutions',
    'sunrise',
    'sunset',
    'timezone',
    'user',
    'mode',
    'project_owner',
    'filename_date_mask',
    'orientation',
    'ts_structure',
    'fn_parse',
    'fn_structure'
]


class SkipImage(StopIteration):

    """
    Exception that specifically means skip this image.

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


def validate_camera(camera):
    """Validates and converts to python types the given camera dict (which
    normally has string values).
    """
    log = logging.getLogger("exif2timestream")

    def date(x):
        """Converter / validator for date field."""
        if isinstance(x, struct_time):
            return x
        else:
            if x.lower() in DATE_NOW_CONSTANTS:
                return localtime()
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
        elif isinstance(x, str):
            x = x.strip().lower()
            if x in {"t", "true", "y", "yes", "f", "false", "n", "no"}:
                return x in {"t", "true", "y", "yes"}
            return bool(int(x))
        raise ValueError

    def int_time_hr_min(x):
        """Validator for time field."""
        if isinstance(x, tuple):
            return x
        else:
            return (int(x) // 100, int(x) % 100)

    def path_exists(x):
        """Validator for path field."""
        if os.path.exists(x):
            return x
        else:
            raise ValueError("path '%s' doesn't exist" % x)

    def resolution_str(x):
        """Validator for resolution field."""
        if not isinstance(x, str):
            raise ValueError
        xs = x.strip().split('~')
        res_list = []
        for res in xs:
            # First, attempt splitting into X and Y components. Non <X>x<Y>
            # resolutions will be returned as a single item in a list,
            # hence the len(xy) below
            xy = res.strip().lower().split("x")
            if res in FULLRES_CONSTANTS:
                res_list.append(res)
            elif len(xy) == 2:
                # it's an XxY thing, hopefully
                x, y = xy
                x, y = int(x), int(y)
                res_list.append((x, y))
            else:
                # we'll pretend it's an int, for X resolution, and any ValueError
                # triggered here will be propagated to the vaildator
                res_list.append((int(res), None))
        return res_list

    def cam_pad_str(x):
        """Pads a numeric string to two digits."""
        if len(str(x)) == 1:
            return '0' + str(x)

    def image_type_str(x):
        """Validator for image type field."""
        if isinstance(x, list):
            return x
        if not isinstance(x, str):
            raise ValueError
        types = x.lower().strip().split('~')
        for t in types:
            if t not in IMAGE_TYPE_CONSTANTS:
                raise ValueError
        return types

    def remove_underscores(x):
        """Replaces '_' with '-'."""
        return x.replace("_", "-")


    class InList(object):
        #pylint:disable=too-few-public-methods
        def __init__(self, valid_values):
            if isinstance(valid_values, list) or isinstance(valid_values, tuple):
                self.valid_values = set(valid_values)

        def __call__(self, x):
            if x not in self.valid_values:
                raise ValueError
            return x

    sch = Schema({
        Required(FIELDS["use"]): bool_str,
        Required(FIELDS["destination"]): path_exists,
        Required(FIELDS["expt"]): remove_underscores,
        Required(FIELDS["cam_num"]): cam_pad_str,
        Required(FIELDS["expt_end"]): date,
        Required(FIELDS["expt_start"]): date,
        Required(FIELDS["image_types"]): image_type_str,
        Required(FIELDS["interval"], default=1): int,
        Required(FIELDS["location"]): remove_underscores,
        Required(FIELDS["archive_dest"]): path_exists,
        Required(FIELDS["method"], default="archive"):
            InList(["copy", "archive", "move", "resize", "json"]),
        Required(FIELDS["source"]): path_exists,
        FIELDS["mode"]: InList(["batch", "watch"]),
        FIELDS["resolutions"]: resolution_str,
        FIELDS["user"]: remove_underscores,
        FIELDS["sunrise"]: int_time_hr_min,
        FIELDS["sunset"]: int_time_hr_min,
        FIELDS["timezone"]: int_time_hr_min,
        FIELDS["project_owner"]: remove_underscores,
        FIELDS["ts_structure"]: str,
        FIELDS["filename_date_mask"]: str,
        FIELDS["orientation"]: str,
        FIELDS["fn_parse"]: str,
        FIELDS["fn_structure"]: str,
    })
    try:
        cam = sch(camera)
        log.debug("Validated camera '{0:s}'".format(cam))
        return cam
    except MultipleInvalid as e:
        if camera[FIELDS["use"]] != '0':
            raise e
        return None

def parse_structures(camera):
    """Parse the file structure of the camera for conversion to timestream format."""
    if (camera[FIELDS["ts_structure"]] is None or
            len(camera[FIELDS["ts_structure"]]) == 0):
        # If we dont have a ts_structure, then lets do the default one
        camera[FIELDS["ts_structure"]] = os.path.join(
            camera[FIELDS["expt"]].replace("_","-"),
            "{folder:s}",
            camera[FIELDS["expt"]].replace("_","-") + '-' + camera[FIELDS["location"]].replace("_","-") + \
            "-C{cam:s}~{res:s}-orig")
    else:
        # Replace the ts_structure with all the other stuff

        for key, value in camera.items():
            camera[FIELDS["ts_structure"]] = camera[FIELDS["ts_structure"]].replace(key.upper() ,str(value))
        # If it starts with a /, then we need to get rid of that
        if camera[FIELDS["ts_structure"]][0] == '/':
            camera[FIELDS["ts_structure"]] = camera[FIELDS["ts_structure"]][1:]
        # Split it up so we can add the "~orig~res" part
        camera[FIELDS["ts_structure"]] = camera[FIELDS["ts_structure"]].replace("_","-")
        direc, fname = os.path.split(camera[FIELDS["ts_structure"]])
        camera[FIELDS["ts_structure"]] = os.path.join(
            direc,
            "{folder:s}",
            (fname + "~" + "{res:s}" + "-orig")
            )
    if len(camera[FIELDS["fn_structure"]]) is 0 or not camera[FIELDS['fn_structure']]:
        camera[FIELDS["fn_structure"]] =  camera[FIELDS["expt"]].replace("_","-") + \
            '-' +  camera[FIELDS["location"]].replace("_","-") + \
            '-C' +  camera[FIELDS["cam_num"]].replace("_","-") +\
            '~{res:s}-orig'
    else:
        for key, value in camera.items():
            camera[FIELDS["fn_structure"]] = camera[FIELDS["fn_structure"]].replace(key.upper() ,str(value))
        camera[FIELDS["fn_structure"]] = camera[FIELDS["fn_structure"]].replace("/", "")
    return camera


def resize_function(camera, image_date, dest):
    """Create a resized image in a new location."""
    print ("Resize Function")
    log = logging.getLogger("exif2timestream")
    # Resize a single image, to its new location
    log.debug(
        "Now checking if we have 1 or two resolution arguments on image '{0:s}'".format(dest))
    if camera[FIELDS["resolutions"]][1][1] is None:
        # Read in image dimensions
        img = skimage.io.imread(dest).shape
        # Calculate the new image dimensions from the old one
        new_res = camera[FIELDS["resolutions"]][1][
            0], (img[0] * camera[FIELDS["resolutions"]][1][0]) / img[1]
        log.debug("One resolution arguments, '{0:d}'".format(new_res[0]))
    else:
        new_res = camera[FIELDS["resolutions"]][1]
        log.debug(
            "Two resolution arguments, '{:d}' x '{:d}'".format(new_res[0], new_res[1]))
    log.debug("Now getting Timestream name")
    # Get the timestream name to save the image as
    ts_name = make_timestream_name(camera, res=new_res[0], step="orig")
    # Get the full output file name from the ts_name and the image date
    resizing_temp_outname = get_new_file_name(
        image_date, ts_name)
    # Based on the value of ts_structure, combine to form a full image path
    resized_img = os.path.join(
        camera[FIELDS["destination"]],
        camera[FIELDS["ts_structure"]].format(folder='outputs', res=str(new_res[0]), cam=camera[FIELDS["cam_num"]], step='outputs'),
        resizing_temp_outname)
    # If the resized image already exists, then just return
    if os.path.isfile(resized_img):
        return
    log.debug(
        "Full resized filename which we will output to is '{0:s}'".format(resized_img))
    resized_img_path = os.path.dirname(resized_img)
    log.debug(
        "Now checking if image path already exists, if it does, skipping")
    if not os.path.exists(resized_img_path):
        try:
            os.makedirs(resized_img_path)
        except OSError:
            log.warn("Could not make dir '{0:s}', skipping image '{1:s}'".format(
                resized_img_path, resized_img))
            raise SkipImage
    log.debug("Now actually resizing image to '{0:s}'".format(dest))
    resize_img(dest, resized_img, new_res[0], new_res[1])

# Function which only performs the actual resize.


def resize_img(filename, destination, to_width, to_height):
    """Actually resizes the image."""
    print ("Resize Image")
    log = logging.getLogger("exif2timestream")
    # Open the Image and get its width
    img = skimage.io.imread(filename)
    # Resize the image
    log.debug("Now resizing the image")
    img = skimage.transform.resize(img, (to_height, to_width))
    # read in old exxif data
    exif_source = pexif.JpegFile.fromFile(filename)
    # Save image
    log.debug("Saving Image")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        skimage.io.imsave(destination, img)
    # Write new exif data from old image
    try:
        exif_dest = pexif.JpegFile.fromFile(destination)
        exif_dest.exif.primary.ExtendedEXIF.DateTimeOriginal = \
            exif_source.exif.primary.ExtendedEXIF.DateTimeOriginal
        exif_dest.exif.primary.Orientation = \
            exif_source.exif.primary.Orientation
        exif_dest.writeFile(destination)
        log.debug("Successfully copied exif data also")
    except AttributeError:
        log.debug("Unable to copy over some exif data")


def get_time_from_filename(filename, mask=EXIF_DATE_MASK):
    """Replace the year with the regex equivalent to parse."""
    regex_mask = mask.replace("%Y", r"\d{4}").replace(
        "%m", r"\d{2}").replace("%d", r"\d{2}")
    regex_mask = regex_mask.replace("%H", r"\d{2}").replace(
        "%M", r"\d{2}").replace("%S", r"\d{2}")
    # Wildcard character before and after the regex
    regex_mask = r"\.*" + regex_mask + r"\.*"
    # compile the regex
    date_reg_exp = re.compile(regex_mask)
    # get the list of possible date matches
    matches_list = date_reg_exp.findall(filename)
    for match in matches_list:
        # Attempt to parse each match into a datetime; return first success
        try:
            datetime = strptime(match, mask)
            return datetime
        except ValueError:
            continue
    return None


def write_exif_date(filename, date_time):
    """Change an image timestamp."""
    try:
        # Read in the file
        img = pexif.JpegFile.fromFile(filename)
        # Edit the exif data
        img.exif.primary.ExtendedEXIF.DateTimeOriginal = strftime(
            EXIF_DATE_FMT, date_time)
        # Write to the file
        img.writeFile(filename)
        return True
    except IOError:
        return False


def get_file_date(filename, round_secs=1):
    """
    Gets a time.struct_time from an image's EXIF, or None if not possible.
    """
    log = logging.getLogger("exif2timestream")
    # Now uses Pexif
    try:
        exif_tags = pexif.JpegFile.fromFile(filename)
        str_date = exif_tags.exif.primary.ExtendedEXIF.DateTimeOriginal
        date = strptime(str_date, EXIF_DATE_FMT)
        # print (date)
    except AttributeError:
        # Try and Grab datetime from the filename
        # Grab only the filename, not the directory
        shortfilename = os.path.basename(filename)
        log.debug("No Exif data in '{0:s}', attempting to read from filename".format(
            shortfilename))
        # Try and grab the date
        # We can put a custom mask in here if we want
        date = get_time_from_filename(filename)
        if date is None:
            log.debug(
                "Unable to scrape date from '{0:s}'".format(shortfilename))
            return None
        else:
            if not write_exif_date(filename, date):
                log.debug("Unable to write Exif Data")
                return None
            return date
    # If its not a jpeg, we have to open with exif reader
    except pexif.JpegFile.InvalidFile:
        shortfilename = os.path.basename(filename)
        log.debug("Unable to Read file '{0:s}', aparently not a jpeg".format(shortfilename))
        with open(filename, "rb") as fh:
            exif_tags = exifread.process_file(
                fh, details=False, stop_tag=EXIF_DATE_TAG)
            try:
                str_date = exif_tags[EXIF_DATE_TAG].values
                date = strptime(str_date, EXIF_DATE_FMT)
            except KeyError:
                return None
    if round_secs > 1:
        date = round_struct_time(date, round_secs)
    log.debug("Date of '{0:s}' is '{1:s}'".format(filename, d2s(date)))
    return date


def get_new_file_name(date_tuple, ts_name, n=0, fmt=TS_FMT, ext="jpg"):
    """
    Gives the new file name for an image within a timestream, based on
    datestamp, timestream name, sub-second series count and extension.
    """
    log = logging.getLogger("exif2timestream")
    if date_tuple is None or not date_tuple:
        log.error("Must supply get_new_file_name with a valid date." +
                  "Date is '{0:s}'".format(d2s(date_tuple)))
        raise ValueError("Must supply get_new_file_name with a valid date.")
    if not ts_name:
        log.error("Must supply get_new_file_name with timestream name." +
                  "TimeStream name is '{0:s}'".format(ts_name))
        raise ValueError("Must supply get_new_file_name with timestream name.")
    date_formatted_name = strftime(fmt, date_tuple)
    name = date_formatted_name.format(tsname=ts_name, n=n, ext=ext)
    log.debug("New filename is '{0:s}'".format(name))
    return name


def round_struct_time(in_time, round_secs, tz_hrs=0, uselocal=True):
    """
    Round a struct_time object to any time interval in seconds
    """
    log = logging.getLogger("exif2timestream")
    seconds = mktime(in_time)
    rounded = int(round(seconds / float(round_secs)) * round_secs)
    if not uselocal:
        rounded -= tz_hrs * 60 * 60  # remove tz seconds, back to UTC
    retval = localtime(rounded)
    # This is hacky as fuck. We need to replace stuff from time module with
    # stuff from datetime, which actually fucking works.
    rv_list = list(retval)
    rv_list[8] = in_time.tm_isdst
    rv_list[6] = in_time.tm_wday
    retval = struct_time(tuple(rv_list))
    log.debug("time {0:s} rounded to {1:d} seconds is {2:s}".format(
        d2s(in_time), round_secs, d2s(retval)))
    return retval


def make_timestream_name(camera, res="fullres", step="orig", folder='original'):
    """
    Makes a timestream name given the format (module-level constant), step,
    resolution and a camera object.
    """
    if isinstance(res, tuple):
        res = "x".join([str(x) for x in res])
    # raise ValueError(str((camera, res, step)))
    ts_name = camera[FIELDS["fn_structure"]].format(
        expt=camera[FIELDS["expt"]],
        loc=camera[FIELDS["location"]],
        cam=camera[FIELDS["cam_num"]],
        res=str(res),
        step=step,
        folder = folder
        )
    return ts_name


def timestreamise_image(image, camera, subsec=0, step="orig"):
    """Process a single image, mv/cp-ing it to its new location"""
    log = logging.getLogger("exif2timestream")
    # Edit the global variable for the date mask
    global EXIF_DATE_MASK
    EXIF_DATE_MASK = camera[FIELDS["filename_date_mask"]]
    image_date = get_file_date(image, camera[FIELDS["interval"]] * 60)
    if not image_date:
        log.warn("Couldn't get date for image {}".format(image))
        raise SkipImage
    in_ext = os.path.splitext(image)[-1].lstrip(".")
    ts_name = make_timestream_name(camera, res="fullres", step=step)
    out_image = get_new_file_name(
        image_date,
        ts_name,
        n=subsec,
        ext=in_ext
    )
    out_image = os.path.join(
        camera[FIELDS["destination"]],
        camera[FIELDS["ts_structure"]].format(folder='original', res='fullres', cam=camera[FIELDS["cam_num"]], step='orig'),
        out_image
    )
    # make the target directory
    out_dir = os.path.dirname(out_image)
    if not os.path.exists(out_dir):
        try:
            os.makedirs(out_dir)
        except OSError:
            log.warn("Could not make dir '{0:s}', skipping image '{1:s}'".format(out_dir, image))
            raise SkipImage
    # And do the copy
    dest = _dont_clobber(out_image, mode=SkipImage)

    try:
        shutil.copy(image, dest)
        log.info("Copied '{0:s}' to '{1:s}".format(image, dest))
    except:
        log.warn("Couldnt copy '{0:s}' to '{1:s}', skipping image".format(
            image, dest))
        raise SkipImage
    if len(camera[FIELDS["orientation"]]) > 0:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            rotations = {'1':270, '2':180, '-1':90}
            orientation = camera[FIELDS["orientation"]]
            if orientation in rotations:
                img = skimage.io.imread(dest)
                img = skimage.transform.rotate(
                    img, rotations[orientation], resize=True)
                skimage.io.imsave(dest, img)
    if len(camera[FIELDS["resolutions"]]) > 1:
        log.info("Going to resize image '{0:s}'".format(dest))
        resize_function(camera, image_date, dest)

def _dont_clobber(fn, mode="append"):
    """Ensure we don't overwrite things, using a variety of methods"""
    #TODO:  will clobber something different if the new filename exists
    log = logging.getLogger("exif2timestream")
    if os.path.exists(fn):
        # Deal with SkipImage or StopIteration exceptions
        if isinstance(mode, StopIteration):
            log.debug("Path '{0}' exists, raising StopIteration".format(fn))
            raise mode
        # Ditto, but if we pass them uninstantiated
        elif inspect.isclass(mode) and issubclass(mode, StopIteration):
            log.debug("Path '{0}' exists, raising an Exception".format(fn))
            raise mode()
        # Otherwise, append something '_1' to the file name to solve our
        # problem
        elif mode == "append":
            log.debug("Path '{0}' exists, adding '_1' to its name".format(fn))
            base, ext = os.path.splitext(fn)
            # append _1 to filename
            if ext != '':
                return ".".join(["_".join([base, "1"]), ext])
            else:
                return "_".join([base, "1"])
        else:
            raise ValueError("Bad _dont_clobber mode: %r", mode)
    else:
        # Doesn't exist, so return good path
        log.debug("Path '{0}' doesn't exist. Returning it.".format(fn))
        return fn


def process_image(args):
    """
    Given a camera config and list of images, will do the required
    move/copy operations.
    """
    print ("Process Image")
    log = logging.getLogger("exif2timestream")
    log.debug("Starting to process image")
    (image, camera, ext) = args
    #EXIF_DATE_MASK = camera[FIELDS["filename_date_mask"]]
    image_date = get_file_date(image, camera[FIELDS["interval"]] * 60)
    if image_date < camera[FIELDS["expt_start"]] or \
            image_date > camera[FIELDS["expt_end"]]:
        log.debug("Skipping {}. Outside of date range {} to {}".format(
            image, d2s(camera[FIELDS["expt_start"]]),
            d2s(camera[FIELDS["expt_end"]])))
        return  # Don't raise SkipImage as it isn't caught

    # archive a backup before we fuck anything up
    if camera[FIELDS["method"]] == "archive":
        log.debug("Will archive {}".format(image))
        ts_name = make_timestream_name(camera, res="fullres")
        archive_image = os.path.join(
            camera[FIELDS["archive_dest"]],
            camera[FIELDS["expt"]],
            ext,
            ts_name,
            os.path.basename(image)
        )
        archive_dir = os.path.dirname(archive_image)
        if not os.path.exists(archive_dir):
            try:
                os.makedirs(archive_dir)
            except OSError as exc:
                if not os.path.exists(archive_dir):
                    raise exc
            log.debug("Made archive dir {}".format(archive_dir))
        archive_image = _dont_clobber(archive_image)
        shutil.copy2(image, archive_image)
        log.debug("Copied {} to {}".format(image, archive_image))
    if camera[FIELDS["method"]] == "resize":
        resize_function(camera, image_date, image)
        log.debug("Rezied Image {}".format(image))
    if camera[FIELDS["method"]] == "json":
        return
        # We have a method to resize everything. Lets do that then.

    # TODO: BUG: this won't work if images aren't in chronological order. Which
    # they never will be.
    # if last_date == image_date:
    # increment the sub-second counter
    #    subsec += 1
    # else:
    # we've moved to the next time, so 0-based subsec counter == 0
    if ext.lower() == "raw" or ext.lower() in RAW_FORMATS:
        step = "raw"
    else:
        step = "orig"
    subsec = 0
    try:
        # deal with original image (move/copy etc)

        timestreamise_image(image, camera, subsec=subsec, step=step)
        log.debug("Successfully timestreamed {}".format(image))
    except SkipImage:
        log.debug("Failed to timestream {} (got SkipImage)".format(image))
        if camera[FIELDS["method"]] == "archive":
            # we have changed this so that all images are moved to the archive
            pass
        else:
            return  # don't delete skipped images if we haven't archived them
    if camera[FIELDS["method"]] in {"move", "archive"}:
        # images have already been archived above, so just delete originals
        try:
            os.unlink(image)
        except OSError:
            log.error("Could not delete '{0}'".format(image))
        log.debug("Deleted {}".format(image))

        # Resize the fields here


def get_local_path(this_path):
    """Replaces slashes with the correct kind for local system paths."""
    return this_path.replace("/", os.path.sep).replace("\\", os.path.sep)


def localise_cam_config(camera):
    """Make camera use localised settings, e.g. path separators"""
    if camera is None:
        return None
    camera[FIELDS["source"]] = get_local_path(camera[FIELDS["source"]])
    camera[FIELDS["archive_dest"]] = get_local_path(
        camera[FIELDS["archive_dest"]])
    camera[FIELDS["destination"]] = get_local_path(
        camera[FIELDS["destination"]])
    return camera


def parse_camera_config_csv(filename):
    """
    Parse a camera configuration
    It yields localised, validated camera dicts
    """
    if filename is None:
        raise StopIteration
    with open(filename) as fh:
        cam_config = csv.DictReader(fh)
        for camera in cam_config:
            camera = validate_camera(camera)
            camera = localise_cam_config(camera)
            if camera is not None and camera[FIELDS["use"]]:
                yield parse_structures(camera)


def find_image_files(camera):
    """
    Scrape a directory for image files, by extension.
    Possibly, in future, use file magic numbers, but a bad idea on windows.
    """
    log = logging.getLogger("exif2timestream")
    exts = camera[FIELDS["image_types"]]
    ext_files = {}
    for ext in exts:
        src = camera[FIELDS["source"]]
        lst = [x for x in os.listdir(src) if not x[0] in ('.', '_')]
        log.debug("List of src valid subdirs is {}".format(lst))
        for node in lst:
            log.debug("Found src subdir {}".format(node))
            if node.lower() == ext:
                src = os.path.join(src, node)
                break
        log.info("Walking from {} to find images".format(src))
        for cur_dir, dirs, files in os.walk(src):
            for d in dirs:
                if not (d.lower() in IMAGE_SUBFOLDERS or d.startswith("_")):
                    if camera[FIELDS["method"]] in ("resize", "json"):
                        log.error("Source directory has too many subdirs.")
            for fle in files:
                this_ext = os.path.splitext(fle)[-1].lower().strip(".")
                if this_ext == ext or ext == "raw" and this_ext in RAW_FORMATS:
                    fle_path = os.path.join(cur_dir, fle)
                    if camera[FIELDS["fn_parse"]] in fle_path:
                        try:
                            ext_files[ext].append(fle_path)
                        except KeyError:
                            ext_files[ext] = []
                            ext_files[ext].append(fle_path)
            log.info("Found {0} {1} files for camera.".format(
                len(ext_files), ext))
    return ext_files


def generate_config_csv(filename):
    """Make a config csv template"""
    with open(filename, "w") as fh:
        fh.write(",".join([FIELDS[x] for x in FIELD_ORDER]))
        fh.write("\n")


def setup_logs():
    """Sets up logging using the log logger object."""
    log = logging.getLogger("exif2timestream")
    if opts.generate:
        # No logging when we're just generating a config file. What could
        # possibly go wrong...
        null = logging.NullHandler()
        log.addHandler(null)
        generate_config_csv(opts.generate)
        sys.exit()
    # we want logging for the real main loop
    fmt = logging.Formatter(
        '%(asctime)s - %(name)s.%(funcName)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    ch.setFormatter(fmt)
    logdir = opts.logdir
    if not os.path.exists(logdir):
        logdir = "."
    log_fh = logging.FileHandler(os.path.join(logdir, "e2t_" + NOW + ".log"))
    log_fh.setLevel(logging.INFO)
    log_fh.setFormatter(fmt)
    log.addHandler(log_fh)
    if opts.debug:
        debug_fh = logging.FileHandler(
            os.path.join(logdir, "e2t_" + NOW + ".debug"))
        debug_fh.setLevel(logging.DEBUG)
        debug_fh.setFormatter(fmt)
        log.addHandler(debug_fh)
    log.addHandler(ch)
    log.setLevel(logging.DEBUG)


def main():
    """The main loop of the module, do the renaming in parallel etc."""
    log = logging.getLogger("exif2timestream")
    setup_logs()
    # beginneth the actual main loop
    start_time = time()
    cameras = parse_camera_config_csv(opts.config)
    n_images = 0
    json_dump = []
    for camera in cameras:
        msg = "Processing experiment {}, location {}\n".format(
            camera[FIELDS["expt"]],
            camera[FIELDS["location"]],
        )
        msg += "Images are coming from {}, being put in {}".format(
            camera[FIELDS["source"]],
            camera[FIELDS["destination"]],
        )
        log.info(msg)
        for ext, images in find_image_files(camera).items():
            images = sorted(images)
            try:
                image_resolution = skimage.novice.open(images[0]).size
            except IOError:
                image_resolution = (0,0)
            log.info("Have {0} {1} images from this camera".format(
                len(images), ext))
            n_images += len(images)
            #last_date = None
            #subsec = 0
            count = 0
            if len(camera[FIELDS["resolutions"]]) > 1:
                folder = "outputs"
                if camera[FIELDS["resolutions"]][1][1] is None:
                    new_res = camera[FIELDS["resolutions"]][1][
                        0], (image_resolution[0] * camera[FIELDS["resolutions"]][1][0]) / image_resolution[1]
                else:
                    new_res = camera[FIELDS["resolutions"]][1]
                res = new_res[0]
            else:
                folder = "original"
                res = 'fullres'
                new_res = image_resolution
            if "a_data" in camera[FIELDS["destination"]]:
                if camera[FIELDS["ts_structure"]]:
                    webrootaddr = "http://phenocam.anu.edu.au/cloud/data" + \
                        camera[FIELDS["destination"]].split(
                            "a_data")[1] + camera[FIELDS["ts_structure"]] + '/'
                else:
                    webrootaddr = "http://phenocam.anu.edu.au/cloud/data" + \
                        camera[FIELDS["destination"]].split(
                            "a_data")[1] + camera[FIELDS["location"]] + '/'
            else:
                webrootaddr = None
            thumb_image = []
            if n_images > 3:
                thumb_image = [n + (n_images / 2) for n in (- 1, 0, 1)]
                for i in range(3):
                    image_date = get_file_date(
                        images[thumb_image[i]], camera[FIELDS["interval"]] * 60)
                    thumb_image[i] = make_timestream_name(
                        camera, new_res[0], 'orig')
                    ts_image = get_new_file_name(image_date, thumb_image[i])
                    ts_path, ts_fname = os.path.split(camera[FIELDS["ts_structure"]])
                    thumb_image[i] = (os.path.join(camera[FIELDS["destination"]] ,ts_path, folder, ts_fname + '~' + str(res) + '-orig' , ts_image))
                    if "a_data" in thumb_image[i]:
                        thumb_image[i] = webrootaddr + thumb_image[i].split("a_data")[1]
                    else:
                        thumb_image[i] = ''
            if camera[FIELDS["orientation"]] in ("1", "-1"):
                j_width_hires = str(image_resolution[1])
                j_height_hires = str(image_resolution[0])
            else:
                j_width_hires=str(image_resolution[0]),
                j_height_hires = str(image_resolution[1])

            # TODO: sort out the whole subsecond clusterfuck
            if opts.single:
                log.info("Using 1 process (What is this? Fucking 1990?)")
                for image in images:
                    count += 1
                    print("Processed {: 5d} Images".format(count), end='\r')
                    process_image((image, camera, ext))
            else:
                threads = max(multiprocessing.cpu_count() - 1, 1)
                if opts.threads:
                    threads = opts.threads

                log.info("Using {0:d} processes".format(threads))
                # set the function's camera-wide arguments
                args = zip(images, itertools.cycle([camera]), itertools.cycle([ext]))
                pool = multiprocessing.Pool(threads)
                for _ in pool.imap(process_image, args):
                    count += 1
                    print("Processed {: 5d} Images".format(count), end='\r')
                pool.close()
                pool.join()


            json_dump.append((dict(
                name=str(
                    camera[FIELDS["expt"]] + '-' + (camera[FIELDS["location"]])),
                utc="false",
                width_hires=str(j_width_hires),
                ts_version=str(1),
                ts_end=str(calendar.timegm(camera[FIELDS["expt_end"]])),
                image_type=str([FIELDS["image_types"]][0]),
                height_hires=str(j_height_hires),
                expt=str(camera[FIELDS["expt"]]),
                width=str(new_res[0]),
                webroot=str(webrootaddr),
                period_in_minutes=str(camera[FIELDS["interval"]]),
                timezone=str(camera[FIELDS["timezone"]][0]),
                ts_start=str(calendar.timegm(camera[FIELDS["expt_start"]])),
                height=str(new_res[1]),
                access=str(0),
                thumbnails=str(thumb_image)
            )))
            print("Processed {: 5d} Images. Finished this cam!".format(count))
            jpath = os.path.dirname(
                camera[FIELDS["ts_structure"]].format(folder="", res="", cam=''))
            if not os.path.exists(os.path.join(camera[FIELDS["destination"]], jpath)):
                try:
                    os.makedirs(os.path.join(camera[FIELDS["destination"]], jpath))
                except OSError:
                    log.warn("Could not make dir '{0:s}', skipping image '{1:s}'".format(
                        jpath, image))
            with open(os.path.join(camera[FIELDS["destination"]], jpath, 'camera.json'), 'a+') as f:
                json.dump(json_dump, f)
    secs_taken = time() - start_time
    print("\nProcessed a total of {0} images in {1:.2f} seconds".format(n_images, secs_taken))


if __name__ == "__main__":
    opts = cli_options()
    if opts.version:
        print("Version {}".format(__version__))
        sys.exit(0)
    main()
