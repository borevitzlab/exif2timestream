from __future__ import print_function
from exif2timestream import cli_options, setup_logs, SkipImage, path_exists, bool_str, date_end, date, \
    int_time_hr_min, get_file_date, d2s, image_type_str, _dont_clobber, find_empty_dirs
import sys
import time
import csv
import logging
import os
import multiprocessing
import shutil
log = logging.getLogger("exif2timestream")
class CameraFields(object):
    """Validate input and translate between exif and config.csv fields."""
    # Validation functions, then schema, then the __init__ and execution

    ts_csv_fields = (
        ('use', 'USE', bool_str),
        ('timestream_name', 'TIMESTREAM_NAME', str),
        ('root_path', 'ROOT_PATH', path_exists),
        ('archive_dest', 'ARCHIVE_DEST', path_exists),
        ('expt_end', 'EXPT_END', date_end),
        ('expt_start', 'EXPT_START', date),
        ('start_time', 'START_TIME', int_time_hr_min),
        ('end_time', 'END_TIME', int_time_hr_min),
        ('image_types', 'IMAGE_TYPES', image_type_str),
        ('date_mask', 'DATE_MASK', str)
        )

    TS_CSV = dict((a, b) for a, b, c in ts_csv_fields)
    CSV_TS = {v: k for k, v in TS_CSV.items()}
    REQUIRED = {"use", "timestream_name", "root_path", "archive_dest", "expt_end", "expt_start", "start_time",
                "end_time", 'image_types'}
    SCHEMA = dict((a, c) for a, b, c in ts_csv_fields)

    def __init__(self, csv_config_dict):
        """Store csv settings as object attributes and validate."""
        csv_config_dict = {self.CSV_TS[k]: v for k, v in
                           csv_config_dict.items() if k in self.CSV_TS}
        if not all(key in csv_config_dict for key in self.REQUIRED):
            raise ValueError('CSV config dict lacks required key/s.' + str(csv_config_dict))

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
        self.root_path = local(self.root_path)
        self.archive_dest = local(self.archive_dest)
        log.debug("Validated camera '{}'".format(csv_config_dict))

def parse_camera_config_csv(filename):
    """Parse a camera configuration, yielding localised and validated
    camera configuration objects."""
    if filename is None:
        raise StopIteration
    with open(filename) as fh:

        cam_config = csv.DictReader(fh)
        cameras = []
        for camera in cam_config:
            try:
                camera = CameraFields(camera)
                if camera.use:
                    cameras.append(camera)
            except (SkipImage, ValueError):
                continue
        return cameras

def find_image_files(camera):
    """Scrape a directory for image files, by extension.
    Possibly, in future, use file magic numbers, but a bad idea on windows.
    """
    exts = camera.image_types
    ext_files = {}
    for ext in exts:
        src = camera.root_path

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
                if ext in (this_ext, "raw"):
                    fle_path = os.path.join(cur_dir, fle)
                    try:
                        ext_files[ext].append(fle_path)
                    except KeyError:
                        ext_files[ext] = []
                        ext_files[ext].append(fle_path)
            log.info("Found {0} {1} files for camera.".format(
                len(ext_files), ext))
    return ext_files

def process_image(args):
    log.debug("Starting to process image")
    image, camera, ext = args
    image_date = get_file_date(image, 0, round_secs=1,date_mask=camera.date_mask)
    time_tuple = (image_date.tm_hour, image_date.tm_min)
    if camera.expt_start > image_date or image_date > camera.expt_end:
        log.debug("Deleting {}. Outside of date range {} to {}".format(
            image, d2s(camera.expt_start), d2s(camera.expt_end)))
        # print("Deleting {}. Outside of date range {} to {}".format(
        #     image, d2s(camera.expt_start), d2s(camera.expt_end)))
    elif(camera.start_time > time_tuple or time_tuple > camera.end_time):
        log.debug("Deleting {}. Outside of Time range {} to {}".format(
            image, camera.start_time, camera.end_time))
        # print("Deleting {}. Outside of Time range {} to {}".format(
        #     image, camera.start_time, camera.end_time))
    else:
        log.debug("Will archive {}".format(image))
        archive_image = os.path.join(
            camera.archive_dest,
            os.path.basename(os.path.normpath(camera.root_path)),
            os.path.relpath(image, camera.root_path))
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
        os.unlink(image)
    except OSError:
        log.error("Could not delete '{0}'".format(image))
    log.debug("Deleted {}".format(image))
    find_empty_dirs(camera.root_path)

    # if camera.start_time > image_date


def process_timestream(camera, ext, images, n_threads=1):
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


def main(configfile, n_threads=1, logdir=None, debug=False):
    setup_logs(logdir, debug)
    start_time = time.time()
    n_images = 0
    for camera in parse_camera_config_csv(configfile):
        print("Processing Timestream {}".format(
            camera.timestream_name))
        print ("Archiving images between Times {}, {} and Dates {}, {}".format(camera.start_time, camera.end_time, time.strftime("%Y/%m/%d", camera.expt_start), time.strftime("%Y/%m/%d", camera.expt_end)))
        log.info("Processing Timestream {}".format(
            camera.timestream_name))
        log.info("Archiving images between Times {}, {} and Dates {}, {}".format(camera.start_time, camera.end_time, time.strftime("%Y/%m/%d", camera.expt_start), time.strftime("%Y/%m/%d", camera.expt_end)))

        print("Images are coming from {}, being put in {}".format(
            camera.root_path, camera.archive_dest))
        log.info("Images are coming from {}, being put in {}".format(
            camera.root_path, camera.archive_dest))

        for ext, images in find_image_files(camera).items():
            print(("Have {0} {1} images from this camera".format(
                len(images), ext)))
            log.info("Have {0} {1} images from this camera".format(
                len(images), ext))
            n_images += len(images)
            process_timestream(camera, ext, sorted(images), n_threads)

if __name__ == "__main__":
    opts = cli_options()
    if opts.version:
        from ._version import get_versions
        print("Version {}".format(get_versions()['version']))
        sys.exit(0)
    main(opts.config, debug=opts.debug, logdir='log', n_threads=opts.threads)