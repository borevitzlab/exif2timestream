#!usr/bin/env python
"""Tests for the exif2timestream module."""

# Standard library imports
import copy
import hashlib
import os
from os import path
import shutil
import tempfile
import time
import unittest

# Module imports
import exif2timestream as e2t
import pexif

SKIMAGE = True
try:
    from skimage import novice
except ImportError:
    SKIMAGE = False


class TestExifTraitcapture(unittest.TestCase):
    dirname = path.dirname(__file__)
    test_config_csv = path.join(dirname, "config.csv")
    test_config_dates_csv = path.join(dirname, "config_dates.csv")
    test_config_raw_csv = path.join(dirname, "config_raw.csv")
    bad_header_config_csv = path.join(dirname, "bad_header_config.csv")
    bad_values_config_csv = path.join(dirname, "bad_values_config.csv")
    unused_bad_cam_csv = path.join(dirname, "unused_cams_with_bad_values.csv")
    out_dirname = path.join(dirname, "out")
    camupload_dir = path.join(dirname, "img", "camupload")
    noexif_testfile = path.join(dirname, "img", "IMG_0001_NOEXIF.JPG")
    jpg_testfile = path.join(camupload_dir, "jpg", "IMG_0001.JPG")
    raw_testfile = path.join(camupload_dir, "raw", "IMG_0001.CR2")
    camera_win32 = {
        'ARCHIVE_DEST': '\\'.join([out_dirname, 'archive']),
        'EXPT': 'BVZ00000',
        'DESTINATION': '\\'.join([out_dirname, 'timestreams']),
        'CAM_NUM': 1,
        'EXPT_END': '2013_12_31',
        'EXPT_START': '2013_11_01',
        'LOCATION': 'EUC-R01C01',
        'METHOD': 'copy',
        'IMAGE_TYPES': 'raw~jpg',
        'INTERVAL': '5',
        'mode': 'batch',
        'resolutions': 'original',
        'SOURCE': '\\'.join([dirname, "img", "camupload"]),
        'sunrise': '500',
        'sunset': '2200',
        'camera_timezone': '1100',
        'USE': '1',
        'user': 'Glasshouses',
        'TS_STRUCTURE': os.path.join(
            'BVZ00000', '{folder}', 'BVZ00000-EUC-R01C01-C01~{res}-{step}'),
        'FN_PARSE': '',
        'PROJECT_OWNER': '',
        'FILENAME_DATE_MASK': ' ',
        'FN_STRUCTURE': 'BVZ00000-EUC-R01C01-C01~{res}-{step}',
        'ORIENTATION': ''
    }
    camera_unix = {
        'ARCHIVE_DEST': '/'.join([out_dirname, 'archive']),
        'EXPT': 'BVZ00000',
        'DESTINATION': '/'.join([out_dirname, 'timestreams']),
        'CAM_NUM': 1,
        'EXPT_END': '2013_12_31',
        'EXPT_START': '2013_11_01',
        'LOCATION': 'EUC-R01C01',
        'METHOD': 'copy',
        'INTERVAL': '5',
        'IMAGE_TYPES': 'raw~jpg',
        'mode': 'batch',
        'resolutions': 'original',
        'SOURCE': '/'.join([dirname, "img", "camupload"]),
        'sunrise': '500',
        'sunset': '2200',
        'camera_timezone': '1100',
        'USE': '1',
        'user': 'Glasshouses',
        'TS_STRUCTURE': os.path.join(
            'BVZ00000', '{folder}', 'BVZ00000-EUC-R01C01-C01~{res}-{step}'),
        'FN_PARSE': '',
        'PROJECT_OWNER': '',
        'FILENAME_DATE_MASK': " ",
        'FN_STRUCTURE': 'BVZ00000-EUC-R01C01-C01~{res}-{step}',
        'ORIENTATION': ''

    }

    r_fullres_path = path.join(
        out_dirname, "timestreams", "BVZ00000", "original",
        'BVZ00000-EUC-R01C01-C01~fullres-orig', '2013', '2013_11',
        '2013_11_12', '2013_11_12_20',
        'BVZ00000-EUC-R01C01-C01~fullres-orig_2013_11_12_20_55_00_00.JPG'
    )

    r_raw_path = path.join(
        out_dirname, "timestreams", "BVZ00000", "original",
        'BVZ00000-EUC-R01C01-C01~fullres-raw', '2013', '2013_11',
        '2013_11_12', '2013_11_12_20',
        'BVZ00000-EUC-R01C01-C01~fullres-raw_2013_11_12_20_55_00_00.CR2'
    )

    maxDiff = None

    # helpers
    def _md5test(self, filename, expected_hash):
        with open(filename, "rb") as fh:
            out_contents = fh.read()
        md5hash = hashlib.md5()
        md5hash.update(out_contents)
        md5hash = md5hash.hexdigest()
        self.assertEqual(md5hash, expected_hash)

    # setup
    def setUp(self):
        cam = self.camera_unix if path.sep == "/" else self.camera_win32
        self.camera_raw = copy.deepcopy(cam)
        self.camera = copy.deepcopy(cam)
        mapping = e2t.CameraFields.TS_CSV
        img_dir = path.dirname(self.camera[mapping['source']])
        for dir_path in (
                img_dir, self.out_dirname,
                self.camera[mapping['destination']],
                self.camera[mapping['archive_dest']]):
            try:
                os.makedirs(dir_path)
            except OSError as e:
                if not os.path.isdir(dir_path):
                    raise e
        shutil.rmtree(img_dir)
        shutil.copytree("./test/unburnable", img_dir)
        self.camera = e2t.CameraFields(self.camera)

    # test for localise_cam_config
    def test_localise_cam_config(self):
        self.assertEqual(
            set(dir(e2t.CameraFields(self.camera_win32))),
            set(dir(e2t.CameraFields(self.camera_unix))))

    # tests for round_struct_time
    def test_round_struct_time_gmt(self):
        start = time.strptime("20131112 205309", "%Y%m%d %H%M%S")
        rnd_5 = e2t.round_struct_time(start, 300, tz_hrs=11, uselocal=False)
        rnd_5_expt = time.strptime("20131112 095500", "%Y%m%d %H%M%S")
        self.assertIsInstance(rnd_5, time.struct_time)
        self.assertEqual(time.mktime(rnd_5), time.mktime(rnd_5_expt))

    def test_round_struct_time_local(self):
        start = time.strptime("20131112 205309", "%Y%m%d %H%M%S")
        rnd_5 = e2t.round_struct_time(start, 300, tz_hrs=11)
        rnd_5_expt = time.strptime("20131112 205500", "%Y%m%d %H%M%S")
        self.assertIsInstance(rnd_5, time.struct_time)
        self.assertEqual(time.mktime(rnd_5), time.mktime(rnd_5_expt))

    # tests for _dont_clobber
    def test_dont_clobber(self):
        stop = e2t.SkipImage()
        fh = tempfile.NamedTemporaryFile()
        fn = fh.name
        # test raise/exception mode
        with self.assertRaises(e2t.SkipImage):
            e2t._dont_clobber(fn, mode=e2t.SkipImage)
        with self.assertRaises(e2t.SkipImage):
            e2t._dont_clobber(fn, mode=stop)
        # test with bad mode
        with self.assertRaises(ValueError):
            e2t._dont_clobber(fn, mode="BADMODE")
        # test append mode
        expt = fn + "_1"
        self.assertEqual(e2t._dont_clobber(fn), expt)
        # test append mode with file extension
        fn_ext = fn + ".txt"
        with open(fn_ext, "w") as fh:
            fh.write("This file will exist")  # make a file with an extension
        e_base, e_ext = path.splitext(fn_ext)
        expt = ".".join(["_".join([e_base, "1"]), e_ext])
        self.assertEqual(e2t._dont_clobber(fn_ext), expt)
        os.unlink(fn_ext)  # we have to remove this ourselves
        # test append mode with file that doesn't exist
        wontexist = fn + "_shouldnteverexist"
        self.assertEqual(e2t._dont_clobber(wontexist), wontexist)

    # tests for get_file_date
    def test_get_file_date_jpg(self):
        actual = time.strptime("20131112 205309", "%Y%m%d %H%M%S")
        jpg_date = e2t.get_file_date(self.jpg_testfile, 0)
        self.assertEqual(jpg_date, actual)

    def test_get_file_date_raw(self):
        actual = time.strptime("20131112 205309", "%Y%m%d %H%M%S")
        raw_date = e2t.get_file_date(self.raw_testfile, 0)
        self.assertEqual(raw_date, actual)

    def test_get_file_date_noexif(self):
        date = e2t.get_file_date(self.noexif_testfile, 0)
        self.assertIsNone(date)

    # tests for get_new_file_name
    def test_get_new_file_name(self):
        date = time.strptime("20131112 205309", "%Y%m%d %H%M%S")
        fn = e2t.get_new_file_name(date, 'test')
        self.assertEqual(fn, ("2013/2013_11/2013_11_12/2013_11_12_20/"
                              "test_2013_11_12_20_53_09_00.jpg"))

    def test_get_new_file_date_from_file(self):
        date = e2t.get_file_date(self.jpg_testfile, 0)
        fn = e2t.get_new_file_name(date, 'test')
        self.assertEqual(fn, ("2013/2013_11/2013_11_12/2013_11_12_20/"
                              "test_2013_11_12_20_53_09_00.jpg"))
        date = e2t.get_file_date(self.jpg_testfile, 0, round_secs=5 * 60)
        fn = e2t.get_new_file_name(date, 'test')
        self.assertEqual(fn, ("2013/2013_11/2013_11_12/2013_11_12_20/"
                              "test_2013_11_12_20_55_00_00.jpg"))

    def test_get_new_file_nulls(self):
        date = time.strptime("20131112 205309", "%Y%m%d %H%M%S")
        with self.assertRaises(e2t.SkipImage):
            e2t.get_new_file_name(None, 'test')
        with self.assertRaises(e2t.SkipImage):
            e2t.get_new_file_name(date, '')
        with self.assertRaises(e2t.SkipImage):
            e2t.get_new_file_name(date, None)
        with self.assertRaises(e2t.SkipImage):
            e2t.get_new_file_name(None, '')

    # tests for make_timestream_name
    def test_make_timestream_name_empty(self):
        name = e2t.make_timestream_name(self.camera)
        exp = 'BVZ00000-EUC-R01C01-C01~fullres-orig'
        self.assertEqual(name, exp)

    def test_make_timestream_name_params(self):
        name = e2t.make_timestream_name(
            self.camera,
            res="1080x720",
            step="clean")
        exp = 'BVZ00000-EUC-R01C01-C01~1080x720-clean'
        self.assertEqual(name, exp)

    # tests for find_image_files
    def test_find_image_files(self):
        expt = {"jpg": {path.join(self.camupload_dir, x) for x in [
                        'jpg/IMG_0001.JPG',
                        'jpg/IMG_0002.JPG',
                        'jpg/IMG_0630.JPG',
                        'jpg/IMG_0633.JPG',
                        'jpg/whroo20131104_020255M.jpg']
                        },
                "raw": {path.join(self.camupload_dir, 'raw/IMG_0001.CR2')},
                }
        got = e2t.find_image_files(self.camera)
        self.assertSetEqual(set(got["jpg"]), expt["jpg"])
        self.assertSetEqual(set(got["jpg"]), expt["jpg"])

    # tests for timestreamise_image
    def test_timestreamise_image(self):
        try:
            e2t.timestreamise_image(self.jpg_testfile, self.camera)
            self.assertTrue(path.exists(self.r_fullres_path))
            self._md5test(self.r_fullres_path,
                          "76ee6fb2f5122d2f5815101ec66e7cb8")
        except e2t.SkipImage:
            pass

    # tests for process_image
    def test_process_image(self):
        e2t.process_image((self.jpg_testfile, self.camera, "jpg"))
        self.assertTrue(path.exists(self.r_fullres_path))
        self._md5test(self.r_fullres_path, "76ee6fb2f5122d2f5815101ec66e7cb8")

    def test_process_image_map(self):
        e2t.process_image((self.jpg_testfile, self.camera, "jpg"))
        self.assertTrue(path.exists(self.r_fullres_path))
        self._md5test(self.r_fullres_path, "76ee6fb2f5122d2f5815101ec66e7cb8")

    # tests for parse_camera_config_csv
    def test_parse_camera_config_csv(self):
        configs = [
            {
                'ARCHIVE_DEST': './test/out/archive',
                'camera_timezone': (11, 0),
                'EXPT': 'BVZ00000',
                'DESTINATION': './test/out/timestreams',
                'CAM_NUM': '01',
                'EXPT_END': time.strptime('2013_12_31', "%Y_%m_%d"),
                'EXPT_START': time.strptime('2012_12_01', "%Y_%m_%d"),
                'INTERVAL': 5,
                'IMAGE_TYPES': ["jpg"],
                'LOCATION': 'EUC-R01C01',
                'METHOD': 'move',
                'mode': 'batch',
                'resolutions': ['original'],
                'SOURCE': './test/img/camupload',
                'sunrise': (5, 0),
                'sunset': (22, 0),
                'USE': True,
                'user': 'Glasshouses',
                'TS_STRUCTURE': ('BVZ00000/{folder}/BVZ00000'
                                 '-EUC-R01C01-C{cam}~{res}-orig'),
                'PROJECT_OWNER': '',
                'FILENAME_DATE_MASK':'',
                'FN_PARSE': '',
                'FN_STRUCTURE': 'BVZ00000-EUC-R01C01-C01~{res}-orig',
                'ORIENTATION': ''

            }
        ]
        result = e2t.parse_camera_config_csv(self.test_config_csv)
        for expt, got in zip(configs, result):
            self.assertDictEqual(got, expt)

    def test_unused_bad_camera(self):
        # first entry is invalid but not used, should return None
        got = list(e2t.parse_camera_config_csv(self.unused_bad_cam_csv))
        self.assertListEqual(got, [])

    def test_parse_camera_config_csv_badconfig(self):
        self.assertFalse(list(
            e2t.parse_camera_config_csv(self.bad_header_config_csv)))

    # tests for generate_config_csv
    def test_generate_config_csv(self):
        out_csv = path.join(self.out_dirname, "test_gencnf.csv")
        try:
            e2t.gen_config(out_csv)
        except SystemExit:
            pass
        self._md5test(out_csv, "27206481b58975b0c3d3c02c6dda6813")

    # Tests for checking parsing of dates from filename
    def test_check_date_parse(self):
        got = e2t.get_time_from_filename(
            "whroo20141101_001212M.jpg", "%Y%m%d_%H%M%S")
        expected = time.strptime("20141101_001212", "%Y%m%d_%H%M%S")
        self.assertEqual(got, expected)
        got = e2t.get_time_from_filename(
            "TRN-NC-DSC-01~640_2013_06_01_10_45_00_00.jpg",
            "%Y_%m_%d_%H_%M_%S")
        expected = time.strptime("2013_06_01_10_45_00", "%Y_%m_%d_%H_%M_%S")
        self.assertEqual(got, expected)

    def test_check_write_exif(self):
        # Write To Exif
        filename = 'jpg/whroo20131104_020255M.jpg'
        date_time = e2t.get_time_from_filename(
            path.join(self.camupload_dir, filename), "%Y%m%d_%H%M%S")
        e2t.write_exif_date(path.join(self.camupload_dir, filename), date_time)
        # Read From Exif
        exif_tags = pexif.JpegFile.fromFile(
            path.join(self.camupload_dir, filename))
        str_date = exif_tags.exif.primary.ExtendedEXIF.DateTimeOriginal
        date = time.strptime(str_date, "%Y:%m:%d %H:%M:%S")
        # Check Equal
        self.assertEqual(date_time, date)

    # Tests for checking image resizing
    def test_check_resize_img(self):
        if not SKIMAGE:
            print("Skimage not available, can't test resizing", ImportWarning)
            return
        filename = 'jpg/whroo20131104_020255M.jpg'
        new_width, w = 400, 0
        try:
            dest = path.join(self.camupload_dir, filename)
            e2t.resize_img(filename, dest, new_width, 300)
            w = novice.open(
                path.join(self.camupload_dir, filename)).width
        except OSError:
            pass
        self.assertEqual(w, new_width)

    def test_main(self):
        e2t.main(self.test_config_csv, logdir=self.out_dirname)
        self.assertTrue(path.exists(self.r_fullres_path))

    def test_main_raw(self):
        e2t.main(self.test_config_raw_csv, logdir=self.out_dirname)
        self.assertTrue(path.exists(self.r_fullres_path))
        self.assertTrue(path.exists(self.r_raw_path))

    def test_main_expt_dates(self):
        e2t.main(self.test_config_dates_csv, logdir=self.out_dirname)
        self.assertFalse(path.exists(self.r_fullres_path))

    def test_main_threads(self):
        # with a good value for threads
        e2t.main(self.test_config_csv, logdir=self.out_dirname, n_threads=2)
        self.assertTrue(path.exists(self.r_fullres_path))

    def test_main_threads_bad(self):
        # and with a bad one (should default back to n_cpus)
        e2t.main(self.test_config_csv, logdir=self.out_dirname, n_threads='v')
        self.assertTrue(path.exists(self.r_fullres_path))

    def test_main_threads_one(self):
        e2t.main(self.test_config_csv, logdir=self.out_dirname, n_threads=1)
        self.assertTrue(path.exists(self.r_fullres_path))
        # IMG0001.JPG should always be the first one, with one core it's
        # deterministic
        self._md5test(self.r_fullres_path, "76ee6fb2f5122d2f5815101ec66e7cb8")


if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=3)
    unittest.main(testRunner=runner)
