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
import datetime
import warnings
import json
# Module imports
from .. import exif2timestream as e2t
import pexif

PIL= True
try:
    from PIL import Image
except ImportError:
    PIL = False


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
        'ARCHIVE_DEST': os.path.sep.join([out_dirname, 'archive']),
        'EXPT': 'BVZ00000',
        'DESTINATION': os.path.sep.join([out_dirname, 'timestreams']),
        'CAM_NUM': 1,
        'EXPT_END': '2013_12_31',
        'EXPT_START': '2013_11_01',
        'LOCATION': 'EUC-R01C01',
        'METHOD': 'copy',
        'IMAGE_TYPES': 'raw~jpg',
        'INTERVAL': '5',
        'MODE': 'batch',
        'RESOLUTIONS': 'original',
        'SOURCE': os.path.sep.join([dirname, "img", "camupload"]),
        'SUNRISE': '500',
        'SUNSET': '2200',
        'CAMERA_TIMEZONE': '1100',
        'USE': '1',
        'USER': 'Glasshouses',
        'TS_STRUCTURE': os.path.join('BVZ00000',"EUC-R01C01-C01-F01", "{folder}", 'BVZ00000-EUC-R01C01-C01-F01~fullres-orig'),
        'FN_PARSE': '',
        'PROJECT_OWNER': '',
        'FILENAME_DATE_MASK': '',
        'FN_STRUCTURE': 'BVZ00000-EUC-R01C01-C01-F01~{res}-{step}',
        'ORIENTATION': '',
        'DATASETID':1,
        'TIMESHIFT':0,
        'USERFRIENDLYNAME':'',
        'JSON_UPDATES':''
    }
    camera_unix = {
        'ARCHIVE_DEST': os.path.sep.join([out_dirname, 'archive']),
        'EXPT': 'BVZ00000',
        'DESTINATION': os.path.sep.join([out_dirname, 'timestreams']),
        'CAM_NUM': 1,
        'EXPT_END': '2013_12_31',
        'EXPT_START': '2013_11_01',
        'LOCATION': 'EUC-R01C01',
        'METHOD': 'copy',
        'INTERVAL': '5',
        'IMAGE_TYPES': 'raw~jpg',
        'MODE': 'batch',
        'RESOLUTIONS': 'original',
        'SOURCE': os.path.sep.join([dirname, "img", "camupload"]),
        'SUNRISE': '500',
        'SUNSET': '2200',
        'CAMERA_TIMEZONE': '1100',
        'USE': '1',
        'USER': 'Glasshouses',
        'TS_STRUCTURE': os.path.join('BVZ00000',"EUC-R01C01-C01-F01", "{folder}", 'BVZ00000-EUC-R01C01-C01-F01~fullres-orig'),
        'FN_PARSE': '',
        'PROJECT_OWNER': '',
        'FILENAME_DATE_MASK': "",
        'FN_STRUCTURE': 'BVZ00000-EUC-R01C01-C01-F01~{res}-{step}',
        'ORIENTATION': '',
        'DATASETID':1,
        'TIMESHIFT':0,
        'USERFRIENDLYNAME':'',
        'JSON_UPDATES':''

    }

    r_fullres_path = os.path.join(
        out_dirname, "timestreams", "BVZ00000", "EUC-R01C01-C01-F01",
        'original', 'BVZ00000-EUC-R01C01-C01-F01~fullres-orig', '2013', '2013_11',
        '2013_11_12', '2013_11_12_20',
        'BVZ00000-EUC-R01C01-C01-F01~fullres-orig_2013_11_12_20_55_00_00.JPG'
    )
    r_resize_path = os.path.join(
        out_dirname, "timestreams", "BVZ00000", "EUC-R01C01-C01-F01",
        'output', 'BVZ00000-EUC-R01C01-C01-F01~1920-orig', '2013', '2013_11',
        '2013_11_12', '2013_11_12_20',
        'BVZ00000-EUC-R01C01-C01-F01~1920-orig_2013_11_12_20_55_00_00.jpg'
    )
    r_datetime_path = os.path.join(
        out_dirname, "timestreams", "BVZ00000", "EUC-R01C01-C01-F01",
        "original",'BVZ00000-EUC-R01C01-C01-F01~fullres-orig', '2013', '2013_11',
        '2013_11_04', '2013_11_04_02',
        'BVZ00000-EUC-R01C01-C01-F01~fullres-orig_2013_11_04_22_05_00_00.JPG'
    )

    r_raw_path = os.path.join(
        out_dirname, "timestreams", "BVZ00000", "EUC-R01C01-C01-F01",
        "original",'BVZ00000-EUC-R01C01-C01-F01~fullres-raw', '2013', '2013_11',
        '2013_11_12', '2013_11_12_20',
        'BVZ00000-EUC-R01C01-C01-F01~fullres-raw_2013_11_12_20_55_00_00'
        '.CR2'
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


    def wipe_output(self):
        cam = self.camera_unix if path.sep == "/" else self.camera_win32
        self.camera_raw = copy.deepcopy(cam)
        self.camera = copy.deepcopy(cam)
        mapping = e2t.CameraFields.TS_CSV
        output_dir = path.dirname(self.camera[mapping['destination']] + os.path.sep + 'timestreams')
        shutil.rmtree(output_dir)
        # archive_path = path.dirname(self.camera[mapping['destination']] + os.path.sep + 'archive')
        # shutil.rmtree(archive_path)

    def test_main_expt_dates(self):
        if path.exists(self.r_fullres_path):
            os.remove(self.r_fullres_path)
        e2t.main(self.test_config_dates_csv, logdir=self.out_dirname)
        self.assertFalse(path.exists(self.r_fullres_path))

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
        exp = 'BVZ00000-EUC-R01C01-C01-F01~fullres-orig'
        self.assertEqual(name, exp)

    def test_make_timestream_name_params(self):
        name = e2t.make_timestream_name(
            self.camera,
            res="1080",
            step="clean")
        exp = 'BVZ00000-EUC-R01C01-C01-F01~1080-clean'
        self.assertEqual(name, exp)

    # tests for find_image_files
    def test_find_image_files(self):
        expt = {"jpg": {path.join(self.camupload_dir, x) for x in [
                        os.path.join('jpg', 'IMG_0001.JPG'),
                        os.path.join('jpg', 'IMG_0002.JPG'),
                        os.path.join('jpg', 'IMG_0630.JPG'),
                        os.path.join('jpg', 'IMG_0633.JPG'),
                        os.path.join('jpg', 'whroo20131104_020255M.jpg')]
                        },
                "raw": {path.join(self.camupload_dir, os.path.join('raw', 'IMG_0001.CR2'))},
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
        self._md5test(self.r_fullres_path, "b0895204732d2806780e87ea6ce8e874")

    def test_process_image_map(self):
        e2t.process_image((self.jpg_testfile, self.camera, "jpg"))
        self.assertTrue(path.exists(self.r_fullres_path))
        self._md5test(self.r_fullres_path, "b0895204732d2806780e87ea6ce8e874")

    # tests for parse_camera_config_csv
    def test_parse_camera_config_csv(self):
        configs = [
            {
                'archive_dest': os.path.sep.join(['.', 'test', 'out', 'archive']),
                'timezone': (11, 0),
                'expt': 'BVZ00000',
                'destination': os.path.sep.join(['.', 'test', 'out', 'timestreams']),
                'cam_num': '01',
                'expt_end': time.strptime('2013_12_31', "%Y_%m_%d"),
                'expt_start': time.strptime('2012_12_01', "%Y_%m_%d"),
                'interval': 5,
                'image_types': ["jpg"],
                'location': 'EUC-R01C01',
                'method': 'move',
                'mode': 'batch',
                'resolutions': ['original'],
                'source': os.path.sep.join(['.', 'test', 'img', 'camupload']),
                'sunrise': (5, 0),
                'sunset': (22, 0),
                'use': True,
                'user': 'Glasshouses',
                'ts_structure': os.path.sep.join(['BVZ00000', 'EUC-R01C01-C01-F01', '{folder}', 'BVZ00000-EUC-R01C01-C01-F01~{res}-{step}']),
                'project_owner': '',
                'filename_date_mask':'',
                'fn_parse': '',
                'fn_structure': 'BVZ00000-EUC-R01C01-C01-F01~{res}-{step}',
                'orientation': '',
                'timeshift':'',
                'datasetID':'-F01',
                'json_updates':'',
                'large_json':0,
                'userfriendlyname':'BVZ00000-EUC-R01C01-C01-F01'
            }
        ]
        result = e2t.parse_camera_config_csv(self.test_config_csv)
        for expt, got in zip(configs, result):
            self.assertDictEqual(got.__dict__, expt)

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
        self._md5test(out_csv, "4be1f4a6c460f789851f60a79c2db91c")

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
        filename = 'jpg' + os.path.sep + 'whroo20131104_020255M.jpg'
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
        if not PIL:
            ("PIL not available, can't test resizing", ImportWarning)
            return
        filename = 'jpg' + os.path.sep + 'whroo20131104_020255M.jpg'
        new_width, w = 400, 0
        try:
            dest = path.join(self.camupload_dir, filename)
            img_array = Image.open(dest)
            e2t.resize_img(path.join(self.camupload_dir, filename), dest, new_width, 300, img_array)
            w = Image.open(
                path.join(self.camupload_dir, filename)).width
        except OSError:
            pass
        self.assertEqual(w, new_width)



    def test_main(self):
        print(self.r_fullres_path)
        e2t.main(self.test_config_csv, logdir=self.out_dirname)
        self.assertTrue(path.exists(self.r_fullres_path))



    def test_main_raw(self):
        e2t.main(self.test_config_raw_csv, logdir=self.out_dirname)
        self.assertTrue(path.exists(self.r_fullres_path))
        self.assertTrue(path.exists(self.r_raw_path))



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



    def test_orientation(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            image_date = e2t.get_file_date(self.r_fullres_path, 0, 60)
            orig = Image.open(self.r_fullres_path).size
            e2t.rotate_image(90, self.r_fullres_path)
            after = Image.open(self.r_fullres_path).size
            self.assertGreater(2, abs(orig[0]- after[1]))
            self.assertGreater(2, abs(orig[1]- after[0]))
            e2t.rotate_image(270, self.r_fullres_path)
            self.assertEqual(True, e2t.write_exif_date(self.r_fullres_path, image_date))
            new_image_date = e2t.get_file_date(self.r_fullres_path, 0, 60)
            self.assertEqual(image_date, new_image_date)

    def test_resize_main(self):
        rotate = e2t.CameraFields({
                'ARCHIVE_DEST': os.path.sep.join(['.', 'test', 'out', 'archive']),
                'CAMERA_TIMEZONE': "11",
                'EXPT': 'BVZ00000',
                'DESTINATION': os.path.sep.join(['.', 'test', 'out', 'timestreams']),
                'CAM_NUM': '01',
                'EXPT_END': "now",
                'EXPT_START': "2012_01_01",
                'INTERVAL': 5,
                'IMAGE_TYPES': "jpg",
                'LOCATION': 'EUC-R01C01',
                'METHOD': 'move',
                'MODE': 'batch',
                'RESOLUTIONS': 'original~1920',
                'SOURCE': os.path.sep.join(['.', 'test', 'img', 'camupload']),
                'SUNRISE': "0500",
                'SUNSET': "2200",
                'USE': True,
                'USER': 'Glasshouses',
                'TS_STRUCTURE': os.path.sep.join(['BVZ00000', 'EUC-R01C01-C01-F01', '{folder}', 'BVZ00000-EUC-R01C01-C01-F01~{res}-{step}']),
                'PROJECT_OWNER': '',
                'FILENAME_DATE_MASK':'',
                'FN_PARSE': '',
                'FN_STRUCTURE': 'BVZ00000-EUC-R01C01-C01-F01~{res}-{step}',
                'ORIENTATION': '',
                'TIMESHIFT':'',
                'DATASETID':'-F01',
                'JSON_UPDATES':'',
                'LARGE_JSON':0,
                'USERFRIENDLYNAME':'BVZ00000-EUC-R01C01-C01-F01'
            })
        orig = Image.open(self.r_fullres_path).size
        print(orig)
        self.wipe_output()
        for ext,images in e2t.find_image_files(rotate).items():
            images = sorted(images)
            e2t.process_camera(rotate, ext, images, n_threads=1)
        new = Image.open(self.r_resize_path).size
        print(new)
        self.assertEqual(new[0], 1920)
        self.assertEqual(new[1], 1280)


    def test_rotate_main(self):
        rotate = e2t.CameraFields({
                'ARCHIVE_DEST': os.path.sep.join(['.', 'test', 'out', 'archive']),
                'CAMERA_TIMEZONE': "11",
                'EXPT': 'BVZ00000',
                'DESTINATION': os.path.sep.join(['.', 'test', 'out', 'timestreams']),
                'CAM_NUM': '01',
                'EXPT_END': "now",
                'EXPT_START': "2012_01_01",
                'INTERVAL': 5,
                'IMAGE_TYPES': "jpg",
                'LOCATION': 'EUC-R01C01',
                'METHOD': 'move',
                'MODE': 'batch',
                'RESOLUTIONS': 'original',
                'SOURCE': os.path.sep.join(['.', 'test', 'img', 'camupload']),
                'SUNRISE': "0500",
                'SUNSET': "2200",
                'USE': True,
                'USER': 'Glasshouses',
                'TS_STRUCTURE': os.path.sep.join(['BVZ00000', 'EUC-R01C01-C01-F01', '{folder}', 'BVZ00000-EUC-R01C01-C01-F01~{res}-{step}']),
                'PROJECT_OWNER': '',
                'FILENAME_DATE_MASK':'',
                'FN_PARSE': '',
                'FN_STRUCTURE': 'BVZ00000-EUC-R01C01-C01-F01~{res}-{step}',
                'ORIENTATION': '90',
                'TIMESHIFT':'',
                'DATASETID':'-F01',
                'JSON_UPDATES':'',
                'LARGE_JSON':0,
                'USERFRIENDLYNAME':'BVZ00000-EUC-R01C01-C01-F01'
            })
        orig = Image.open(self.r_fullres_path).size
        self.wipe_output()
        for ext,images in e2t.find_image_files(rotate).items():
            images = sorted(images)
            e2t.process_camera(rotate, ext, images, n_threads=1)
        new = Image.open(self.r_fullres_path).size
        self.assertNotEquals(orig, new)
        self.assertLess(abs(orig[1]- new[0]), 2)
        self.assertLess(abs(orig[0]- new[1]), 2)





    def test_rotate_resize_main(self):
        rotate_resize = e2t.CameraFields({
                'ARCHIVE_DEST': os.path.sep.join(['.', 'test', 'out', 'archive']),
                'CAMERA_TIMEZONE': "11",
                'EXPT': 'BVZ00000',
                'DESTINATION': os.path.sep.join(['.', 'test', 'out', 'timestreams']),
                'CAM_NUM': '01',
                'EXPT_END': "now",
                'EXPT_START': "2012_01_01",
                'INTERVAL': 5,
                'IMAGE_TYPES': "jpg",
                'LOCATION': 'EUC-R01C01',
                'METHOD': 'move',
                'MODE': 'batch',
                'RESOLUTIONS': 'original~1920',
                'SOURCE': os.path.sep.join(['.', 'test', 'img', 'camupload']),
                'SUNRISE': "0500",
                'SUNSET': "2200",
                'USE': True,
                'USER': 'Glasshouses',
                'TS_STRUCTURE': os.path.sep.join(['BVZ00000', 'EUC-R01C01-C01-F01', '{folder}', 'BVZ00000-EUC-R01C01-C01-F01~{res}-{step}']),
                'PROJECT_OWNER': '',
                'FILENAME_DATE_MASK':'',
                'FN_PARSE': '',
                'FN_STRUCTURE': 'BVZ00000-EUC-R01C01-C01-F01~{res}-{step}',
                'ORIENTATION': '90',
                'TIMESHIFT':'',
                'DATASETID':'-F01',
                'JSON_UPDATES':'',
                'LARGE_JSON':0,
                'USERFRIENDLYNAME':'BVZ00000-EUC-R01C01-C01-F01'
            })
        self.wipe_output()
        for ext,images in e2t.find_image_files(rotate_resize).items():
            images = sorted(images)
            e2t.process_camera(rotate_resize, ext, images, n_threads=1)
        new = Image.open(self.r_resize_path).size
        print(new)
        self.assertEqual(new[1], 1920)
        self.assertEqual(new[0], 1280)

    def test_start_end(self):
        start_end = e2t.CameraFields({
                'ARCHIVE_DEST': os.path.sep.join(['.', 'test', 'out', 'archive']),
                'CAMERA_TIMEZONE': "11",
                'EXPT': 'BVZ00000',
                'DESTINATION': os.path.sep.join(['.', 'test', 'out', 'timestreams']),
                'CAM_NUM': '01',
                'EXPT_END': "now",
                'EXPT_START': "2002_01_01",
                'INTERVAL': 5,
                'IMAGE_TYPES': "jpg~raw",
                'LOCATION': 'EUC-R01C01',
                'METHOD': 'move',
                'MODE': 'batch',
                'RESOLUTIONS': 'original~1920',
                'SOURCE': os.path.sep.join(['.', 'test', 'img', 'camupload']),
                'SUNRISE': "0500",
                'SUNSET': "2200",
                'USE': True,
                'USER': 'Glasshouses',
                'TS_STRUCTURE': os.path.sep.join(['BVZ00000', 'EUC-R01C01-C01-F01', '{folder}', 'BVZ00000-EUC-R01C01-C01-F01~{res}-{step}']),
                'PROJECT_OWNER': '',
                'FILENAME_DATE_MASK':'',
                'FN_PARSE': '',
                'FN_STRUCTURE': 'BVZ00000-EUC-R01C01-C01-F01~{res}-{step}',
                'ORIENTATION': '90',
                'TIMESHIFT':'',
                'DATASETID':'-F01',
                'JSON_UPDATES':'',
                'LARGE_JSON':0,
                'USERFRIENDLYNAME':'BVZ00000-EUC-R01C01-C01-F01'
            })
        # Check Jpeg
        images_both = e2t.find_image_files(start_end)
        images = images_both['jpg']
        images = sorted(images)
        start, end = e2t.get_actual_start_end(start_end, images, 'jpg')
        start_actual_jpg = time.strptime("20131112_205500", "%Y%m%d_%H%M%S")
        end_actual_jpg = time.strptime("20131104_020500", "%Y%m%d_%H%M%S")
        self.assertEqual(start_actual_jpg, start)
        self.assertEqual(end_actual_jpg, end)

        # Check Raw
        images = images_both['raw']
        print(images)
        images = sorted(images)
        start, end = e2t.get_actual_start_end(start_end, images, 'raw')
        start_actual_raw = time.strptime("20131112_205500", "%Y%m%d_%H%M%S")
        end_actual_raw = time.strptime("20131112_205500", "%Y%m%d_%H%M%S")
        self.assertEqual(start_actual_raw, start)
        self.assertEqual(end_actual_raw, end)

        # Check Null
        images = []
        start, end = e2t.get_actual_start_end(start_end, images, 'jpg')
        self.assertEqual(start, start_end.expt_start)
        self.assertEqual(end, start_end.expt_end)


    def test_timeshift(self):
        before = e2t.get_file_date(self.r_fullres_path, "", 60)
        after = e2t.get_file_date(self.r_fullres_path, "10", 60)
        self.assertNotEqual(before, after)
        shift_to_equal = datetime.timedelta(hours=10)
        before = (datetime.datetime.fromtimestamp(time.mktime(before))+shift_to_equal).timetuple()
        self.assertEqual(before, after)

    def test_filename_parse(self):
        expt = {"jpg": {path.join(self.camupload_dir, x) for x in [
                        os.path.join('jpg', 'IMG_0001.JPG'),
                        os.path.join('jpg', 'IMG_0002.JPG'),
                        os.path.join('jpg', 'IMG_0630.JPG'),
                        os.path.join('jpg', 'IMG_0633.JPG'),]
                        },
                }
        self.camera.fn_parse = "IMG_"
        got = e2t.find_image_files(self.camera)
        self.assertSetEqual(set(got["jpg"]), expt["jpg"])
        self.assertSetEqual(set(got["jpg"]), expt["jpg"])


    def test_json_mode(self):
        no_large_json = e2t.CameraFields({
                'ARCHIVE_DEST': os.path.sep.join(['.', 'test', 'out', 'archive']),
                'CAMERA_TIMEZONE': "11",
                'EXPT': 'BVZ00000',
                'DESTINATION': os.path.sep.join(['.', 'test', 'out', 'timestreams']),
                'CAM_NUM': '01',
                'EXPT_END': "now",
                'EXPT_START': "2002_01_01",
                'INTERVAL': 5,
                'IMAGE_TYPES': "jpg~raw",
                'LOCATION': 'EUC-R01C01',
                'METHOD': 'move',
                'MODE': 'batch',
                'RESOLUTIONS': 'original~1920',
                'SOURCE': os.path.sep.join(['.', 'test', 'img', 'camupload']),
                'SUNRISE': "0500",
                'SUNSET': "2200",
                'USE': True,
                'USER': 'Glasshouses',
                'TS_STRUCTURE': os.path.sep.join(['BVZ00000', 'EUC-R01C01-C01-F01', '{folder}', 'BVZ00000-EUC-R01C01-C01-F01~{res}-{step}']),
                'PROJECT_OWNER': '',
                'FILENAME_DATE_MASK':'',
                'FN_PARSE': '',
                'FN_STRUCTURE': 'BVZ00000-EUC-R01C01-C01-F01~{res}-{step}',
                'ORIENTATION': '',
                'TIMESHIFT':'',
                'DATASETID':'1',
                'JSON_UPDATES':'',
                'LARGE_JSON':0,
                'USERFRIENDLYNAME':'BVZ00000-EUC-R01C01-C01-F01'
            })
        self.wipe_output()
        for ext,images in e2t.find_image_files(no_large_json).items():
            images = sorted(images)
            output = e2t.process_camera(no_large_json, ext, images, n_threads=1)
            self.assertEqual(False,output)
        #     Check the Mini-Jsons
        #     First the original value
        file_path = os.path.join(no_large_json.destination, no_large_json.ts_structure.format(
            folder='original', res='fullres', step='orig'), no_large_json.userfriendlyname
                                                     + '-ts-info.json')
        original_json = eval(open(file_path).read())
        test_json ={
            "width":5184,
            "period_in_minutes":5,
            "expt":"BVZ00000",
            "ts_end":"now",
            "height":3456,
            "posix_start":1384289700.0,
            "image_type":"JPG",
            "height_hires":3456,
            "timezone":0,
            "width_hires":5184,
            "webroot_hires":"http://phenocam.anu.edu.au/cloud/a_data./test/out/timestreams/BVZ00000/EUC-R01C01-C01-F01/original/BVZ00000-EUC-R01C01-C01-F01~fullres-orig",
            "ts_name":"BVZ00000/EUC-R01C01-C01-F01/original/BVZ00000-EUC-R01C01-C01-F01~fullres-orig",
            "ts_start":'2013_11_12_20_55_00',
            "owner":"",
            "webroot":"http://phenocam.anu.edu.au/cloud/a_data./test/out/timestreams/BVZ00000/EUC-R01C01-C01-F01/original/BVZ00000-EUC-R01C01-C01-F01~fullres-orig",
            "name":"BVZ00000-EUC-R01C01-C01-F01",
            "ts_id":"BVZ00000-EUC-R01C01-C01-F01",
            "posix_end":1383530700.0,
        }
        self.assertDictEqual(original_json, test_json)
        # Then the Raw
        file_path_raw = os.path.join(no_large_json.destination, no_large_json.ts_structure.format(
            folder='original', res='fullres', step='raw'), no_large_json.userfriendlyname
                                                     + '-ts-info.json')
        raw_original_json = eval(open(file_path_raw).read())
        raw_test_json ={
            "width":3456,
            "period_in_minutes":5,
            "expt":"BVZ00000",
            "ts_end":"now",
            "height":5184,
            "posix_start":1384289700.0,
            "image_type":"RAW",
            "height_hires":5184,
            "timezone":0,
            "width_hires":3456,
            "webroot_hires":"http://phenocam.anu.edu.au/cloud/a_data./test/out/timestreams/BVZ00000/EUC-R01C01-C01-F01/original/BVZ00000-EUC-R01C01-C01-F01~fullres-orig",
            "ts_name":"BVZ00000/EUC-R01C01-C01-F01/original/BVZ00000-EUC-R01C01-C01-F01~fullres-raw",
            "ts_start":'2013_11_12_20_55_00',
            "owner":"",
            "webroot":"http://phenocam.anu.edu.au/cloud/a_data./test/out/timestreams/BVZ00000/EUC-R01C01-C01-F01/original/BVZ00000-EUC-R01C01-C01-F01~fullres-raw",
            "name":"BVZ00000-EUC-R01C01-C01-F01",
            "ts_id":"BVZ00000-EUC-R01C01-C01-F01",
            "posix_end":1384289700.0,
        }
        self.assertDictEqual(raw_original_json, raw_test_json)

        # Finally the resized json
        file_path_resized = os.path.join(no_large_json.destination, no_large_json.ts_structure.format(
            folder='output', res='1920', step='orig'), no_large_json.userfriendlyname
                                                     + '-ts-info.json')
        resized_json= eval(open(file_path_resized).read())

        resized_test_json  ={
            "width":1920,
            "period_in_minutes":5,
            "expt":"BVZ00000",
            "ts_end":"now",
            "height":1280,
            "posix_start":1384289700.0,
            "image_type":"JPG",
            "height_hires":3456,
            "timezone":0,
            "width_hires":5184,
            "webroot_hires":"http://phenocam.anu.edu.au/cloud/a_data./test/out/timestreams/BVZ00000/EUC-R01C01-C01-F01/original/BVZ00000-EUC-R01C01-C01-F01~fullres-orig",
            "ts_name":"BVZ00000/EUC-R01C01-C01-F01/output/BVZ00000-EUC-R01C01-C01-F01~1920-orig",
            "ts_start":'2013_11_12_20_55_00',
            "owner":"",
            "webroot":"http://phenocam.anu.edu.au/cloud/a_data./test/out/timestreams/BVZ00000/EUC-R01C01-C01-F01/output/BVZ00000-EUC-R01C01-C01-F01~1920-orig",
            "name":"BVZ00000-EUC-R01C01-C01-F01",
            "ts_id":"BVZ00000-EUC-R01C01-C01-F01",
            "posix_end":1383530700.0,
        }
        self.assertDictEqual(resized_json, resized_test_json)

        # Delete all the JSONS
        for file in [file_path, file_path_raw, file_path_resized]:
            os.remove(file)

        no_large_json.method = 'json'
        no_large_json.source = no_large_json.destination
        no_large_json.large_json = 1

        both_image_types =e2t.find_image_files(no_large_json)
        images = both_image_types["raw"]
        images = sorted(images)
        output = e2t.process_camera(no_large_json, "raw", images, n_threads=1)
        raw_large = False
        self.assertEqual(raw_large,output)

        images = both_image_types["jpg"]
        images = sorted(images)
        output = e2t.process_camera(no_large_json, "jpg", images, n_threads=1)
        jpg_large = {
            'utc': 'false',
            'height': '1280',
            'ts_version': '1',
            'name': 'BVZ00000-EUC-R01C01-C01-F01',
            'ts_end': 'now',
            'image_type': 'JPG',
            'height_hires': '720',
            'posix_end': '1385209500.0',
            'expt': 'BVZ00000',
            'access': '0',
            'webroot': 'http://phenocam.anu.edu.au/cloud/a_data./test/out/timestreams/BVZ00000/EUC-R01C01-C01-F01/output/BVZ00000-EUC-R01C01-C01-F01~1920-orig',
            'webroot_hires': 'http://phenocam.anu.edu.au/cloud/a_data./test/out/timestreams/BVZ00000/EUC-R01C01-C01-F01/original/BVZ00000-EUC-R01C01-C01-F01~fullres-orig',
            'thumbnails': '[]',
            'period_in_minutes': '5',
            'timezone': '0',
            'ts_start': '2013_11_04_02_05_00',
            'width_hires': '1280',
            'posix_start': '1383530700.0',
            'width': '1920',
            'ts_id': 'BVZ00000-EUC-R01C01-C01-F01'
        }

        self.assertEqual(jpg_large,output)



    def test_structure_format_none(self):
        ts_format_test = e2t.CameraFields({
            'ARCHIVE_DEST': '/',
            'EXPT': 'BVZ00000',
            'DESTINATION': '/',
            'CAM_NUM': 1,
            'EXPT_END': '2013_12_31',
            'EXPT_START': '2013_11_01',
            'LOCATION': 'EUC-R01C01',
            'METHOD': 'copy',
            'INTERVAL': '5',
            'IMAGE_TYPES': 'raw~jpg',
            'MODE': 'batch',
            'RESOLUTIONS': 'original',
            'SOURCE': '/',
            'SUNRISE': '500',
            'SUNSET': '2200',
            'CAMERA_TIMEZONE': '1100',
            'USE': '1',
            'USER': 'Glasshouses',
            'TS_STRUCTURE': '',
            'FN_PARSE': '',
            'PROJECT_OWNER': '',
            'FILENAME_DATE_MASK': "",
            'FN_STRUCTURE': '',
            'ORIENTATION': '',
            'DATASETID':1,
            'TIMESHIFT':0,
            'USERFRIENDLYNAME':'',
            'JSON_UPDATES':''

        })
        output= (e2t.parse_structures(ts_format_test))
        self.assertEqual(os.path.join("BVZ00000", "EUC-R01C01-C01-F01", "{folder}", "BVZ00000-EUC-R01C01-C01-F01~{res}-{step}"), output.ts_structure)
        self.assertEqual("BVZ00000-EUC-R01C01-C01-F01~{res}-{step}", output.fn_structure)
        self.assertEqual('BVZ00000-EUC-R01C01-C01-F01', output.userfriendlyname)

    def test_structure_format_all(self):
        ts_format_test = e2t.CameraFields(
         {
            'ARCHIVE_DEST': '/',
            'EXPT': 'BVZ00000',
            'DESTINATION': '/',
            'CAM_NUM': 1,
            'EXPT_END': '2013_12_31',
            'EXPT_START': '2013_11_01',
            'LOCATION': 'EUC-R01C01',
            'METHOD': 'copy',
            'INTERVAL': '5',
            'IMAGE_TYPES': 'raw~jpg',
            'MODE': 'batch',
            'RESOLUTIONS': 'original',
            'SOURCE': '/',
            'SUNRISE': '500',
            'SUNSET': '2200',
            'CAMERA_TIMEZONE': '1100',
            'USE': '1',
            'USER': 'Glasshouses',
            'TS_STRUCTURE': os.path.join('EXPT','LOCATION-location','potato'),
            'FN_PARSE': '',
            'PROJECT_OWNER': '',
            'FILENAME_DATE_MASK': "",
            'FN_STRUCTURE': os.path.join('EXPT', 'LOCATION-location' ,'potato'),
            'ORIENTATION': '',
            'DATASETID':1,
            'TIMESHIFT':0,
            'USERFRIENDLYNAME': os.path.join('EXPT','LOCATION-location','potato'),
            'JSON_UPDATES':''
        }
        )
        output= (e2t.parse_structures(ts_format_test))
        self.assertEqual(os.path.join("BVZ00000", "EUC-R01C01-location", "potato~{res}-{step}"), output.ts_structure)
        self.assertEqual(''.join(["BVZ00000", "EUC-R01C01-location", "potato~{res}-{step}"]), output.fn_structure)
        self.assertEqual(''.join(["BVZ00000", "EUC-R01C01-location", "potato"]), output.userfriendlyname)




if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=3)
    unittest.main(testRunner=runner)
