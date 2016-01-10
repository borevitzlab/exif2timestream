""" Crawl through a root directory and audit all of the timestreams inside"""
import csv
import matplotlib.pyplot as plt
import argparse
from datetime import datetime, timedelta, date
from os import walk, path
import re
from collections import Counter
import numpy as np
from exif2timestream import get_time_from_filename
def cli_options():
    """Return CLI arguments with argparse."""
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--directory',
                        help='Root Directory')
    parser.add_argument('-o', '--output',
                        help='Output Directory')
    parser.add_argument('-t', '--threads', type=int, default=1,
                        help='Number of Threads')
    return parser.parse_args()



def find_timestreams(input_directory):
    """ Given an input of a directory, output a bunch of folder directories of timestreams"""
    timestreams = []
    for root, dirs, files in walk(input_directory):
        for directory in dirs:
            prog = re.compile("~\w*-\w*")
            if (prog.search(directory)):
                timestreams.append(root + path.sep + directory)

    return timestreams

def find_images(timestream_directory):
    """ Given a timestream directory, return a bunch of datetime objects which store imagea dates"""
    images = []
    for root, dirs, files in walk(timestream_directory):
        regex = re.compile("(\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2})")
        for file in files:
            matches = regex.findall(file)
            if(matches):
                images.append(datetime.strptime((matches)[0], "%Y_%m_%d_%H_%M_%S"))
    return images

def get_interval(date_times):
    """ Given a list of sorted datetimes, calculate the interval between images """
    dates = {}
    for d in date_times:
        date = d.date()
        try:
            dates[str(d.year) + "-" + str(d.month) + "-" + str(d.day)].append(d)
        except:
            dates[str(d.year) + "-" + str(d.month) + "-" + str(d.day)] = [(d)]
    differences = []
    for date, times in dates.iteritems():
        if len(times)>1:
            diff = ([j-i for i, j in zip(times[:-1], times[1:])])
            mc = Counter(diff).most_common(1)[0][0]
            if (mc):
                differences.append(Counter(diff).most_common(1)[0][0])
            else:
                differences.append(Counter(diff).most_common(2)[-1][0])
    interval = (sum(differences, timedelta(0)) / len(differences))
    if ((interval.seconds / 60) > 30):
        interval = (((interval.seconds/60)+1)*60)
    else:
        interval = ((interval.seconds/60)*60)
    return interval



def get_start_end(date_times):
    """ Given a sorted list of datetimes, calculate the start and end times of images"""
    times = sorted(date.time() for date in date_times)
    min, max = times[0], times[-1]
    return min, max

def find_missing_images(date_times, start_date, end_date, start_time, end_time, interval):
    today = start_date
    first = True
    missing = {}
    while(today <= end_date):
        if len(date_times):
            time = start_time
            while (time <= end_time):
                if len(date_times):
                    now = datetime(today.year, today.month, today.day, time.hour, time.minute, time.second)
                    if now in date_times:
                        date_times.remove(now)
                        first = False
                    elif not first:
                        try:
                            missing[today].append(now)
                        except:
                            missing[today] = [now]
                now += timedelta(seconds = interval)
                time = now.time()
        today += timedelta(days=1)
    return missing

def plot_missing_images_graph(missing_images, timestream, start_date, end_date,ipd):
    pltx = []
    plty = []
    for date, images in missing_images.iteritems():
        number_missing = len(images)/ipd
        pltx.append(date)
        plty.append(number_missing)
    plt.plot(pltx, plty, 'ro')
    plt.xlim([start_date, end_date])
    plt.xticks(rotation='vertical')
    if not len(missing_images):
        plt.ylim([0, 1])
    plt.suptitle(timestream.split(path.sep)[-1] + " As Of " + str(datetime.now().date()))
    plt.savefig(timestream + path.sep + "missing_images.jpg", bbox_inches='tight')
    return pltx, plty

def output_missing_images_csv(missing_images, timestream):
    with open(timestream + path.sep + "missing_images.csv", 'w+') as csvfile:
        field_names = ["Date", "Time"]
        writer = csv.DictWriter(csvfile, fieldnames = field_names)
        writer.writeheader()
        for date, images in missing_images.iteritems():
            for image in images:
                writer.writerow({"Date":date,"Time":image.time()})

def images_per_day(start_time, end_time, interval):
    images = (datetime.combine(date.today(), end_time) - datetime.combine(date.today(), start_time)).total_seconds()/interval
    return images

def output_all_missing_images(ts_missing, output_directory):
    with open(output_directory + path.sep + "missing_images.csv", 'w+') as csvfile:
        field_names = ["Timestream", "Percentage_missing"]
        writer = csv.DictWriter(csvfile, fieldnames = field_names)
        writer.writeheader()
        for timestream,(dates, per_missing) in ts_missing.iteritems():
            percentage_missing = (sum(per_missing)/len(per_missing) if len(per_missing) else 0.0)
            writer.writerow({"Timestream":timestream, "Percentage_missing" : percentage_missing })

def graph_all_missing_images(all_missing_images, output_directory):
    pltx = [] #Timestream names
    plty = [] # % of missing images
    for timestream, (dates, per_missing) in all_missing_images.iteritems():
        pltx.append(timestream.split(path.sep)[-1])
        percentage_missing = (sum(per_missing)/len(per_missing) if len(per_missing) else 0.0)
        plty.append(percentage_missing)

    N = len(pltx)

    ind = np.arange(N)  # the x locations for the groups
    width = 0.35       # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(ind, plty, width*2, color='r')

    # add some text for labels, title and axes ticks
    ax.set_ylabel('% Of Missing Images')
    ax.set_title('Percentage of Missing Images by Timestream')
    ax.set_xticks(ind + width)
    plt.xticks(rotation="vertical")
    ax.set_xticklabels(pltx)


    def autolabel(rects):
        # attach some text labels
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
                    '%d' % int(height),
                    ha='center', va='bottom')

    autolabel(rects1)

    plt.savefig(output_directory + path.sep + "total_missing_images.jpg", bbox_inches='tight')

def main(input_directory, output_directory):
    # Find all timestreams in parent folder (Returns a bunch of folder addressess
    print("Finding timestreams in " + input_directory)
    all_timestreams = find_timestreams(input_directory)
    all_missing_images = {}
    for timestream in all_timestreams:
        date_times= sorted(find_images(timestream))
        if(date_times):
            print("Beginning timestream " + timestream)
            print("Getting relevant data")
            start_date = date_times[0].date()
            end_date = date_times[-1].date()
            start_time, end_time = get_start_end(date_times)
            interval = get_interval(date_times)
            print("Calculating images per day")
            ipd = images_per_day(start_time, end_time, interval)
            print("Finding Missing Images")
            missing_images=find_missing_images(date_times, start_date, end_date, start_time, end_time, interval)
            print("Outputting Missing Images")
            dates, per_missing = plot_missing_images_graph(missing_images, timestream, start_date, end_date, ipd)
            output_missing_images_csv(missing_images, timestream)
            all_missing_images[timestream] = (dates, per_missing)
        else:
            print("No images in this timestream")
    print("Outputting csv and graph")
    output_all_missing_images(all_missing_images, output_directory)
    graph_all_missing_images(all_missing_images, output_directory)

    pass

if __name__ == "__main__":
    opts = cli_options()
    main(opts.directory, opts.output)
