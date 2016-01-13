""" Crawl through a root directory and audit all of the timestreams inside"""
from operator import itemgetter
import multiprocessing
import csv
import matplotlib.pyplot as plt
import argparse
from datetime import datetime, timedelta, date
from os import walk, path#, listdir
import re
from collections import Counter, OrderedDict
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
    timestreams = []
    for root, dirs, files in walk(input_directory):
        for directory in dirs:
            prog = re.compile("~fullres-(orig|raw)")
            if (prog.search(directory)):
                timestreams.append(root + path.sep + directory)
    return timestreams
    # prog = re.compile("~fullres-(orig|raw)")
    # if prog.search(input_directory):
    #     return [input_directory]
    # else:
    #     sub_streams = []
    #     for item in listdir(input_directory):
    #         if path.isdir(input_directory + path.sep + item):
    #             sub_streams+=(find_timestreams(input_directory + path.sep + item))
    #     return sub_streams

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
    possible_intervals = []
    for x in [60, 30, 15, 10, 5, 1]:
        possible_intervals.append(timedelta(minutes=x))
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
            int = timedelta(minutes=60)
            sort_diff = (sorted(Counter(diff).most_common(), key=itemgetter(1), reverse=True))
            for pos_int in sort_diff:
                if pos_int[0] in possible_intervals:
                    int = pos_int[0]
                    break
            differences.append(int)
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
                new_date = now + timedelta(seconds = interval)
                if now.date() < (new_date).date():
                    break
                now = new_date
                time = now.time()
        today += timedelta(days=1)
    return missing

def plot_missing_images_graph(missing_images, timestream, start_date, end_date,ipd):
    pltx = []
    plty = []
    today = start_date
    while today<= end_date:
        pltx.append(today)
        if today in missing_images.keys():
            plty.append((len(missing_images[today])/ipd)*100)
        else:
            plty.append(0)
        today += timedelta(days=1)
    N = len(pltx)
    ind = np.arange(N)  # the x locations for the groups
    width = 0.35       # the width of the bars
    fig, ax = plt.subplots()
    rects1 = ax.bar(ind, plty, width*2, bottom=None)

    # add some text for labels, title and axes ticks
    ax.set_ylabel('% Of Missing Images')
    ax.set_xticks(ind + width)
    plt.ylim([0.0, 100.0])
    plt.xticks(rotation=30)
    ax.set_xticklabels(pltx)
    rects = ax.patches

    def autolabel(rects):
        # attach some text labels
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
                    '{}'.format(round(height, 3)),
                    ha='center', va='bottom')

    autolabel(rects1)
    fig = plt.gcf()
    fig.set_size_inches((5 + 1*len(pltx)), 5)
    plt.suptitle(timestream.split(path.sep)[-1] + " As Of " + str(datetime.now().date()))
    plt.savefig(timestream + path.sep + "missing_images.jpg", bbox_inches='tight')
    plt.clf()
    return pltx, plty

def output_missing_images_csv(missing_images, timestream, interval):
    with open(timestream + path.sep + "missing_images.csv", 'w+') as csvfile:
        field_names = ["Timestream", "Interval", "Date"]
        writer = csv.DictWriter(csvfile, fieldnames = field_names)
        writer.writeheader()
        for date, images in missing_images.iteritems():
            for image in images:
                writer.writerow({"Timestream":Timestream, "Date":date,"Time":image.time()})

def images_per_day(start_time, end_time, interval):
    images = (datetime.combine(date.today(), end_time) - datetime.combine(date.today(), start_time)).total_seconds()/interval
    return images

def output_all_missing_images(ts_missing, output_directory, start_date, end_date):
    with open(output_directory + path.sep + "missing_images.csv", 'w+') as csvfile:
        field_names = ["date"]
        for timestream, other in ts_missing.iteritems():
            field_names.append(timestream.split(path.sep)[-1])
        print("Field names", field_names)
        #Dictionary[date][timestream]
        d ={}
        for timestream, (dates, per_missing) in ts_missing.iteritems():
            for (x, y) in zip(dates, per_missing):
                try:
                    d[x.strftime("%Y_%m_%d")][timestream] =  y
                except:
                    d[x.strftime("%Y_%m_%d")] = {}
                    d[x.strftime("%Y_%m_%d")][timestream] =  y
        writer = csv.writer(csvfile,  lineterminator='\n')
        output = []
        writer.writerow(field_names)
        for date, timestreams in d.iteritems():
            row = [date]
            print(timestreams)
            appended = False
            for timestream, perc in timestreams.iteritems():
                row.append(perc)
                appended = True
            if not appended:
                row.append(0)
            output.append(row)
        output = sorted(output)
        for line in output:
            writer.writerow(line)

def graph_all_missing_images(all_missing_images, output_directory, start_date, end_date):
    pltx = [] #Timestream names
    plty = [] # % of missing images
    for timestream, (dates, per_missing) in all_missing_images.iteritems():
        pltx.append(timestream.split(path.sep)[-1])
        percentage_missing = ((sum(per_missing)/len(per_missing)) if len(per_missing) else 0.0)
        plty.append(percentage_missing)

    N = len(pltx)

    ind = np.arange(N)  # the x locations for the groups
    width = 0.35       # the width of the bars
    fig, ax = plt.subplots()
    rects1 = ax.bar(ind, plty, width*2, bottom=None)

    # add some text for labels, title and axes ticks
    ax.set_ylabel('% Of Missing Images')
    ax.set_title('Percentage of Missing Images by Timestream')
    ax.set_xticks(ind + width)
    plt.ylim([0.0, 100.0])
    plt.xticks(rotation="vertical")
    ax.set_xticklabels(pltx)
    rects = ax.patches

    def autolabel(rects):
        # attach some text labels
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
                    '{}'.format(round(height, 3)),
                    ha='center', va='bottom')

    autolabel(rects1)
    fig.set_size_inches((5 + 1*len(pltx)), 5)
    plt.savefig(output_directory + path.sep + "total_missing_images.jpg", bbox_inches='tight')
    fig = plt.gcf()

def graph_all_missing_images_over_time(all_missing_images, output_directory, start_date, end_date):
    plt.close('all')
    N = len(all_missing_images)
    f, plots = plt.subplots(N, sharex=True, sharey=True)
    f.subplots_adjust(hspace=0)
    plt.setp([a.get_xticklabels() for a in f.axes[:-1]], visible=False)
    i = 0
    for timestream, (dates, per_missing) in all_missing_images.iteritems():
        plots[i].plot(dates, per_missing)
        plots[i].set_ylabel(timestream.split(path.sep)[-1], rotation="horizontal", labelpad=130)
        i+= 1
    f.set_size_inches((5 + (end_date-start_date).days/30 ),(5 + 1*len(all_missing_images)))
    plt.ylim([-10.0, 110.0])
    plt.xticks(rotation=30)
    plt.savefig(output_directory + path.sep + "total_missing_images_over_time.jpg", bbox_inches='tight')


def timestream_function(timestream):
    date_times= sorted(find_images(timestream))
    if(date_times):
        print("Beginning timestream " + timestream)
        # print("Getting relevant data")
        start_date = date_times[0].date()
        end_date = date_times[-1].date()
        start_time, end_time = get_start_end(date_times)
        interval = get_interval(date_times)
        # print("Calculating images per day")
        ipd = images_per_day(start_time, end_time, interval)
        # print("Finding Missing Images")
        missing_images=find_missing_images(date_times, start_date, end_date, start_time, end_time, interval)
        # print("Outputting Missing Images")
        dates, per_missing = plot_missing_images_graph(missing_images, timestream, start_date, end_date, ipd)
        #output_missing_images_csv(missing_images, timestream)
        return (timestream, dates, per_missing)
    else:
        print("No images in ", timestream)
        return (False, False, False)

def main(input_directory, output_directory, threads):
    print("Using {} threads".format(threads))
    if input_directory[-1] == path.sep:
        input_directory = input_directory[:-1]
    if output_directory[-1] == path.sep:
        output_directory = output_directory[:-1]
    # Find all timestreams in parent folder (Returns a bunch of folder addressess
    print("Finding timestreams in " + input_directory)
    all_timestreams = find_timestreams(input_directory)
    all_missing_images = {}
    pool = multiprocessing.Pool(threads)
    if threads > len(all_timestreams):
        threads = len(all_timestreams)
    for count, b in enumerate(pool.imap(timestream_function, all_timestreams)):
        if (b[0]):
            all_missing_images[b[0]] = (b[1], b[2])
    # for timestream in all_timestreams:
    #     date_times= sorted(find_images(timestream))
    #     if(date_times):
    #         print("Beginning timestream " + timestream)
    #         print("Getting relevant data")
    #         start_date = date_times[0].date()
    #         end_date = date_times[-1].date()
    #         start_time, end_time = get_start_end(date_times)
    #         interval = get_interval(date_times)
    #         print("Calculating images per day")
    #         ipd = images_per_day(start_time, end_time, interval)
    #         print("Finding Missing Images")
    #         missing_images=find_missing_images(date_times, start_date, end_date, start_time, end_time, interval)
    #         print("Outputting Missing Images")
    #         dates, per_missing = plot_missing_images_graph(missing_images, timestream, start_date, end_date, ipd)
    #         output_missing_images_csv(missing_images, timestream)
    #         all_missing_images[timestream] = (dates, per_missing)
    #     else:
    #         print("No images in this timestream")
    print("")
    print("Outputting Overall csv and graph")
    start_date = all_missing_images.itervalues().next()[0][0]
    end_date = all_missing_images.itervalues().next()[0][-1]
    for timestream, (dates, per_missing) in all_missing_images.iteritems():
        if dates[0] < start_date:
            start_date = dates[0]
        if dates[-1] > end_date:
            end_date = dates[-1]
    ordered_dict = OrderedDict(sorted(all_missing_images.items()))
    output_all_missing_images(ordered_dict, output_directory, start_date, end_date)
   # graph_all_missing_images(ordered_dict, output_directory, start_date, end_date)
   # graph_all_missing_images_over_time(ordered_dict, output_directory, start_date, end_date)

    pass

if __name__ == "__main__":
    opts = cli_options()
    main(opts.directory, opts.output, opts.threads)
