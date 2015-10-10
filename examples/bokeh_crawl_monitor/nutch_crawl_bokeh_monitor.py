from __future__ import division
from __future__ import print_function

import os
import signal
import time
from datetime import datetime
from Queue import Empty
import json

import numpy as np

from bokeh.models import Range1d
from bokeh.plotting import cursession, figure, output_server, show

from kombu import Connection

import nutch

def check_supervisord():
    """
    This example uses supervisord to coordinate the various services.  See the README for details.
    """
    location = __file__
    supervisor_pidfile = os.path.join(os.path.dirname(location), 'supervisord.pid')
    if not os.path.exists(supervisor_pidfile):
        raise Exception("No supervisor.pid file found, is supervisord running?")

def term_supervisord():
    """
    Try to send a terminate signal to the supervisor daemon.
    """

    location = __file__
    supervisor_pidfile = os.path.join(os.path.dirname(location), 'supervisord.pid')
    if not os.path.exists(supervisor_pidfile):
        return
    with open(supervisor_pidfile) as f:
        pid = int(f.readline())
        os.kill(pid, signal.SIGTERM)
        print("Sent termination signal to supervisor daemon at {}\n".format(pid))

def launch_crawl(seed_url, rounds):
    """
    Use the Nutch RESTful interface to launch a crawl
    :param seed_url: The URL to start from
    :param rounds: The number of rounds to crawl
    :return: The nutch.CrawlClient corresponding to this crawl
    """
    n = nutch.Nutch()
    sc = n.Seeds()
    seed_urls = [seed_url]
    seed = sc.create('restful_stream_viz_crawl_seed', seed_urls)
    return n.Crawl(seed, rounds=rounds)


class NutchUrlTrails:
    """
    Class for managing URL Trails visualizations
    """

    @staticmethod
    def strip_url(url):
        """
        Make a URL safe for visualization in Bokeh server
        :param url: a URL to be shortened/stripped
        :return: The stripped URL
        """
        # TODO: remove protocol-stripping on next Bokeh release
        return url.replace('https://', '').replace('http://', '').replace(':', '_').replace('-', '_')[:250]

    @staticmethod
    def jtime_to_datetime(t):
        """
        Convert a Java-format Epoch time stamp into np.datetime64 object
        :param t: Java-format Epoch time stamp (milliseconds)
        :return: A np.datetime64 scalar
        """
        return np.datetime64(datetime.fromtimestamp(t/1000.0))

    def __init__(self, num_urls=25):
        """
        Create a NutchUrlTrails instance for visualizing a running Nutch crawl in real-time using Bokeh
        :param num_urls: The number of URLs to display in the visualization
        :return: A NutchUrLTrails instance
        """
        self.num_urls = num_urls
        self.open_urls = {}
        self.closed_urls = {}
        self.old_segments = None
        self.old_circles = None
        self.plot = None
        self.show_plot = True
        con = Connection()
        # TODO: consider exposing this
        self.queue = con.SimpleQueue(name='fetcher_log', exchange_opts={'durable': False})
        output_server("Streaming Nutch Plot")

    def handle_messages(self):
        """
        Get and parse up to 250 messages from the queue then plot.  Break early if less.
        """

        for i in range(250):
            try:
                m = self.queue.get(block=True, timeout=1)
                self.parse_message(m)
            except Empty:
                break
        self.plot_urls()

    def parse_message(self, message):
        """
        Parse a single message arriving from the queue.  Updates list of open/closed urls.
        :param message: A message from the queue
        """
        print(message.body)
        message = json.loads(message.body)
        url = NutchUrlTrails.strip_url(message["url"])
        if message["eventType"] == "START":
            self.open_urls[url] = NutchUrlTrails.jtime_to_datetime(message["timestamp"])
        elif message["eventType"] == "END":
            self.closed_urls[url] = (self.open_urls[url], NutchUrlTrails.jtime_to_datetime(message["timestamp"]))
            del self.open_urls[url]
        else:
            raise Exception("Unexpected message type")

    def plot_urls(self):
        """
        Visualize crawler activity by showing the most recently crawled URLs and the fetch time.
        """

        # don't plot if no URLs available
        if not (self.open_urls or self.closed_urls):
            return

        # x0/x0, left and right boundaries of segments, correspond to fetch time
        x0 = []
        x = []
        # y-axis, name of URL being fetched
        urls = []

        # maintain x and URL of circles in a separate list
        circles = []
        circle_urls = []

        current_time = np.datetime64(datetime.now())

        # For open URLs (not completed fetching), draw a segment from start time to now
        for url, start_t in self.open_urls.items():
            x0.append(start_t)
            x.append(current_time)
            urls.append(url)

        # For closed URLs (completed fetching), draw a segment from start to end time, and a circle as well.
        for url, (start_t, end_t) in self.closed_urls.items():
            x0.append(start_t)
            x.append(end_t)
            circles.append(end_t)
            urls.append(url)
            circle_urls.append(url)

        x0 = np.asarray(x0)
        x = np.asarray(x)
        circles = np.asarray(circles)

        # sort segments
        sort_index = np.argsort(x0)[::-1]
        x0 = x0[sort_index]
        x = x[sort_index]
        urls = [urls[i] for i in sort_index]

        # sort circles
        if self.closed_urls:
            circle_sort_index = np.argsort(circles)[::-1]
            circles = circles[circle_sort_index]
            circle_urls = [circle_urls[i] for i in circle_sort_index]

        # Filter to latest num_url URLs (ascending order)
        min_x = min(x0[:self.num_urls])
        max_x = max(x)
        x_range = Range1d(min_x, max_x)

        active_min = x0.searchsorted(min_x)
        active_max = x.searchsorted(max_x, side='right')

        # filter segments
        active_x0 = x0[active_min:active_max]
        active_x = x[active_min:active_max]
        active_urls = urls[active_min:active_max]

        # filter circles (some of these might not be displayed)
        if self.closed_urls:
            active_circle_min = circles.searchsorted(min_x)
            active_circle_max = circles.searchsorted(max_x, side='right')
            active_circles = circles[active_circle_min:active_circle_max]
            active_circle_urls = circle_urls[active_circle_min:active_circle_max]

        if self.plot is None:
            self.plot = figure(title="Streaming Nutch Plot", tools="pan,wheel_zoom,resize,save,hover", x_range=x_range,
                               y_range=active_urls, x_axis_type="datetime", width=1200, height=600)
        else:
            self.plot.x_range.start = min_x
            self.plot.x_range.end = max_x
            self.plot.y_range.factors = active_urls

        if self.old_segments:
            self.plot.renderers.remove(self.old_segments)
        if self.old_circles:
            self.plot.renderers.remove(self.old_circles)

        self.old_segments = self.plot.segment(active_x0, active_urls, active_x, active_urls,
                                              line_width=10, line_color="orange")
        if self.closed_urls:
            self.old_circles = self.plot.circle(active_circles, active_circle_urls, size=10,
                                                fill_color="green", line_color="orange", line_width=4)

        # only needs to be done once
        if self.show_plot:
            show(self.plot)
            self.show_plot = False
        cursession().store_objects(self.plot)


def main(seed='https://en.wikipedia.org/wiki/Main_Page', rounds=3, num_urls=25):
    check_supervisord()
    crawl = launch_crawl(seed, rounds)

    url_trails = NutchUrlTrails(num_urls)

    while crawl.progress():
        time.sleep(1)
        crawl.progress()
        url_trails.handle_messages()
    print("\n\n\nCrawl completed.\nTrying to stop supervisord\n")
    term_supervisord()

if __name__ == '__main__':
    # TODO: expose "seed/seeds" "num_urls", and "rounds" to the command line
    main()