from __future__ import division
from __future__ import print_function

import time

from datetime import datetime, timedelta
from Queue import Empty
import json

import numpy as np

from bokeh.models import Range1d, FactorRange, CategoricalTickFormatter
from bokeh.plotting import cursession, figure, output_file, output_server, show

from kombu import Connection

def plot_server_stream():
    con = Connection()
    queue = con.SimpleQueue(name='fetcher_log', exchange_opts={'durable': False})

    output_server("Streaming Nutch Plot")

    def jtime_to_datetime(t):
        return np.datetime64(datetime.fromtimestamp(t/1000.0))

    m_bodies = [
        {"eventType":"START","eventData":None,"url":"http://www.google.com/","timestamp":1444002262091},
        {"eventType":"START","eventData":None,"url":"http://aron.ahmadia.net/","timestamp":1444002262095},
        {"eventType":"END","eventData":{"status":"success"},"url":"http://aron.ahmadia.net/","timestamp":1444002262604},
        {"eventType":"END","eventData":{"status":"success"},"url":"http://www.google.com/","timestamp":1444002262613},
    ]


    def strip_url(url):
        return url.replace('https://','').replace('http://','').replace(':', '_').replace('-', '_')[:250]

    def parse_message(message, open_urls, closed_urls):
        print(message.body)
        message = json.loads(message.body)
        url = strip_url(message["url"])
        if message["eventType"] == "START":
            open_urls[url] = jtime_to_datetime(message["timestamp"])
        elif message["eventType"] == "END":
            closed_urls[url] = (open_urls[url], jtime_to_datetime(message["timestamp"]))
            del open_urls[url]
        else:
            raise Exception("Unexpected message type")

    def plot_urls(open_urls, closed_urls, current_time=None, p1=None, old_segments=None, old_circles=None, show_plot=False):
        x0 = []
        x = []
        urls = []
        urlcircle = []
        xcircle = []

        if current_time is None:
            current_time = np.datetime64(datetime.now())

        for url, start_t in open_urls.items():
            x0.append(start_t)
            x.append(current_time)
            urls.append(url)

        for url, (start_t, end_t) in closed_urls.items():
            x0.append(start_t)
            x.append(end_t)
            xcircle.append(end_t)
            urlcircle.append(end_t)
            urls.append(url)

        x0 = np.asarray(x0)
        x = np.asarray(x)
        xcircle = np.asarray(xcircle)
        urlcircle = np.asarray(urlcircle)

        #sort
        sort_index = np.argsort(x0)[::-1]
        x0 = x0[sort_index]
        x = x[sort_index]
        urls = [urls[i] for i in sort_index]

        # Filter to latest 25 URLs (ascending order)
        min_x = min(x0[:25])
        max_x = max(x)
        x_range = Range1d(min_x, max_x)

        active_min = x0.searchsorted(min_x)
        active_max = x.searchsorted(max_x, side='right')

        # filter
        active_x = x[active_min:active_max]
        active_x0 = x0[active_min:active_max]
        active_urls = urls[active_min:active_max]
        active_xcircle = xcircle[active_min:active_max]
        active_urlcircle = urlcircle[active_min:active_max]

        print(active_urls)
        print(max_x)

        if p1 is None:
            p1 = figure(title="Streaming Nutch Plot", tools="pan,wheel_zoom,resize,save,hover", y_range=active_urls, x_axis_type="datetime", width=1200, height=600)
            p1.x_range = x_range
        p1.x_range.start = min_x
        p1.x_range.end = max_x
        p1.y_range.factors = active_urls

        if old_segments:
            p1.renderers.remove(old_segments)
        if old_circles:
            p1.renderers.remove(old_circles)

        old_segments = p1.segment(active_x0, active_urls, active_x, active_urls, line_width=10, line_color="orange")
        old_circles = p1.circle(active_xcircle, active_urlcircle, size=5, fill_color="green", line_color="orange", line_width=12)

        if show_plot:
            show(p1)
        cursession().store_objects(p1)

        return p1, old_segments, old_circles

    open_urls = {}
    closed_urls = {}

    try:
        m = queue.get(block=True)
        parse_message(m, open_urls, closed_urls)
    except Empty:
        raise Exception("Unable to retrieve any useful messages")
    p1, old_segments, old_circles = plot_urls(open_urls, closed_urls, show_plot=True)

    while True:
        time.sleep(1)
        while True:
            try:
                m = queue.get(block=True, timeout=1)
                parse_message(m, open_urls, closed_urls)
            except Empty:
                break
        p1, old_segments, old_circles = plot_urls(open_urls, closed_urls, None, p1, old_segments, old_circles)

if __name__ == '__main__':
    plot_server_stream()
#    plot_crawl_lines()
