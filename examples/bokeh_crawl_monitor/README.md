Example of using the Nutch REST API, a RabbitMQ message broker, and Bokeh server to visualize a Nutch crawl in real-time.

# INSTALLATION

Nutch and RabbitMQ need to be installed separately.

RabbitMQ >= 3.5 is recommended
Nutch 1.11 (or svn>1707174) is required

The rest of the requirements can be installed via conda env, which requires that Anaconda or Miniconda is installed. Miniconda is a minimal Anaconda installation that bootstraps conda and Python on any operating system. Install Anaconda from http://continuum.io/downloads or Miniconda from http://conda.pydata.org/miniconda.html

Then create the conda environment for this example:

```
conda env create

```

This will create a `bokeh_stream` environment with the remaining requirements for this example.

To activate the environment in the future (once per shell session):

```
source activate bokeh_stream
```

To run the demo, activate the environment, start the supervisor, wait a few seconds for the
Nutch server to become available, then start the crawl monitoring script.

```
source activate bokeh_stream
supervisord &
sleep 3
python nutch_crawl_bokeh_monitor.py
```
