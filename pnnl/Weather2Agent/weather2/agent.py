# -*- coding: utf-8 -*- {{{
# vim: set fenc=utf-8 ft=python sw=4 ts=4 sts=4 et:

# Copyright (c) 2016, Battelle Memorial Institute
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and documentation
# are those of the authors and should not be interpreted as representing
# official policies, either expressed or implied, of the FreeBSD
# Project.
#
# This material was prepared as an account of work sponsored by an
# agency of the United States Government.  Neither the United States
# Government nor the United States Department of Energy, nor Battelle,
# nor any of their employees, nor any jurisdiction or organization that
# has cooperated in the development of these materials, makes any
# warranty, express or implied, or assumes any legal liability or
# responsibility for the accuracy, completeness, or usefulness or any
# information, apparatus, product, software, or process disclosed, or
# represents that its use would not infringe privately owned rights.
#
# Reference herein to any specific commercial product, process, or
# service by trade name, trademark, manufacturer, or otherwise does not
# necessarily constitute or imply its endorsement, recommendation, or
# favoring by the United States Government or any agency thereof, or
# Battelle Memorial Institute. The views and opinions of authors
# expressed herein do not necessarily state or reflect those of the
# United States Government or any agency thereof.
#
# PACIFIC NORTHWEST NATIONAL LABORATORY
# operated by BATTELLE for the UNITED STATES DEPARTMENT OF ENERGY
# under Contract DE-AC05-76RL01830

# }}}
import os
import sys
import logging
import json
import time
import urllib2
import pytz
from dateutil import parser
from datetime import datetime, timedelta

from volttron.platform.vip.agent import Agent, Core, PubSub, RPC, compat
from volttron.platform.agent import utils
from volttron.platform.agent.utils import (get_aware_utc_now,
                                           format_timestamp)
from volttron.platform.messaging import headers as headers_mod, topics

__version__ = '1.0.0'

HEADER_NAME_DATE = headers_mod.DATE
HEADER_NAME_CONTENT_TYPE = headers_mod.CONTENT_TYPE

utils.setup_logging()
_log = logging.getLogger(__name__)

location = ["local_tz_long", "observation_location", "display_location",
            "station_id"]
time_topics = ["local_time_rfc822", "local_tz_short", "local_tz_offset",
               "local_epoch", "observation_time", "observation_time_rfc822",
               "observation_epoch"]
temperature = ["temperature_string", "temp_f", "temp_c", "feelslike_c",
               "feelslike_f", "feelslike_string", "windchill_c",
               "windchill_f", "windchill_string", "heat_index_c",
               "heat_index_f", "heat_index_string"]
wind = ["wind_gust_kph", "wind_string", "wind_mph", "wind_dir",
        "wind_degrees", "wind_kph", "wind_gust_mph", "pressure_in"]
cloud_cover = ["weather", "solarradiation", "visibility_mi", "visibility_km",
               "UV"]
precipitation = ["dewpoint_string", "precip_today_string", "dewpoint_f",
                 "dewpoint_c", "precip_today_metric", "precip_today_in",
                 "precip_1hr_in", "precip_1hr_metric", "precip_1hr_string"]
pressure_humidity = ["pressure_trend", "pressure_mb", "relative_humidity"]
observation_epoch = 'observation_epoch'

all_current = [observation_epoch] + temperature + wind + cloud_cover + precipitation + pressure_humidity

history_points = ["tempm", "tempi", "dewptm", "dewpti", "hum", "wspdm", "wspdi", "wgustm", "wgusti", "wdird", "wdire", "vism", "visi", "pressurem", "pressurei", "windchillm", "windchilli", "heatindexm", "heatindexi", "precipm", "precipi", "conds", "icon", "fog", "rain", "snow", "hail", "thunder", "tornado", "metar"]
all_history = history_points

forecast_points = ["temp", "dewpoint", "condition", "sky", "wspd", "wdir", "wx", "uvi", "humidity", "windchill", "heatindex", "feelslike", "qpf", "snow", "pop", "mslp"]
all_forecast = forecast_points


class Weather2Agent(Agent):
    """
    Currently using data from WU.
    TODO: Integrate NOAA and TMY3 data
    """
    def __init__(self, config_path, **kwargs):
        super(Weather2Agent, self).__init__(**kwargs)

        self.config = utils.load_config(config_path)
        self.wu_api_key = self.config.get('wu_api_key')
        self.operation_mode = self.config.get('operation_mode', 2)
        self.poll_time = self.config.get('poll_time')
        self.zip = self.config.get('zip')
        self.city = self.config.get('city')
        self.region = self.config.get('region')

        self.supported_features = ['history',
                                   'current',
                                   'forecast3', 'forecast10']
        self.request_topic_prefix = 'weather2/request'
        self.response_topic_prefix = 'weather2/response'
        self.wu_service = Wunderground(self.wu_api_key)

    @Core.receiver('onstart')
    def onstart(self, sender, **kwargs):
        _log.debug('Weather2Agent: Subscribing to ' + self.request_topic_prefix)
        self.vip.pubsub.subscribe(peer='pubsub',
                                  prefix=self.request_topic_prefix,
                                  callback=self.on_request)
        if self.operation_mode == 2 and self.poll_time is not None:
            self.weather = self.core.periodic(self.poll_time,
                                              self.on_polling,
                                              wait=0)



    def on_request(self, peer, sender, bus, topic, headers, message):
        #
        # weather2/request/{feature}/{region}/{city}/all
        # weather2/request/{feature}/{region}/{city}/{point}
        #
        feature = region = city = zip_code = point = start = end = None

        # Analyze request topic
        try:
            topic_parts = topic.split('/')
            topic_parts = [x.lower() for x in topic_parts]
            (feature, region, city, point) = topic_parts[2:6]
            if len(topic_parts) > 6:
                start = topic_parts[6]
            if len(topic_parts) > 7:
                end = topic_parts[7]

            if not self.is_point_supported(feature, point, start, end):
                _log.debug("Weather2Agent: Not supported topic " + topic)
                return
            if region == 'ZIP':
                zip_code = city
        except ValueError as (errno, strerror):
            _log.debug("Unpack request error: " + strerror)
            return

        # Query weather data
        kwargs = {}
        wu_resp = None

        if zip_code is not None:
            kwargs['zip'] = zip_code
        else:
            kwargs['region'] = region
            kwargs['city'] = city

        publish_items = []
        if feature == 'history':
            kwargs['start_date'] = parser.parse(start)
            if end is not None:
                end_time = parser.parse(end)
                if end_time >= kwargs['start_date'] + timedelta(days=1):
                    kwargs['end_date'] = end_time
            wu_resp = self.wu_service.history(**kwargs) #array
            publish_items = self.build_resp_history(wu_resp)
        elif feature == 'current':
            wu_resp = self.wu_service.current(**kwargs)
            publish_items = self.build_resp_current(wu_resp)
        elif feature == 'forecast3':
            wu_resp = self.wu_service.forecast_3day(**kwargs)
            publish_items = self.build_resp_forecast(wu_resp)
        elif feature == 'forecast10':
            wu_resp = self.wu_service.forecast_10day(**kwargs)
            publish_items = self.build_resp_forecast(wu_resp)

        if len(publish_items) > 0:
            headers = {
                HEADER_NAME_DATE: format_timestamp(utils.get_aware_utc_now()),
                HEADER_NAME_CONTENT_TYPE: headers_mod.CONTENT_TYPE.JSON
            }
            resp_topic = topic.replace('request', 'response')
            self.vip.pubsub.publish(peer='pubsub',
                                    topic=resp_topic,
                                    message=publish_items,
                                    headers=headers)

    def build_resp_history(self, wu_resp_arr):
        publish_items = []
        if wu_resp_arr is None:
            return publish_items

        try:
            for wu_resp in wu_resp_arr:
                parsed_json = json.loads(wu_resp)
                if 'history' in parsed_json and 'observations' in parsed_json['history']:
                    observations = parsed_json['history']['observations']
                    for observation in observations:
                        parsed_values = {}
                        obs_time_json = observation['utcdate']
                        t = datetime(int(obs_time_json['year']),
                                     int(obs_time_json['mon']),
                                     int(obs_time_json['mday']),
                                     int(obs_time_json['hour']),
                                     int(obs_time_json['min']), tzinfo=pytz.utc)
                        # Convert time to epoch string to be consistent with current condition
                        parsed_values[observation_epoch] = str(int((t - datetime(1970,1,1,tzinfo=pytz.utc)).total_seconds()))
                        for point in all_history:
                            if point in observation:
                                parsed_values[point] = observation[point]
                        if parsed_values:
                            publish_items.append(parsed_values)
        except Exception as e:
            _log.exception(e)

        return publish_items

    def build_resp_current(self, wu_resp):
        publish_items = []
        if wu_resp is None:
            return publish_items

        try:
            parsed_values = {}
            parsed_json = json.loads(wu_resp)
            if 'current_observation' in parsed_json:
                cur_observation = parsed_json['current_observation']
                for point in all_current:
                    if point in cur_observation:
                        parsed_values[point] = cur_observation[point]
                if parsed_values:
                    publish_items.append(parsed_values)
        except Exception as e:
            _log.exception(e)

        return publish_items

    def build_resp_forecast(self, wu_resp):
        publish_items = []
        if wu_resp is None:
            return publish_items

        try:
            parsed_json = json.loads(wu_resp)
            if 'hourly_forecast' in parsed_json:
                observations = parsed_json['hourly_forecast']
                for observation in observations:
                    parsed_values = {}
                    parsed_values[observation_epoch] = observation['FCTTIME']['epoch']
                    for point in all_forecast:
                        if point in observation:
                            if isinstance(observation[point], dict) \
                                    and 'english' in observation[point]:
                                parsed_values[point] = observation[point]['english']
                            else:
                                parsed_values[point] = observation[point]
                    if parsed_values:
                        publish_items.append(parsed_values)
        except Exception as e:
            _log.exception(e)

        return publish_items

    def on_polling(self):
        if self.zip is None and (self.region is None or self.city is None):
            return

        kwargs = {}
        if self.zip is not None:
            kwargs['zip'] = self.zip
            topic = 'weather2/polling/current/ZIP/{zip}/all'.format(zip=self.zip)
        else:
            kwargs['region'] = self.region
            kwargs['city'] = self.city
            topic = 'weather2/polling/current/{region}/{city}/all'.format(
                region=self.region,
                city=self.city
            )
        wu_resp = self.wu_service.current(**kwargs)
        publish_items = self.build_resp_current(wu_resp)

        if len(publish_items) > 0:
            headers = {
                HEADER_NAME_DATE: format_timestamp(utils.get_aware_utc_now()),
                HEADER_NAME_CONTENT_TYPE: headers_mod.CONTENT_TYPE.JSON
            }
            self.vip.pubsub.publish(peer='pubsub',
                                    topic=topic,
                                    message=publish_items,
                                    headers=headers)
            _log.debug(publish_items)

    def is_feature_supported(self, feature):
        if feature in self.supported_features:
            return True
        return False

    def is_point_supported(self, feature, point, start, end):
        if not self.is_feature_supported(feature):
            return False

        if feature == 'history':
            if start is None:
                return False
            try:
                start_time = parser.parse(start)
                if end is not None:
                    end_time = parser.parse(end)
            except:
                return False


        if point == 'all':
            return True

        # Support all points for now. Limit later.
        return True


class Wunderground:
    """Requests weather data from WeatherUnderground and provides following services:
        1) Historical data
        2) Current data
        3) Forecast data
        Weather data service is provided per zip code or city
        TODO: provide services based on airport_code, weather_station, etc.
    """
    def __init__(self, wu_api_key):
        self.api_key = wu_api_key
        self.base_url = "http://api.wunderground.com/api/{api_key}/{feature}/q/{query}.json"

    def history(self, **kwargs):
        ret_val = []
        if 'start_date' in kwargs:
            start_date = kwargs['start_date']
            if 'end_date' in kwargs:
                end_date = kwargs['end_date']
            else:
                end_date = start_date + timedelta(days=1)

            for a_date in date_range(start_date, end_date):
                kwargs['feature'] = 'history_' + a_date.strftime("%Y%m%d")
                one_date = self.query(**kwargs)
                ret_val.append(one_date)

        return ret_val

    def yesterday(self, **kwargs):
        kwargs['feature'] = 'yesterday'
        return self.query(**kwargs)

    def current(self, **kwargs):
        kwargs['feature'] = 'conditions'
        return self.query(**kwargs)

    def forecast_3day(self, **kwargs):
        kwargs['feature'] = 'hourly'
        return self.query(**kwargs)

    def forecast_10day(self, **kwargs):
        kwargs['feature'] = 'hourly10day'
        return self.query(**kwargs)

    def query(self, **kwargs):
        query_resp = None
        feature = kwargs['feature']

        if 'zip' in kwargs:
            query = kwargs['zip']
        elif 'city' in kwargs and 'region' in kwargs:
            query = "{region}/{city}".format(region=kwargs['region'], city=kwargs['city'])
        else:
            query = None

        if query is not None and feature is not None:
            url = self.base_url.format(api_key=self.api_key,
                                       feature=feature,
                                       query=query)
            try:
                f = urllib2.urlopen(url)
                query_resp = f.read()
                f.close()
            except:
                # Log exception
                query_resp = None

        return query_resp


def main(argv=sys.argv):
    '''Main method called by the eggsecutable.'''
    try:
        utils.vip_main(Weather2Agent)
    except Exception as e:
        _log.exception('unhandled exception')

if __name__ == '__main__':
    # Entry point for script
    sys.exit(main())
