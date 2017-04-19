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
import datetime
import pytz
from dateutil import parser

from volttron.platform.vip.agent import Agent, Core, PubSub, RPC, compat
from volttron.platform.agent import utils
from volttron.platform.agent.utils import (get_aware_utc_now,
                                           format_timestamp)

import pandas as pd
import statsmodels.formula.api as sm

utils.setup_logging()
_log = logging.getLogger(__name__)


class TargetAgent(Agent):
    def __init__(self, config_path, **kwargs):
        super(TargetAgent, self).__init__(**kwargs)
        self.config = utils.load_config(config_path)
        self.site = self.config.get('campus')
        self.building = self.config.get('building')
        self.temp_unit = self.config.get('temp_unit')
        self.power_unit = self.config.get('power_unit')
        self.out_temp_name = self.config.get('out_temp_name')
        self.power_name = self.config.get('power_name')
        self.aggregate_in_min = self.config.get('aggregate_in_min')
        self.aggregate_freq = str(self.aggregate_in_min) + 'Min'
        self.ts_name = self.config.get('ts_name')

        self.window_size_in_day = int(self.config.get('window_size_in_day'))
        self.min_required_window_size_in_percent = float(self.config.get('min_required_window_size_in_percent'))
        self.interval_in_min = int(self.config.get('interval_in_min'))
        self.no_of_recs_needed = self.window_size_in_day * 24 * (60 / self.interval_in_min)
        self.min_no_of_records_needed_after_aggr = \
            int(self.min_required_window_size_in_percent/100 *
                self.no_of_recs_needed /
                (self.aggregate_in_min/self.interval_in_min))
        self.schedule_run_in_sec = int(self.config.get('schedule_run_in_hr')) * 3600
        self.tz = self.config.get('tz')
        #Debug
        self.debug_folder = self.config.get('debug_folder')

        # Testing
        #self.no_of_recs_needed = 200
        #self.min_no_of_records_needed_after_aggr = self.no_of_recs_needed/self.aggregate_in_min


    @Core.receiver('onstart')
    def onstart(self, sender, **kwargs):
        #self.core.periodic(self.schedule_run_in_sec, self.publish_target_info)
        self.publish_target_info()


    def get_event_info(self):
        """
        Get event start and end datetime
        Returns:
            A dictionary that has start & end time for event day
        """
        event_info = {}
        tz = pytz.timezone(self.tz)
        event_info['start'] = datetime.datetime(2017, 4, 14, 13, 0, 0, tzinfo=tz)
        event_info['end'] = datetime.datetime(2017, 4, 30, 17, 0, 0, tzinfo=tz)
        return event_info

    def get_baseline_target(self, dt1, dt2):
        """
        Get baseline value from PGNE agent
        Returns:
            Average value of the next 2 baseline prediction
        """
        # prediction for dt1
        prediction1 = 90
        # prediction for dt2
        prediction2 = 95
        # avg
        baseline_target = (prediction1+prediction2)/2.0

        return baseline_target

    @RPC.export('get_target_info')
    def get_target_info(self):
        """
        Combine event start, end, and baseline target
        Returns:
            A dictionary of start, end datetime and baseline target
        """
        target_info = {}
        event_info = self.get_event_info()
        if len(event_info.keys()) > 0:
            start = event_info['start']
            end = event_info['end']
            cur_time = datetime.datetime.now(pytz.timezone(self.tz))
            if start <= cur_time < end:
                one_hour = datetime.timedelta(hours=1)
                next_hour = \
                    cur_time.replace(minute=0, second=0, microsecond=0) + one_hour
                next_hour_end = next_hour.replace(minute=59, second=59)
                baseline_target = \
                    self.get_baseline_target(next_hour, next_hour+one_hour)
                target_info['id'] = format_timestamp(next_hour)
                target_info['start'] = format_timestamp(next_hour)
                target_info['end'] = format_timestamp(next_hour_end)
                target_info['target'] = baseline_target

        return target_info

    def publish_target_info(self):
        target_info = self.get_target_info()
        if len(target_info.keys()) > 0:
            headers = {'Date': format_timestamp(get_aware_utc_now())}
            tz = self.tz
            target_meta = {'type': 'float', 'tz': tz, 'units': 'kW'}
            target_topic = '/'.join([self.site, self.building, 'target'])
            target_msg = [{
                "id": target_info['id'],
                "start": target_info['start'],
                "end": target_info['end'],
                "target": target_info['target']
            }, target_meta]
            self.vip.pubsub.publish(
                'pubsub', target_topic, headers, target_msg).get(timeout=10)
            _log.debug("{topic}: {value}".format(
                topic=target_topic,
                value=target_msg))
            # Schedule the next run at minute 30 of next hour
            cur_time = datetime.datetime.now(pytz.timezone(self.tz))
            one_hour = datetime.timedelta(hours=1)
            next_update_time = \
                cur_time.replace(minute=30, second=0, microsecond=0) + one_hour
            self._still_connected_event = self.core.schedule(
                next_update_time, self.publish_target_info)

def main(argv=sys.argv):
    '''Main method called by the eggsecutable.'''
    try:
        utils.vip_main(TargetAgent, identity='target_agent')
    except Exception as e:
        _log.exception('unhandled exception')

if __name__ == '__main__':
    # Entry point for script
    sys.exit(main())
