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
        self.core.periodic(self.schedule_run_in_sec, self.publish_target_info)

    def get_event_info(self):
        """
        Get event start and end datetime
        Returns:
            A dictionary that has start & end time for event day
        """
        tz = pytz.timezone(self.tz)
        start_time = datetime.datetime(2016, 5, 1, 13, 0, 0, tzinfo=tz)
        end_time = datetime.datetime(2016, 5, 1, 17, 0, 0, tzinfo=tz)
        return {
            'start': format_timestamp(start_time),
            'end': format_timestamp(end_time)
        }

    def get_baseline_target(self):
        """
        Get baseline value from PGNE agent
        Returns:
            Average value of the next 2 baseline prediction
        """
        prediction1 = 90
        prediction2 = 95
        baseline_target = (prediction1+prediction2)/2.0
        return baseline_target

    @RPC.export('get_target_info')
    def get_target_info(self):
        """
        Combine event start, end, and baseline target
        Returns:
            A dictionary of start, end datetime and baseline target
        """
        target_info = self.get_event_info()
        baseline_target = self.get_baseline_target()
        target_info['target'] = baseline_target
        return target_info

    def publish_target_info(self):
        target_info = self.get_target_info()
        headers = {'Date': format_timestamp(get_aware_utc_now())}
        tz = self.tz
        time_meta = {'type': 'datetime', 'tz': tz, 'units': 'ISO-8601'}
        target_meta = {'type': 'float', 'tz': tz, 'units': 'kW'}
        #start
        start_topic = 'target/start'
        start_msg = [{"value": target_info['start']}, time_meta]
        #end
        end_topic = 'target/end'
        end_msg = [{"value": target_info['end']}, time_meta]
        #target
        target_topic = 'target/target'
        target_msg = [{"value": target_info['target']}, target_meta]
        self.vip.pubsub.publish(
            'pubsub', start_topic, headers, start_msg).get(timeout=10)
        self.vip.pubsub.publish(
            'pubsub', end_topic, headers, end_msg).get(timeout=10)
        self.vip.pubsub.publish(
            'pubsub', target_topic, headers, target_msg).get(timeout=10)
        _log.debug("{topic}: {value}".format(
            topic=start_topic,
            value=start_msg))
        _log.debug("{topic}: {value}".format(
            topic=end_topic,
            value=end_msg))
        _log.debug("{topic}: {value}".format(
            topic=target_topic,
            value=target_msg))

def main(argv=sys.argv):
    '''Main method called by the eggsecutable.'''
    try:
        utils.vip_main(TargetAgent, identity='target_agent')
    except Exception as e:
        _log.exception('unhandled exception')

if __name__ == '__main__':
    # Entry point for script
    sys.exit(main())
