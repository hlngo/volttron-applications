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
from datetime import datetime, timedelta
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
        self.tz = self.config.get('tz')
        self.debug_folder = self.config.get('debug_folder')

    @Core.receiver('onstart')
    def onstart(self, sender, **kwargs):
        # for real time
        #cur_time = local_tz.localize(datetime.now())
        # for simulation
        local_tz = pytz.timezone(self.tz)
        cur_time = local_tz.localize(datetime(2017, 8, 15, 13, 0, 0))
        self.publish_target_info(format_timestamp(cur_time), self.tz)
        # subscribe to ILC start event
        ilc_start_topic = '/'.join([self.site, self.building, 'ilc/start'])
        _log.debug('Subscribing to ' + ilc_start_topic)
        self.vip.pubsub.subscribe(peer='pubsub',
                                  prefix=ilc_start_topic,
                                  callback=self.on_ilc_start)

    def on_ilc_start(self, peer, sender, bus, topic, headers, message):
        local_tz = pytz.timezone(self.tz)
        cur_time = local_tz.localize(datetime.now())
        self.publish_target_info(format_timestamp(cur_time), self.tz)

    def get_event_info(self):
        """
        Get event start and end datetime from OpenADR agent
        Returns:
            A dictionary that has start & end time for event day
        """
        event_info = {}
        # for simulation
        local_tz = pytz.timezone(self.tz)
        event_info['start'] = local_tz.localize(datetime(2017, 8, 15, 13, 0, 0))
        event_info['end'] = local_tz.localize(datetime(2017, 8, 15, 17, 0, 0))

        return event_info

    def get_baseline_target(self, cur_time):
        """
        Get baseline value from PGNE agent
        Returns:
            Average value of the next 2 baseline prediction
        """
        baseline_target = None
        message = self.vip.rpc.call(
            'baseline_agent', 'get_prediction',
            format_timestamp(cur_time), self.tz).get(timeout=100)
        if len(message) > 0:
            values = message[0]
            prediction1 = float(values["value_hr1"])
            prediction2 = float(values["value_hr2"])
            baseline_target = (prediction1+prediction2)/2.0

        return baseline_target

    @RPC.export('get_target_info')
    def get_target_info(self, in_time, in_tz):
        """
        Combine event start, end, and baseline target
        Inputs:
            in_time: string cur_time
            in_tz: string timezone
        Returns:
            A dictionary of start, end datetime and baseline target
        """
        target_info = []
        event_info = self.get_event_info()
        if len(event_info.keys()) > 0:
            start = event_info['start']
            end = event_info['end']

            cur_time = parser.parse(in_time)
            if cur_time.tzinfo is None:
                tz = pytz.timezone(in_tz)
                cur_time = tz.localize(cur_time)

            if start <= cur_time < end:
                one_hour = timedelta(hours=1)
                next_hour = \
                    cur_time.replace(minute=0, second=0, microsecond=0) + one_hour
                next_hour_end = next_hour.replace(minute=59, second=59)
                baseline_target = \
                    self.get_baseline_target(cur_time)
                if baseline_target is not None:
                    meta = {'type': 'float', 'tz': self.tz, 'units': 'kW'}
                    time_meta = {'type': 'datetime', 'tz': self.tz, 'units': 'datetime'}
                    target_info = [{
                        "id": format_timestamp(next_hour),
                        "start": format_timestamp(next_hour),
                        "end": format_timestamp(next_hour_end),
                        "target": baseline_target
                    }, {
                        "id": time_meta,
                        "start": time_meta,
                        "end": time_meta,
                        "target": meta
                    }]
                _log.debug(
                    "At time {ts} target info is {ti}".format(ts=cur_time,
                                                              ti=target_info))
        return target_info

    def publish_target_info(self, in_time, in_tz):
        cur_time = parser.parse(in_time)
        if cur_time.tzinfo is None:
            tz = pytz.timezone(in_tz)
            cur_time = tz.localize(cur_time)

        # local_tz = pytz.timezone(self.tz)
        # if cur_time is None:
        #     cur_time = local_tz.localize(datetime.now())

        message = self.get_target_info(format_timestamp(cur_time), self.tz)
        if len(message) > 0:
            target_info = message[0]
            headers = {'Date': format_timestamp(get_aware_utc_now())}
            meta = {'type': 'float', 'tz': self.tz, 'units': 'kW'}
            time_meta = {'type': 'datetime', 'tz': self.tz, 'units': 'datetime'}
            target_topic = '/'.join(['analysis','target_agent',self.site, self.building, 'goal'])
            target_msg = [{
                "id": target_info['id'],
                "start": target_info['start'],
                "end": target_info['end'],
                "target": target_info['target']
            }, {
                "id": time_meta,
                "start": time_meta,
                "end": time_meta,
                "target": meta
            }]
            self.vip.pubsub.publish(
                'pubsub', target_topic, headers, target_msg).get(timeout=10)
            _log.debug("{topic}: {value}".format(
                topic=target_topic,
                value=target_msg))
            # Schedule the next run at minute 30 of next hour
            one_hour = timedelta(hours=1)
            next_update_time = \
                cur_time.replace(minute=30, second=0, microsecond=0) + one_hour
            self._still_connected_event = self.core.schedule(
                next_update_time, self.publish_target_info,
                format_timestamp(next_update_time), self.tz)

def main(argv=sys.argv):
    '''Main method called by the eggsecutable.'''
    try:
        utils.vip_main(TargetAgent)
    except Exception as e:
        _log.exception('unhandled exception')

if __name__ == '__main__':
    # Entry point for script
    sys.exit(main())
