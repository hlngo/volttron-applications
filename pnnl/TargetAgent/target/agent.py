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
import dateutil.tz
from dateutil import parser

from volttron.platform.vip.agent import Agent, Core, PubSub, RPC, compat
from volttron.platform.agent import utils
from volttron.platform.agent.utils import (get_aware_utc_now,
                                           format_timestamp)

__version__ = '1.0.0'

utils.setup_logging()
_log = logging.getLogger(__name__)


class TargetAgent(Agent):
    def __init__(self, config_path, **kwargs):
        super(TargetAgent, self).__init__(**kwargs)
        self.config = utils.load_config(config_path)

        # Building info
        self.site = self.config.get('campus')
        self.building = self.config.get('building')

        # Local timezone
        self.tz = self.config.get('tz')
        self.local_tz = pytz.timezone(self.tz)

        # Bidding value
        self.cbp = self.config.get('cbp', None)
        if self.cbp is None:
            raise "CBP values are required"
        if len(self.cbp)<24:
            raise "CBP is required for every hour (i.e., 24 values)"

        #Occupancy
        self.cont_after_dr = self.config.get('cont_after_dr')
        self.occ_time = self.config.get('occ_time')
        if self.cont_after_dr == 'yes':
            try:
                self.occ_time = parser.parse(self.occ_time)
                self.occ_time = self.local_tz.localize(self.occ_time)
                self.occ_time_utc = self.occ_time.astimezone(pytz.utc)
            except:
                raise "The DR was set to continue after end time " \
                      "but could not parse occupancy end time"

        # DR mode
        self.dr_mode = self.config.get('dr_mode')
        self.start_time = self.config.get('start_time')
        self.end_time = self.config.get('end_time')
        self.cur_time = self.config.get('cur_time')
        if self.dr_mode == 'manual' or self.dr_mode == 'auto':
            try:
                self.start_time = parser.parse(self.start_time)
                self.end_time = parser.parse(self.end_time)
                self.cur_time = parser.parse(self.cur_time)
            except:
                raise "The DR mode is manual mode but could not " \
                      "parse start, end, or current time"

        #Simulation
        self.simulation = self.config.get('simulation_running', True)
        self.last_publish_time = None

        _log.debug("TARGET_AGENT_DEBUG: Running simulation {}".format(self.simulation))

    @Core.receiver('onstart')
    def onstart(self, sender, **kwargs):
        _log.debug('TargetAgent: OnStart ')
        one_hour = timedelta(hours=1)
        cur_time = self.local_tz.localize(datetime.now())
        if self.dr_mode == 'manual':
            cur_time = self.local_tz.localize(self.cur_time)

        # Set cur_time to previous hour to get current hour baseline values
        cur_time_utc = cur_time.astimezone(pytz.utc)
        prev_time_utc = cur_time_utc - one_hour

        # subscribe to ILC start event
        ilc_start_topic = '/'.join([self.site, self.building, 'ilc/start'])
        _log.debug('TargetAgent: Subscribing to ' + ilc_start_topic)
        self.vip.pubsub.subscribe(peer='pubsub',
                                  prefix=ilc_start_topic,
                                  callback=self.on_ilc_start)

        if self.simulation:
            manual_periodic = '/'.join(['devices', self.site, self.building, 'METERS'])
            _log.debug("TARGET_AGENT_DEBUG: Simulation handler topic -- {}".format(manual_periodic))
            self.vip.pubsub.subscribe(peer='pubsub',
                                      prefix=manual_periodic,
                                      callback=self.simulation_publish_handler)

        # Always put these lines at the end of the method
        self.publish_target_info(format_timestamp(cur_time_utc))

    def simulation_publish_handler(self, peer, sender, bus, topic, headers, message):
        _log.debug("TARGET_AGENT_DEBUG: Running simulation publish handler")
        tz_replace = dateutil.tz.gettz(self.tz)
        current_time = parser.parse(headers['Date']).replace(tzinfo=tz_replace)

        if self.last_publish_time is None:
            self.last_publish_time = current_time

        _log.debug("TARGET_AGENT_DEBUG: current time - {} ----- last publish time - {}".format(current_time,
                                                                                               self.last_publish_time))

        if self.last_publish_time is not None and (current_time - self.last_publish_time) >= timedelta(minutes=5):
            _log.debug("TARGET_AGENT_DEBUG: Running periodic publish.")
            self.publish_target_info(format_timestamp(current_time))
            self.last_publish_time = current_time

    def on_ilc_start(self, peer, sender, bus, topic, headers, message):
        cur_time = self.local_tz.localize(datetime.now())
        cur_time_utc = cur_time.astimezone(pytz.utc)
        one_hour = timedelta(hours=1)
        prev_time_utc = cur_time_utc - one_hour
        self.publish_target_info(format_timestamp(prev_time_utc))
        self.publish_target_info(format_timestamp(cur_time_utc))

    def get_dr_days(self):
        """
        Get DR days from historian previous published by OpenADR agent
        Returns:
            A list of dr event days
        """
        dr_days = self.config.get('dr_days', [])
        parsed_dr_days = []
        for dr_day in dr_days:
            try:
                parsed_dr_day = parser.parse(dr_day)
                if parsed_dr_day.tzinfo is None:
                    parsed_dr_day = self.local_tz.localize(parsed_dr_day)
                parsed_dr_day_utc = parsed_dr_day.astimezone(pytz.utc)
                parsed_dr_days.append(format_timestamp(parsed_dr_day_utc))
            except:
                _log.error(
                    "TARGETAGENT: Could not parse DR day {d}".format(d=dr_day))

        return parsed_dr_days

    def get_event_info(self):
        """
        Get event start and end datetime from OpenADR agent
        Returns:
            A dictionary that has start & end time for event day
        """
        # Get info from OpenADR, with timezone info

        if self.dr_mode == 'openADR': #from OpenADR agent
            start_time = self.local_tz.localize(datetime(2017, 5, 3, 13, 0, 0))
            end_time = self.local_tz.localize(datetime(2017, 5, 3, 17, 0, 0))
        else:
            start_time = self.local_tz.localize(self.start_time)
            end_time = self.local_tz.localize(self.end_time)

        # for simulation
        event_info = {}
        event_info['start'] = start_time.astimezone(pytz.utc)
        event_info['end'] = end_time.astimezone(pytz.utc)

        return event_info

    def get_baseline_target(self, cur_time_utc, start_utc, end_utc, cbp):
        """
        Get baseline value from PGNE agent
        Returns:
            Average value of the next 2 baseline prediction
        """
        baseline_target = None
        message = []
        dr_days = self.get_dr_days()
        try:
            message = self.vip.rpc.call(
                'baseline_agent', 'get_prediction',
                format_timestamp(cur_time_utc),
                format_timestamp(start_utc),
                format_timestamp(end_utc),
                'UTC', dr_days).get(timeout=60)
        except:
            _log.debug("TargetAgentError: Cannot RPC call to PGnE baseline agent")

        if len(message) > 0:
            values = message[0]
            prediction1 = float(values["value_hr1"])
            prediction2 = float(values["value_hr2"])
            #baseline_target = (prediction1+prediction2)/2.0
            baseline_target = prediction1
            baseline_target -= cbp
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
        _log.debug("TargetAgent: event info length is " +
                   str(len(event_info.keys())))
        if len(event_info.keys()) > 0:
            start = event_info['start']
            end = event_info['end']
            _log.debug('TargetAgent: EventInfo '
                       'Start: {start} End: {end} '.format(start=start,
                                                           end=end))
            cur_time = parser.parse(in_time)
            if cur_time.tzinfo is None:
                tz = pytz.timezone(in_tz)
                cur_time = tz.localize(cur_time)

            # Convert to UTC before doing any processing
            start_utc = start.astimezone(pytz.utc)
            end_utc = end.astimezone(pytz.utc)
            cur_time_utc = cur_time.astimezone(pytz.utc)
            one_hour = timedelta(hours=1)
            start_utc_prev_hr = start_utc - one_hour
            end_utc_prev_hr = end_utc - one_hour

            # Use occupancy time if cont_after_dr is enabled
            if self.cont_after_dr == 'yes':
                end_utc_prev_hr = self.occ_time_utc - one_hour

            if start_utc_prev_hr <= cur_time_utc < end_utc_prev_hr:
                next_hour_utc = \
                    cur_time_utc.replace(minute=0, second=0, microsecond=0) + one_hour
                next_hour_end = next_hour_utc.replace(minute=59, second=59)

                # Decide cpb value
                cur_time_local = cur_time_utc.astimezone(self.local_tz)
                cbp_idx = cur_time_local.hour + 1  # +1 for next hour
                cbp = self.cbp[cbp_idx]
                if cur_time_utc > end_utc:
                    cbp = 0

                # Calculate baseline target
                baseline_target = self.get_baseline_target(
                    cur_time_utc, start_utc, end_utc, cbp
                )

                # Package output
                if baseline_target is not None:
                    meta = {'type': 'float', 'tz': 'UTC', 'units': 'kW'}
                    time_meta = {'type': 'datetime', 'tz': 'UTC', 'units': 'datetime'}
                    target_info = [{
                        "id": format_timestamp(next_hour_utc),
                        "start": format_timestamp(next_hour_utc),
                        "end": format_timestamp(next_hour_end),
                        "target": baseline_target,
                        "cbp": cbp
                    }, {
                        "id": time_meta,
                        "start": time_meta,
                        "end": time_meta,
                        "target": meta,
                        "cbp": meta
                    }]
                _log.debug(
                    "TargetAgent: At time (UTC) {ts}"
                    " TargetInfo is {ti}".format(ts=cur_time_utc,
                                                 ti=target_info))
            else:
                _log.debug('TargetAgent: Not in event timeframe {start} {cur} {end}'.format(
                    start=start_utc_prev_hr,
                    cur=cur_time_utc,
                    end=end_utc_prev_hr
                ))
        return target_info

    def publish_target_info(self, cur_time_utc):
        cur_time_utc = parser.parse(cur_time_utc)

        target_msg = self.get_target_info(format_timestamp(cur_time_utc), 'UTC')
        if len(target_msg) > 0:
            headers = {'Date': format_timestamp(get_aware_utc_now())}
            target_topic = '/'.join(['analysis', 'target_agent', self.site, self.building, 'goal'])
            self.vip.pubsub.publish(
                'pubsub', target_topic, headers, target_msg).get(timeout=10)
            _log.debug("TargetAgent {topic}: {value}".format(
                topic=target_topic,
                value=target_msg))

        # Schedule next run at min 30 of next hour only if current min >= 30
        if not self.simulation:
            one_hour = timedelta(hours=1)
            cur_min = cur_time_utc.minute
            next_update_time = cur_time_utc.replace(minute=30,
                                                    second=0,
                                                    microsecond=0)
            if cur_min >= 30:
                next_update_time += one_hour
            self.core.schedule(
                next_update_time, self.publish_target_info,
                format_timestamp(next_update_time))

def main(argv=sys.argv):
    '''Main method called by the eggsecutable.'''
    try:
        utils.vip_main(TargetAgent)
    except Exception as e:
        _log.exception('unhandled exception')

if __name__ == '__main__':
    # Entry point for script
    sys.exit(main())
