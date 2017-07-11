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
from datetime import datetime, timedelta
import pytz
from dateutil import parser
import numpy as np
import scipy, scipy.stats, scipy.interpolate

from volttron.platform.vip.agent import Agent, Core, PubSub, RPC, compat
from volttron.platform.agent import utils
from volttron.platform.agent.utils import (get_aware_utc_now,
                                           format_timestamp)

__version__ = '1.0.0'

utils.setup_logging()
_log = logging.getLogger(__name__)

econ_code = {
    0: 'GREEN',
    0.0: 'GREEN',
    0.1: 'RED',
    1.1: 'RED',
    2.1: 'RED',
    3.2: 'GREY',
    10: 'GREEN',
    10.0: 'GREEN',
    11.1: 'RED',
    12.1: 'RED',
    13.2: 'GREY',
    20: 'GREEN',
    20.0: 'GREEN',
    21.1: 'RED',
    23.2: 'GREY',
    30: 'GREEN',
    30.0: 'GREEN',
    31.2: 'GREY',
    32.1: 'RED',
    33.1: 'RED',
    34.1: 'RED',
    35.2: 'GREY',
    40: 'GREEN',
    40.0: 'GREEN',
    41.2: 'GREY',
    42.1: 'RED',
    43.1: 'RED',
    44.2: 'GREY'
}

airside_code = {
    0: 'GREEN',
    0.0: 'GREEN',
    1.1: 'RED',
    2.2: 'GREY',
    10: 'GREEN',
    10.0: 'GREEN',
    11.1: 'RED',
    12.1: 'GREY',
    13.1: 'RED',
    14.1: 'RED',
    15.1: 'RED',
    16.2: 'GREY',
    20: 'GREEN',
    20.0: 'GREEN',
    21.1: 'RED',
    22.1: 'GREY',
    23.1: 'RED',
    24.1: 'RED',
    25.1: 'RED',
    26.2: 'GREY',
    30: 'GREEN',
    30.0: 'GREEN',
    31.1: 'RED',
    32.2: 'GREY',
    40: 'GREEN',
    40.0: 'GREEN',
    41.1: 'RED',
    42.1: 'GREY',
    43.1: 'RED',
    44.1: 'RED',
    45.1: 'RED',
    46.2: 'RED',
    50: 'GREEN',
    50.0: 'GREEN',
    51.1: 'RED',
    52.1: 'GREY',
    53.1: 'RED',
    54.1: 'RED',
    55.1: 'RED',
    56.2: 'RED',
    60: 'GREEN',
    60.0: 'GREEN',
    61.2: 'GREY',
    63.1: 'RED',
    64.2: 'GREY',
    70: 'GREEN',
    70.0: 'GREEN',
    71.1: 'RED',
    80: 'GREEN',
    80.0: 'GREEN',
    81.1: 'RED'
}

class AfddAggregationAgent(Agent):
    """
    Aggregate Afdd result
    """
    def __init__(self, config_path, **kwargs):
        super(AfddAggregationAgent, self).__init__(**kwargs)

        self.config = utils.load_config(config_path)

        # Building info
        self.site = self.config.get('campus')
        self.building = self.config.get('building')
        self.unit = self.config.get('unit')
        self.airside = self.config.get('Airside_RCx')
        self.airside = [x for x in self.airside if x and x.strip()]
        self.econ = self.config.get('Economizer_RCx')
        self.econ = [x for x in self.econ if x and x.strip()]

        # Local timezone
        self.tz = self.config.get('tz')
        self.local_tz = pytz.timezone(self.tz)

        self.min_num_points = self.config.get('min_num_points', 5)
        self.min_num_recs_daily = self.config.get('min_num_recs_daily', 3)
        self.min_num_recs_weekly = self.config.get('min_num_recs_weekly', 2*24)
        self.min_num_recs_monthly = self.config.get('min_num_recs_monthly', 7*24)
        self.p = self.config.get('p', 0.5)
        self.conf = self.config.get('confidence', 0.95)

        # Decide current time based on operation mode
        self.op_mode = self.config.get('op_mode')
        self.cur_time = self.config.get('cur_time')
        if self.op_mode == 'manual':
            try:
                self.cur_time = parser.parse(self.cur_time)
            except:
                raise "The operation mode is manual mode " \
                      "but could not parse current time"

        # Debug folder
        self.debug_folder = self.config.get('debug_folder') + '/'
        self.debug_folder = self.debug_folder.replace('//', '/')

    @Core.receiver('onstart')
    def onstart(self, sender, **kwargs):
        _log.debug('FddAggrAgent: OnStart ')
        cur_time_local = self.local_tz.localize(datetime.now())
        if self.op_mode == 'manual':
            cur_time_local = self.local_tz.localize(self.cur_time)

        self.pre_aggr(cur_time_local)

    def pre_aggr(self, cur_analysis_time):
        _log.debug("AfddAggrAgent: start aggregating result...")

        # Do aggregation at the beginning of day, week, month
        if cur_analysis_time.minute == 0: # start of hour
            if cur_analysis_time.hour == 0: # start of day
                self.daily_aggr(cur_analysis_time)
                if cur_analysis_time.weekday() == 0: # start of week (Monday)
                    self.weekly_aggr(cur_analysis_time)
                if cur_analysis_time.day == 1: # start of month
                    self.monthly_aggr(cur_analysis_time)

        # Schedule for next hour
        next_analysis_time = cur_analysis_time.replace(minute=0,
                                                       second=0,
                                                       microsecond=0)
        next_analysis_time += timedelta(hours=1)
        cur_analysis_time_utc = cur_analysis_time.astimezone(pytz.utc)
        next_run_time_utc = cur_analysis_time_utc + timedelta(hours=1)

        # Set next_run_time to 15s after current run if it is in the past
        if self.op_mode == 'manual':
            if next_run_time_utc < get_aware_utc_now():
                next_run_time_utc = get_aware_utc_now() + timedelta(seconds=60)

        self.core.schedule(next_run_time_utc, self.pre_aggr, next_analysis_time)

    def daily_aggr(self, cur_analysis_time):
        _log.debug("AfddAggrAgent: DAILY aggregating result...")
        cur_analysis_time_utc = cur_analysis_time.astimezone(pytz.utc)
        start_time_utc = cur_analysis_time_utc - timedelta(days=1)
        end_time_utc = cur_analysis_time_utc - timedelta(seconds=1)
        # Airside
        for algo in self.airside:
            err_code = self.aggregrate_by_app('Airside_RCx', algo,
                                              start_time_utc, end_time_utc, self.min_num_recs_daily)
            # Do something with result

        # Economizer
        for algo in self.econ:
            err_code = self.aggregrate_by_app('Economizer_RCx', algo,
                                              start_time_utc, end_time_utc, self.min_num_recs_daily)
            # Do something with result

    def weekly_aggr(self, cur_analysis_time):
        _log.debug("AfddAggrAgent: WEEKLY aggregating result...")
        cur_analysis_time_utc = cur_analysis_time.astimezone(pytz.utc)
        start_time_utc = cur_analysis_time_utc - timedelta(days=7)
        end_time_utc = cur_analysis_time_utc - timedelta(seconds=1)
        # Airside
        for algo in self.airside:
            err_code = self.aggregrate_by_app('Airside_RCx', algo,
                                              start_time_utc, end_time_utc, self.min_num_recs_weekly)
            # Do something with result

        # Economizer
        for algo in self.econ:
            err_code = self.aggregrate_by_app('Economizer_RCx', algo,
                                              start_time_utc, end_time_utc, self.min_num_recs_weekly)
            # Do something with result

    def monthly_aggr(self, cur_analysis_time):
        _log.debug("AfddAggrAgent: MONTHLY aggregating result...")
        cur_analysis_time_utc = cur_analysis_time.astimezone(pytz.utc)
        start_time = cur_analysis_time.replace(day=1,
                                               hour=0,
                                               minute=0,
                                               second=0,
                                               microsecond=0)
        start_time_utc = start_time.astimezone(pytz.utc)
        end_time_utc = cur_analysis_time_utc - timedelta(seconds=1)
        # Airside
        for algo in self.airside:
            err_code = self.aggregrate_by_app('Airside_RCx', algo,
                                              start_time_utc, end_time_utc, self.min_num_recs_monthly)
            # Do something with result

        # Economizer
        for algo in self.econ:
            err_code = self.aggregrate_by_app('Economizer_RCx', algo,
                                              start_time_utc, end_time_utc, self.min_num_recs_monthly)
            # Do something with result

    def aggregrate_by_app(self, app, algo, start_time_utc, end_time_utc, min_num_recs):
        err_code = 'GREY'
        topic = self.get_topic(app, algo)
        result = self.vip.rpc.call('platform.historian',
                                   'query',
                                   topic=topic,
                                   start=start_time_utc.isoformat(' '),
                                   end=end_time_utc.isoformat(' '),
                                   count=1000000).get(timeout=1000)
        _log.debug("AfddAggrAgent: query result length is {len}".format(len=len(result)))
        if len(result) > 0:
            err_code = self.aggregrate(result['values'], airside_code, min_num_recs)

        file = "{folder}{building}_{app}.csv".format(
            folder=self.debug_folder,
            building=self.building,
            app=app)
        row = "{algo},{start},{end},{err_code}\n".format(algo=algo,
                                                         start=start_time_utc,
                                                         end=end_time_utc,
                                                         err_code=err_code)
        self.expand_4_debug(file, row)

        return err_code

    def aggregrate(self, arr, err_codes, min_n):
        out_code = 'GREY'
        fault_arr = [x[0] for x in arr if err_codes[x[1]] == 'RED']
        no_fault_arr = [x[0] for x in arr if err_codes[x[1]] == 'GREEN']
        n = len(fault_arr) + len(no_fault_arr)

        if n < min_n:
            n = min_n

        x = scipy.linspace(0, n, n+1)
        xmf = scipy.stats.binom.pmf(x, n, self.p)

        if x[-1:] == 0:
            _log.debug("AfddAggrAgent: No Dx out")
        else:
            xnew = np.linspace(x.min(), x.max(), x.max()*2+1)
            #power_smooth = scipy.interpolate.spline(x, xmf, xnew)
            xmf_case = scipy.stats.binom.pmf(len(fault_arr), n, self.p)
            xmf_case_cdf = scipy.stats.binom.cdf(len(fault_arr), n, self.p)
            Interval = scipy.stats.binom.interval(self.conf, n, self.p, loc=0)
            threshold = n/2

            xmf_case1 = scipy.stats.binom.pmf(int(Interval[0]), n, self.p)
            xmf_case2 = scipy.stats.binom.pmf(int(Interval[1]), n, self.p)
            xmf_case3 = scipy.stats.binom.pmf(threshold, n, self.p)

            Prob_case1 = scipy.stats.binom.cdf(int(Interval[0]), n, self.p)
            Prob_case2 = scipy.stats.binom.cdf(int(Interval[0]), n, self.p)
            Prob_case3 = scipy.stats.binom.cdf(threshold, n, self.p)

            if n > self.min_num_points:
                if xmf_case1 <= xmf_case_cdf:
                    out_code = 'RED'
                else:
                    out_code = 'GREEN'
            else:
                out_code = 'GREY'

        return out_code

    def is_feature_supported(self, feature):
        if feature in self.supported_features:
            return True
        return False

    def get_topic(self, app, algo):
        topic_tmpl = "{app}/{campus}/{building}/{unit}/{algo}/diagnostic message"
        topic = topic_tmpl.format(app=app,
                                  campus=self.site,
                                  building=self.building,
                                  unit=self.unit,
                                  algo=algo)
        return topic

    def expand_4_debug(self, file_name, row):
        _log.debug('FDDAggr: expanding to {file}'.format(file=file_name))
        if self.debug_folder is not None:
            try:
                if os.path.isfile(file_name):
                    fd = open(file_name, 'a+')
                else:
                    fd = open(file_name, 'w+')
                    fd.write("Dx,Start,End,Value\n")
                fd.write(row)
                fd.close()
            except Exception as ex:
                _log.debug(ex)

def main(argv=sys.argv):
    '''Main method called by the eggsecutable.'''
    try:
        utils.vip_main(AfddAggregationAgent)
    except Exception as e:
        _log.exception('unhandled exception')

if __name__ == '__main__':
    # Entry point for script
    sys.exit(main())
