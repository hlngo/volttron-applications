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
from dateutil import parser
import pandas as pd
import statsmodels.formula.api as sm
from pandas.tseries.offsets import CustomBusinessDay, BDay
from pandas.tseries.holiday import USFederalHolidayCalendar
import pytz

from volttron.platform.vip.agent import Agent, Core, PubSub, RPC, compat
from volttron.platform.agent import utils
from volttron.platform.agent.utils import (get_aware_utc_now,
                                           format_timestamp)

utils.setup_logging()
_log = logging.getLogger(__name__)


class PGnEAgent(Agent):
    def __init__(self, config_path, **kwargs):
        super(PGnEAgent, self).__init__(**kwargs)
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
        #self.core.periodic(self.schedule_run_in_sec, self.calculate_latest_coeffs)

        # cur_time = local_tz.localize(datetime.now())
        # FOR TESTING
        # local_tz = pytz.timezone(self.tz)
        # cur_time = local_tz.localize(datetime(2016, 8, 17, 8, 0, 0))
        # self.calculate_latest_baseline(cur_time)
        pass

    @RPC.export('get_prediction')
    def get_prediction(self, in_time, in_tz):
        cur_time = parser.parse(in_time)
        if cur_time.tzinfo is None:
            tz = pytz.timezone(in_tz)
            cur_time = tz.localize(cur_time)

        return self.calculate_latest_baseline(cur_time)

    def calculate_latest_baseline(self, cur_time):
        baseline_values = []
        unit_topic_tmpl = "{campus}/{building}/{unit}/{point}"
        unit_points = [self.out_temp_name, self.power_name]
        df = None

        # Calculate start time as midnight of 10 business day before the cur date
        # Support USFederalHoliday only, add custom holidays later (state, regional, etc.)
        #local_tz = pytz.timezone(self.tz)
        #cur_time = local_tz.localize(datetime.now())
        #cur_time = local_tz.localize(datetime(2016, 8, 18, 8, 0, 0))
        bday_us = CustomBusinessDay(calendar=USFederalHolidayCalendar())
        start_time = cur_time - 10*bday_us
        start_time = start_time.replace(
            hour=0, minute=0, second=0, microsecond=0)
        start_date_utc = start_time.astimezone(pytz.utc)
        cur_time_utc = cur_time.astimezone(pytz.utc)
        one_hour = timedelta(hours=1)
        next_time_utc1 = \
            cur_time_utc.replace(minute=0, second=0, microsecond=0) + one_hour
        next_time_utc2 = next_time_utc1 + one_hour

        #Get data
        unit = self.temp_unit
        df_extension = {self.ts_name: [
            next_time_utc1.strftime('%Y-%m-%d %H:%M:%S'),
            next_time_utc2.strftime('%Y-%m-%d %H:%M:%S')
        ]}
        df_extension[self.ts_name] = pd.to_datetime(df_extension[self.ts_name])
        for point in unit_points:
            if point == self.power_name:
                unit = self.power_unit
            unit_topic = unit_topic_tmpl.format(campus=self.site,
                                                building=self.building,
                                                unit=unit,
                                                point=point)
            result = self.vip.rpc.call('platform.historian',
                                       'query',
                                       topic=unit_topic,
                                       start=start_date_utc.isoformat(' '),
                                       count=1000000,
                                       order="LAST_TO_FIRST").get(timeout=10000)
            if len(result)>0:
                df2 = pd.DataFrame(result['values'], columns=[self.ts_name, point])
                df2[self.ts_name] = pd.to_datetime(df2[self.ts_name])
                df2 = df2[df2[self.ts_name]<cur_time_utc]
                df2 = df2.groupby([pd.TimeGrouper(key=self.ts_name, freq=self.aggregate_freq)]).mean()
                #df2.resample('60min').mean()
                # df2[self.ts_name] = df2[self.ts_name].apply(lambda dt: dt.replace(second=0, microsecond=0))
                #df = df2 if df is None else pd.merge(df, df2, how='outer', left_index=True, right_index=True)
                df = df2 if df is None else pd.merge(df, df2, how='outer', left_index=True, right_index=True)
            if not point in df_extension:
                df_extension[point] = [-1.0]
            df_extension[point].append(-1.0)

        #Calculate coefficients
        result_df = None
        if df is not None:
            #Expand dataframe for 2hour prediction
            df_extension = pd.DataFrame(df_extension)
            df_extension = df_extension.set_index([self.ts_name])
            result_df = self.calculate_baseline_logic(df)

        if result_df is not None:
            #self.publish_baseline(result_df, cur_time_utc)
            last_idx = len(result_df.index) - 1
            sec_last_idx = last_idx - 1
            # use adj avg for 10 day
            value_hr2 = result_df['pow_adj_avg'][last_idx]
            value_hr1 = result_df['pow_adj_avg'][sec_last_idx]
            meta = {'type': 'float', 'tz': self.tz, 'units': 'kW'}
            baseline_values = [{
                "value_hr1": value_hr1,
                "value_hr2": value_hr2
            }, {
                "value_hr1": meta,
                "value_hr2": meta
            }]

        # Schedule the next run at minute 30 of next hour
        # one_hour = timedelta(hours=1)
        # next_update_time = \
        #     cur_time.replace(minute=20, second=0, microsecond=0) + one_hour
        # self._still_connected_event = self.core.schedule(
        #     next_update_time, self.calculate_latest_baseline)
        _log.debug("Baseline values: ")
        _log.debug(baseline_values)
        return baseline_values

    def calculate_baseline_logic(self, dP):
        # Not used anymore, it's here for information purpose
        dP['DayOfWeek'] = dP.index.map(lambda v: v.weekday())

        # Delete the weekend
        dP.columns = [
            "OutdoorAirTemperature",
            "WholeBuildingPower",
            "DayOfWeek"]
        dP['year'] = dP.index.year
        dP['month'] = dP.index.month
        dP['hour'] = dP.index.hour
        dP['day'] = dP.index.day
        dP = dP[dP.DayOfWeek<5] #Not sat or sun

        # if (len(dP.index) < self.min_no_of_records_needed_after_aggr):
        #     _log.exception('Not enough data to process')
        #     return None

        # Hourly average value
        df = dP.resample('60min').mean()

        #dP = dP[dP.OutdoorAirTemperature < 150]
        #dP = dP[dP.OutdoorAirTemperature > 20]
        #dP = dP.dropna()
        self.save_4_debug(df, 'data1.csv')

        df = df.pivot_table(index=["year", "month", "day"], columns=["hour"], values=["OutdoorAirTemperature","WholeBuildingPower"])

        df_length = len(df.index)
        if (df_length < 11): #10prev business day + current day
            _log.exception('Not enough data to process')
            return None

        # Average using high five outdoor temperature data based on 10 day moving windows
        for i in range(0, df_length):
            for j in range(0, 24):
                df['hot5_pow_avg', j] = None
        for i in range(10, df_length):
            for j in range(0, 24):
                df['hot5_pow_avg', j][i] = \
                    df.ix[i-10:i-1, :].sort([('OutdoorAirTemperature', j)], ascending=False).head(5).ix[:, (j+24):(j+25)].mean().ix[0]
        self.save_4_debug(df, 'data2.csv')

        # Average based on 10 day moving windows
        for i in range(0, 24):
            df['Tout_avg', i] = df.ix[:, i:i + 1].rolling(window=10, min_periods=10).mean().shift()

        for i in range(0, 24):
            df['pow_avg', i] = df.ix[:, i + 24:i + 25].rolling(window=10, min_periods=10).mean().shift()

        self.save_4_debug(df, 'data3.csv')

        df = df.stack(level=['hour'])

        #df.hot5_pow_avg = df.hot5_pow_avg.shift(216)
        df = df.dropna()
        dq = df.reset_index()
        dq['Data'] = pd.to_datetime(
            dq.year.astype(int).apply(str) + '/' + dq.month.astype(int).apply(str) + '/' + dq.day.astype(int).apply(
                str) + ' ' + dq.hour.astype(int).apply(str) + ":00", format='%Y/%m/%d %H:%M')
        dq = dq.set_index(['Data'])
        dq = dq.drop(['year', 'month', 'day', 'hour'], axis=1)
        self.save_4_debug(dq, 'data4.csv')
        # Adjusted average based on 10 day moving windows
        dq_length = len(dq.index)
        dq_length = dq_length - 4
        dq["Adj"] = 1.0
        for i in range(0, dq_length):
            dq['Adj'][i + 4] = (dq['WholeBuildingPower'][i:i+3].mean()) / (dq['pow_avg'][i:i + 3].mean())

        dq['pow_adj_avg'] = dq['pow_avg'] * dq['Adj']
        self.save_4_debug(dq, 'data5.csv')

        # Adjusted average using high five outdoor temperature data based on 10 day moving windows
        dq_length = len(dq.index) - 4
        dq["Adj2"] = 1.0
        for i in range(0, dq_length):
            dq['Adj2'][i + 4] = (dq['WholeBuildingPower'][i:i+3].mean()) / (dq['hot5_pow_avg'][i:i + 3].mean())
        self.save_4_debug(dq, 'data6.csv')

        dq['Adj2'] = dq.Adj2.shift(2)
        dq['hot5_pow_adj_avg'] = dq['hot5_pow_avg'] * dq['Adj2']
        self.save_4_debug(dq, 'result.csv')

        return dq

    def publish_baseline(self, df, cur_time):
        topic_tmpl = "analysis/PGnE/{campus}/{building}/"
        topic_prefix = topic_tmpl.format(campus=self.site,
                                         building=self.building)
        headers = {'Date': format_timestamp(cur_time)}
        last_idx = len(df.index)-1
        sec_last_idx = last_idx - 1
        #avg 10 day
        topic1 = topic_prefix + "avg10"
        value1 = df['pow_avg'][last_idx]
        #adj avg 10 day
        topic2 = topic_prefix + "adj_avg10"
        value_hr1 = df['pow_adj_avg'][sec_last_idx]
        value_hr2 = df['pow_adj_avg'][last_idx]
        #avg 5 hottest in 10 day
        topic3 = topic_prefix + "hot5_avg10"
        value3 = df['hot5_pow_avg'][last_idx]
        #adj avg 5 hottest in 10 day
        topic4 = topic_prefix + "hot5_adj_avg10"
        value4 = df['hot5_pow_adj_avg'][last_idx]

        #publish to message bus: only 10 day adjustment
        meta = {'type': 'float', 'tz': self.tz, 'units': 'kW'}
        msg = [{
            "value_hr1": value_hr1,
            "value_hr2": value_hr2
        }, {
            "value_hr1": meta,
            "value_hr2": meta
        }]
        self.vip.pubsub.publish(
            'pubsub', topic2, headers, msg).get(timeout=10)
        _log.debug("{topic}: {value}".format(
            topic=topic2, value=msg))
        #publish to message bus
        # topics = [topic1,topic2,topic3,topic4]
        # values = [value1,value2,value3,value4]
        # meta = {'type': 'float', 'tz': self.tz, 'units': 'kW'}
        # for i in xrange(0,4):
        #     message = [{"value":values[i]}, meta]
        #     self.vip.pubsub.publish(
        #         'pubsub', topics[i], headers, message).get(timeout=10)
        #     _log.debug("{topic}: {value}".format(
        #         topic=topics[i],
        #         value=values[i]))

    def save_4_debug(self, df, name):
        if self.debug_folder != None:
            try:
                df.to_csv(self.debug_folder + name)
            except Exception as ex:
                _log.debug(ex)

def main(argv=sys.argv):
    '''Main method called by the eggsecutable.'''
    try:
        utils.vip_main(PGnEAgent, 'baseline_agent')
    except Exception as e:
        _log.exception('unhandled exception')

if __name__ == '__main__':
    # Entry point for script
    sys.exit(main())
    #test_api()
