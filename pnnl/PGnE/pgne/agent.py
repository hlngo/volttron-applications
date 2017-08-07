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
from dateutil import parser
import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay, BDay
from pandas.tseries.holiday import USFederalHolidayCalendar
import pytz
import numpy as np

from volttron.platform.vip.agent import Agent, Core, PubSub, RPC, compat
from volttron.platform.agent import utils
from volttron.platform.agent.utils import (get_aware_utc_now,
                                           format_timestamp)

__version__ = '1.0.0'

utils.setup_logging()
_log = logging.getLogger(__name__)


class PGnEAgent(Agent):
    def __init__(self, config_path, **kwargs):
        super(PGnEAgent, self).__init__(**kwargs)
        self.config = utils.load_config(config_path)
        self.site = self.config.get('campus')
        self.building = self.config.get('building')
        self.out_temp_unit = self.config.get('out_temp_unit')
        self.out_temp_name = self.config.get('out_temp_name')
        self.power_unit = self.config.get('power_unit')
        self.power_name = self.config.get('power_name')
        self.aggregate_in_min = self.config.get('aggregate_in_min')
        self.aggregate_freq = str(self.aggregate_in_min) + 'Min'
        self.ts_name = self.config.get('ts_name')
        self.calc_mode = self.config.get('calculation_mode', 0)
        self.manual_set_adj_value = self.config.get('manual_set_adj_value', False)
        self.fix_adj_value = self.config.get('fix_adj_value', 1.26)

        self.tz = self.config.get('tz')
        self.local_tz = pytz.timezone(self.tz)
        self.one_day = timedelta(days=1)

        self.min_adj = 1
        self.max_adj = 1.4

        #Debug
        self.debug_folder = self.config.get('debug_folder') + '/'
        self.debug_folder = self.debug_folder.replace('//', '/')

        #
        self.bday_us = CustomBusinessDay(calendar=USFederalHolidayCalendar())

    @Core.receiver('onstart')
    def onstart(self, sender, **kwargs):
        #self.core.periodic(self.schedule_run_in_sec, self.calculate_latest_coeffs)

        # cur_time = local_tz.localize(datetime.now())
        # FOR TESTING
        #local_tz = pytz.timezone(self.tz)
        #cur_time = local_tz.localize(datetime(2016, 8, 16, 8, 0, 0))
        #return_values = self.calculate_latest_baseline(cur_time)
        pass

    @RPC.export('get_prediction')
    def get_prediction(self, in_time, in_start, in_end, in_tz, exclude_days):
        _log.debug("PGnEAgent: RPC get_prediction is called...")
        cur_time = parser.parse(in_time)
        event_start = parser.parse(in_start)
        event_end = parser.parse(in_end)
        tz = pytz.timezone(in_tz)
        if cur_time.tzinfo is None:
            cur_time = tz.localize(cur_time)
        if event_start.tzinfo is None:
            event_start = tz.localize(event_start)
        if event_end.tzinfo is None:
            event_end = tz.localize(event_end)
        # Convert to UTC before doing anything else
        cur_time_utc = cur_time.astimezone(pytz.utc)
        #event_start_utc = event_start.astimezone(pytz.utc)
        #event_end_utc = event_end.astimezone(pytz.utc)
        event_start_local = event_start.astimezone(self.local_tz)
        event_end_local = event_end.astimezone(self.local_tz)
        return self.calculate_latest_baseline(
            cur_time_utc, event_start_local, event_end_local, exclude_days)

    def calculate_latest_baseline(self, cur_time_utc,
                                  event_start_local, event_end_local,
                                  exclude_days_utc):
        baseline_values = []
        unit_topic_tmpl = "{campus}/{building}/{unit}/{point}"
        unit_points = [self.power_name]
        if self.calc_mode == 1:
            unit_points = [self.out_temp_name, self.power_name]
        df = None

        # Calculate start time as midnight of 10 business day before the cur date
        # Use 15 days for safety of not having enough data after filtering out <0 and nan values
        # Support USFederalHoliday only, add custom holidays later (state, regional, etc.)
        cur_time_local = cur_time_utc.astimezone(self.local_tz)
        start_time_local = cur_time_local - 11*self.bday_us
        start_time_local = start_time_local.replace(
            hour=0, minute=0, second=0, microsecond=0)

        # Exclude DR event days from calculation
        cont = True
        parsed_exclude_days = []
        parsed_exclude_days_utc = []
        for day in exclude_days_utc:
            parsed_day_utc = parser.parse(day)
            parsed_day_local = parsed_day_utc.astimezone(self.local_tz)
            parsed_exclude_days.append(parsed_day_local)
            parsed_exclude_days_utc.append(parsed_day_utc)
        while cont:
            cont = False
            for day in parsed_exclude_days:
                if day >= start_time_local:
                    cont = True
                    start_time_local -= 1*self.bday_us
                    parsed_exclude_days.remove(day)
                    break

        start_time_utc = start_time_local.astimezone(pytz.utc)
        one_hour = timedelta(hours=1)
        next_time_utc1 = \
            cur_time_utc.replace(minute=0, second=0, microsecond=0) + one_hour
        next_time_utc2 = next_time_utc1 + one_hour

        #Get data
        unit = self.out_temp_unit
        df_extension = {self.ts_name: [
            next_time_utc1.strftime('%Y-%m-%d %H:%M:%S'),
            next_time_utc2.strftime('%Y-%m-%d %H:%M:%S')
        ]}
        df_extension[self.ts_name] = pd.to_datetime(df_extension[self.ts_name])
        _log.debug("PGnEAgent: start querying data...")
        for point in unit_points:
            if point == self.power_name:
                unit = self.power_unit
            unit_topic = unit_topic_tmpl.format(campus=self.site,
                                                building=self.building,
                                                unit=unit,
                                                point=point)
            # Remove empty unit or building
            unit_topic = unit_topic.replace("//", "/")

            # Query data
            result = self.vip.rpc.call('platform.historian',
                                       'query',
                                       topic=unit_topic,
                                       start=start_time_utc.isoformat(' '),
                                       count=1000000,
                                       order="LAST_TO_FIRST").get(timeout=1000)
            _log.debug("PgneAgent: dataframe length is {len}".format(len=len(result)))
            if len(result) > 0:
                df2 = pd.DataFrame(result['values'], columns=[self.ts_name, point])
                df2[self.ts_name] = pd.to_datetime(df2[self.ts_name], utc=True)
                # Filter days for simulation purpose
                df2 = df2[df2[self.ts_name] < cur_time_utc]
                # Exclude dr days
                for exclude_day_utc in parsed_exclude_days_utc:
                    start = exclude_day_utc
                    end = exclude_day_utc + self.one_day
                    df2 = df2[(df2[self.ts_name] < start) | (df2[self.ts_name] >= end)]

                df2[point] = pd.to_numeric(df2[point])
                df2 = df2.groupby([pd.TimeGrouper(key=self.ts_name, freq=self.aggregate_freq)]).mean()
                if df is None:
                    df = df2
                else:
                    df = pd.merge(df, df2, how='outer', left_index=True, right_index=True)

            if point not in df_extension:
                df_extension[point] = [99999]
            df_extension[point].append(99999)

        #Calculate coefficients
        result_df = None
        if df is not None and not df.empty:
            # Expand dataframe for 2hour prediction
            df_extension = pd.DataFrame(df_extension)
            df_extension = df_extension.set_index([self.ts_name])
            df_extension = df.append(df_extension)

            # Convert to local timezone so the day exclusion doesn't create holes in data
            df_extension = df_extension.tz_localize(pytz.utc).tz_convert(self.local_tz)

            result_df = self.calculate_baseline_logic(
                df_extension, event_start_local, event_end_local)
        else:
            _log.debug("PgneAgent: df is None")

        if result_df is not None:
            #self.publish_baseline(result_df, cur_time_utc)
            last_idx = len(result_df.index) - 1
            sec_last_idx = last_idx - 1

            # use adj avg for 10 day
            # value_hr1 hr2 ex: 13:00 14:00 respectively
            _log.debug("PGnEAgent: Building baseline values message")
            value_hr0 = result_df['pow_adj_avg'][last_idx-2]
            value_hr1 = result_df['pow_adj_avg'][last_idx-1]
            value_hr2 = result_df['pow_adj_avg'][last_idx]
            if self.calc_mode == 1:
                value_hr0 = result_df['hot5_pow_adj_avg'][last_idx-2]
                value_hr1 = result_df['hot5_pow_adj_avg'][last_idx-1]
                value_hr2 = result_df['hot5_pow_adj_avg'][last_idx]
            meta = {'type': 'float', 'tz': self.tz, 'units': 'kW'}
            baseline_values = [{
                "value_hr0": value_hr0, #next hour
                "value_hr1": value_hr1, #next hour + 1
                "value_hr2": value_hr2  #next hour + 2
            }, {
                "value_hr0": meta,
                "value_hr1": meta,
                "value_hr2": meta
            }]
        else:
            _log.debug("PGnEAgent: result_df is None")

        _log.debug("PGnEAgent: Sending baseline values...")

        return baseline_values

    def map_day(self, d):
        np_datetime = np.datetime64("{:02d}-{:02d}-{:02d}".format(d.year, d.month, d.day))
        if np_datetime in self.bday_us.holidays:
            return 8
        return d.weekday()

    def calculate_baseline_logic(self, dP, event_start_local, event_end_local):
        # Not used anymore, it's here for information purpose
        #dP['DayOfWeek'] = dP.index.map(lambda v: v.weekday())
        dP['DayOfWeek'] = dP.index.map(lambda v: self.map_day(v))

        # Delete the non business days
        base_cols = [self.power_name]
        if self.calc_mode == 1:
            base_cols = [self.out_temp_name, self.power_name]
        base_cols.append("DayOfWeek")
        dP.columns = base_cols

        dP['year'] = dP.index.year
        dP['month'] = dP.index.month
        dP['hour'] = dP.index.hour
        dP['day'] = dP.index.day
        dP = dP[dP.DayOfWeek < 5] #Not sat/sun/holiday

        # Hourly average value
        self.save_4_debug(dP, 'data0.csv')
        dP = dP[dP[self.power_name] > 0].dropna()
        df = dP.resample('60min').mean()
        df = df.dropna()
        self.save_4_debug(df, 'data1.csv')

        _log.debug("PgneAgent: Creating pivot table")
        df = df.pivot_table(
            index=["year", "month", "day"], columns=["hour"],
            values=base_cols)
        _log.debug("PgneAgent: Created pivot table")

        df_length = len(df.index)
        if (df_length < 11): #10prev business day + current day
            _log.exception('PgneAgent: Not enough data to process')
            return None

        # Average using high five outdoor temperature data based on 10 day moving windows
        if self.calc_mode == 1:
            for i in range(0, df_length):
                for j in range(0, 24):
                    df['hot5_pow_avg', j] = None
            for i in range(10, df_length):
                for j in range(0, 24):
                    df['hot5_pow_avg', j][i] = \
                        df.ix[i-10:i-1, :].sort([(self.out_temp_name, j)], ascending=False).head(5).ix[:, (j+24):(j+25)].mean().ix[0]
            self.save_4_debug(df, 'data2h5.csv')

            # Average based on 10 day moving windows
            for i in range(0, 24):
                df['Tout_avg', i] = df.ix[:, i:i + 1].rolling(window=10, min_periods=10).mean().shift()

            self.save_4_debug(df, 'data2.csv')
            for i in range(0, 24):
                df['pow_avg', i] = df.ix[:, i + 24:i + 25].rolling(window=10, min_periods=10).mean().shift()
                #df['pow_avg', i] = df.ix[:, i:i + 1].rolling(window=10, min_periods=10).mean().shift()
        else:
            for i in range(0, 24):
                df['pow_avg', i] = df.ix[:, i:i + 1].rolling(window=10, min_periods=9).mean().shift()

        self.save_4_debug(df, 'data3.csv')

        df = df.stack(level=['hour'])
        df = df.dropna()
        dq = df.reset_index()
        try:
            dq[self.ts_name] = pd.to_datetime(
                dq.year.astype(int).apply(str) + '/' + dq.month.astype(int).apply(str) + '/' + dq.day.astype(int).apply(
                    str) + ' ' + dq.hour.astype(int).apply(str) + ":00", format='%Y/%m/%d %H:%M')
        except:
            _log.error("PGnEAgent: Hole in data. Please check data3.csv for info. Stop Calculation.")
            return None

        dq = dq.set_index([self.ts_name])
        dq = dq.drop(['year', 'month', 'day', 'hour'], axis=1)
        self.save_4_debug(dq, 'data4.csv')
        # Adjusted average based on 10 day moving windows
        dq_length = len(dq.index) - 4
        dq["Adj"] = 1.0
        for i in range(0, dq_length):
            dq['Adj'][i + 4] = (dq[self.power_name][i:i+3].mean()) / (dq['pow_avg'][i:i + 3].mean())

        self.save_4_debug(dq, 'data5.csv')
        dq.loc[dq['Adj'] < self.min_adj, 'Adj'] = self.min_adj
        dq.loc[dq['Adj'] > self.max_adj, 'Adj'] = self.max_adj

        dq = dq.sort_index()
        #Filter out all data greater than event start time
        #naive_ev_start_local = event_start_local.replace(tzinfo=None)
        #if len(dq[dq.index >= naive_ev_start_local]) > 0:
        #    dq['Adj'] = dq[dq.index >= naive_ev_start_local]['Adj'][0]

        #Need to change later for simulation
        #cur_time = self.local_tz.localize(datetime.now())
        #cur_time = cur_time.replace(hour=11, minute=59, second=0)
        #Aug 4 fix
        # dq.loc[(dq.index.hour < 12) & (dq.index.day == 4), 'Adj'] = 1.12
        # dq.loc[(dq.index.hour >= 12) & (dq.index.day == 4), 'Adj'] = \
        #     dq.loc[dq.index.hour == 12, 'Adj'][0]
        #Aug 7 test
        if self.manual_set_adj_value:
            dq['Adj'] = self.fix_adj_value



        dq['pow_adj_avg'] = dq['pow_avg'] * dq['Adj']
        self.save_4_debug(dq, 'data5a.csv')

        df_24 = dq[-24:]
        df_24 = df_24.tz_localize(self.local_tz).tz_convert(pytz.utc)
        value = df_24['pow_adj_avg'].to_json()
        value = json.loads(value)
        meta2 = {'type': 'string', 'tz': 'UTC', 'units': ''}
        baseline_msg = [{
            "value": value
        }, {
            "value": meta2
        }]
        headers = {'Date': format_timestamp(get_aware_utc_now())}
        target_topic = '/'.join(['analysis', 'PGnE', self.site, self.building, 'baseline'])
        self.vip.pubsub.publish(
            'pubsub', target_topic, headers, baseline_msg).get(timeout=10)
        _log.debug("PGnE {topic}: {value}".format(
            topic=target_topic,
            value=baseline_msg))

        # Adjusted average using high five outdoor temperature data based on 10 day moving windows
        if self.calc_mode == 1:
            dq_length = len(dq.index) - 4
            dq["Adj2"] = 1.0
            for i in range(0, dq_length):
                dq['Adj2'][i + 4] = (dq[self.power_name][i:i+3].mean()) / (dq['hot5_pow_avg'][i:i + 3].mean())
            self.save_4_debug(dq, 'data6.csv')
            dq['Adj2'] = dq.Adj2.shift(2)
            dq.loc[dq['Adj2'] < self.min_adj, 'Adj2'] = self.min_adj
            dq.loc[dq['Adj2'] > self.max_adj, 'Adj2'] = self.max_adj
            self.save_4_debug(dq, 'data7.csv')
            dq['Adj2'] = dq[dq.index >= event_start_local]['Adj2'][0]
            self.save_4_debug(dq, 'data8.csv')
            dq['hot5_pow_adj_avg'] = dq['hot5_pow_avg'] * dq['Adj2']
            self.save_4_debug(dq, 'data9.csv')

        _log.debug("PGnEAgent: Finish calculate baseline")
        return dq

    def publish_baseline(self, df, cur_time):
        """
        This method is obsolete. Keep here for reference only.
        """
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
        # _log.debug("{topic}: {value}".format(
        #     topic=topic2, value=msg))
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
        _log.debug('PgneAgent: saving to {file}'.format(file=name))
        if self.debug_folder is not None:
            try:
                df.to_csv(self.debug_folder + name)
            except Exception as ex:
                _log.debug(ex)

def main(argv=sys.argv):
    '''Main method called by the eggsecutable.'''
    try:
        utils.vip_main(PGnEAgent)
    except Exception as e:
        _log.exception('unhandled exception')

if __name__ == '__main__':
    # Entry point for script
    sys.exit(main())
