from __future__ import annotations
from datetime import datetime, timedelta


class RequestManager:

    def __init__(self, params):
        self.params = params

    def get_new_requests(self, df_req: type[DataFrame]) -> type[DataFrame]:

        exec_ts = datetime.strptime(self.params.get_date_to(), '%Y-%m-%d')
        last_ts = exec_ts - timedelta(days=1)

        # Extract the last 20 request rows
        df_tmp = df_req.tail(20)
        df_tmp['datetime_req'] = df_tmp['Timestamp'].map(lambda x: datetime.strptime(x, '%m/%d/%Y %H:%M:%S'))
        new_req = df_tmp[(last_ts < df_tmp['datetime_req']) & (df_tmp['datetime_req'] < exec_ts)]

        return new_req
