from __future__ import annotations
from datetime import datetime


class RequestManager:

    def __init__(self, params):
        self.params = params

    def get_new_requests(self, df_req: type[DataFrame]) -> type[DataFrame]:

        exec_date = datetime.strptime(self.params.get_date_to(), '%Y-%m-%d')

        # Extract the last 20 request rows
        df_tmp = df_req.tail(20)
        df_tmp['datetime_req'] = df_tmp['Timestamp'].map(lambda x: datetime.strptime(x, '%Y-%m-%d'))
        new_req = df_tmp[df_tmp['datetime_req'] == exec_date]

        return new_req
