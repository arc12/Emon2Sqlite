# Query an EmonCMS and build a local Sqlite3 Database of feed data.
# This will either create afresh or update the local DB.
# Various components used to this aim are also exposed.
# Adapted from work by Trystan Lee: https://github.com/emoncms/usefulscripts/tree/master/backup_py


import requests
from os import path
import  json
import struct
import logging
import sqlite3

logging.basicConfig(level=logging.INFO)

class Emon2Sqlite:
    def __init__(self, emon_cms_root, emon_cms_username, emon_cms_password, local_db_path=None):
        self.emon_cms_root = emon_cms_root
        self.username = emon_cms_username
        self.password = emon_cms_password
        self.db_name = "emon.db"
        self.local_db_path = "." if local_db_path is None else local_db_path

        self.apikey = None  # authenticate() to fill me

        # a dict of items, keyed against the feed id like:
        #{'engine': '5', 'id': '1', 'name': 'power1', 'processList': '', 'public': '0', 'size': '34142424', 'tag': 'emonpi', 'time': 1741461865, 'unit': '', 'userid': '1', 'value': 948}
        self.feeds = dict()

        # Get existing table names for easier reporting
        db_conn = sqlite3.connect(path.join(self.local_db_path, self.db_name))
        cursor = db_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        self.tables = [r[0] for r in cursor.fetchall()]
        cursor.close()
        db_conn.close()

    def authenticate(self):
        """
        Authenticate against the EmonCMS.
        :return:
        """
        try:
            result = requests.post(f"{self.emon_cms_root}/user/auth.json", data={'username': self.username, 'password': self.password})
            user_auth = json.loads(result.text)
            self.apikey = user_auth['apikey_read']
        except Exception as ex:
            logging.error(f"Failed to authenticate to EmonCMS at {self.emon_cms_root}: got {result}.", exc_info=ex)
            return False

        return True

    def get_feeds(self):
        if self.apikey is None:
            if not self.authenticate():
                return dict()

        result = requests.get(f"{self.emon_cms_root}/feed/list.json", params={'apikey': self.apikey})
        feed_list = json.loads(result.text)

        self.feeds = {f["id"]: f for f in feed_list}

        return self.feeds

    def make_db_names(self, feed_id):
        """

        :param feed_id:
        :return: tuple of DB table name and data column name
        """
        tag, name = self.feeds[feed_id]["tag"], self.feeds[feed_id]["name"]
        table_name = name if (len(tag) == 0  or name.startswith(tag)) else f"{tag}_{name}"
        units = self.feeds[feed_id]["unit"]
        return table_name, units if len(units) > 0 else "value"

    def sync_feeds(self, ignore_feed_ids=()):
        ignore_feed_ids = [str(fid) for fid in ignore_feed_ids]  # allow use of integers
        if len(self.feeds) == 0:
            self.get_feeds()

        for feed_id in list(self.feeds):
            if feed_id not in ignore_feed_ids:
                self.sync_feed(feed_id)

    def sync_feed(self, feed_id):
        """
        Syncs one feed from EmonCMS to SQLite
        :param feed_id:
        :return: number of rows added to DB
        """
        if not isinstance(feed_id, str):
            feed_id = str(feed_id)
        if len(self.feeds) == 0:
            self.get_feeds()

        n_inserted = 0

        feed = self.feeds[feed_id]
        logging.info(f"Working on feed: {feed}")
        if feed["engine"] == "5":
            n_inserted = self._get_fina(feed_id)
        # elif feed["engine"] == "2":
        #     self.get_timeseries
        else:
            logging.warning(f"Only phpfina engine (5) supported; found {feed['engine']} for feed id {feed_id}")

        return n_inserted

    def _get_fina(self, feed_id):
        """
        Gets data update from EmonCMS and inserts into the database.
        :param feed_id:
        :return: number of records inserted. May be 0 if an error occurred.
        """
        if self.apikey is None:
            if not self.authenticate():
                return 0

        # we will need the "meta" file to work out timestamps to match the sequence of data values
        response = requests.get(f"{self.emon_cms_root}/feed/getmeta.json", params={'id': feed_id, 'apikey': self.apikey})
        meta = json.loads(response.text)  # key facts are against keys "start_time", "end_time", "interval", and "npoints"
        if meta["npoints"] == 0:
            return 0

        logging.info(meta)

        # prepare the url to get only new data
        max_db_timestamp = self.check_table(feed_id)
        if max_db_timestamp == 0:
            download_start = 0
            chunk_start_time = meta["start_time"]
        else:
            download_start = 4 * (1 + (max_db_timestamp - meta["start_time"]) // meta["interval"])
            chunk_start_time = max_db_timestamp + meta["interval"]
        url = f"{self.emon_cms_root}/feed/export.json?id={feed_id}&start={download_start}&apikey={self.apikey}"
        logging.info(url)

        # request chunks of data, processing each as a separate transaction. NB the chunk size must be a multiple of 4 bytes to align with phpfina storage
        table_name, col_name = self.make_db_names(feed_id)
        batch_size = 2048  # number of 4 byte data values to get per iteration of chunked data download
        n_inserted = 0
        with sqlite3.connect(path.join(self.local_db_path, self.db_name)) as db_conn:
            cursor = db_conn.cursor()
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                try:
                    for chunk in r.iter_content(chunk_size=batch_size * 4):
                        values_in_chunk = len(chunk) // 4
                        timestamps = [chunk_start_time + meta["interval"] * i for i in range(0, values_in_chunk)]
                        chunk_start_time = timestamps[-1] + meta["interval"]  # for next iteration
                        values = [struct.unpack("f", chunk[i * 4: (i+1) * 4])[0] for i in range(0, values_in_chunk)]
                        cursor.executemany(f"INSERT INTO {table_name} (timestamp, {col_name}) VALUES (?, ?)", zip(timestamps, values))
                        this_inserted = cursor.rowcount
                        n_inserted += this_inserted
                        db_conn.commit()
                        logging.info(f"Inserted {this_inserted} rows to {timestamps[-1]} in {table_name}.")

                except Exception as ex:
                    logging.error(f"Aborted working on feed id: {feed_id} due to exception.", exc_info=ex)

        db_conn.close()

        return n_inserted

    def check_table(self, feed_id):
        """
        Creates table for feed if required, returning the last timestamp stored, or 0 if newly created
        :param feed_id:
        :return:
        """
        table_name, col_name = self.make_db_names(feed_id)
        if table_name not in self.tables:
            logging.info(f"Creating new table '{table_name} for feed id {feed_id}")
            with sqlite3.connect(path.join(self.local_db_path, self.db_name)) as db_conn:
                cursor = db_conn.cursor()
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (timestamp INTEGER NOT NULL, {col_name} NUMERIC)")
                cursor.execute(f'CREATE INDEX IF NOT EXISTS "{table_name}_timestamp" ON "{table_name}" ("timestamp")')
                cursor.close()
            db_conn.close()
            return 0
        with sqlite3.connect(path.join(self.local_db_path, self.db_name)) as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(f"SELECT MAX(timestamp) FROM {table_name}")
            max_ts = cursor.fetchone()
            cursor.close()
        db_conn.close()

        return max_ts[0] if max_ts[0] is not None else 0
