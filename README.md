# Emon2Sqlite
This is a utility Python class to sync data from an EmonCMS instance with a SQLite database. It was written to make it easier to perform ad hoc data analysis in Jupyter Notebooks than is possible with the phpfina storage used as default by EmonCMS.

Notes:  
- Currently limited to processing phpfina data.
- The SQLite database file will be MUCH larger than the phpfina.
- For browsing the data and ad hoc queries, I find this multi-OS application useful: https://sqlitebrowser.org/ .
- See the comments in emon2sqlite.py for more info.
- Only tested against a local EmonPi.

## Usage
Basic usage to fetch all EmonCMS feeds would be achieved with a Python script like:
```
from emon2sqlite import Emon2Sqlite

e2s = Emon2Sqlite("url", "username", "password")  # home page, username, and password for your EmonCMS
e2s.sync_feeds()  # will create a new database and tables as required
```

Get info on the available feeds:
```
from emon2sqlite import Emon2Sqlite

e2s = Emon2Sqlite("url", "username", "password")

e2s.get_feeds()
for id, feed in e2s.feeds.items():
	print(id, feed)
```

Sync one feed:
```
from emon2sqlite import Emon2Sqlite

e2s = Emon2Sqlite("url", "username", "password")
e2s.sync_feed(26)  # can use numeric or string values for parameter
```

Ignore some feeds (e.g. avoid syncing the accumulated kWh):
```
from emon2sqlite import Emon2Sqlite

e2s = Emon2Sqlite("url", "username", "password")
e2s.sync_feeds(ignore_feed_ids=(2, 3, 8))
```