# twstock-chips

Temporary downloader for FinMind free chip data (margin, foreign shareholding, institutional buy/sell) used by a private research project.
12 parallel GitHub jobs, no token, each job stops when its IP quota is used up; a new round starts every 10 minutes.
Output: `data/<kind>/<stock_id>.csv.gz`. Data source: FinMind (https://finmindtrade.com/). Delete this repository when the download is done.
