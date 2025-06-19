## Running ETL Scripts


Example to initiate

`uv run cpc-chirps.py instantiate cpc-precip-conus`


## Before Appending

`uv run cpc-chirps.py get-available-timespan cpc-tmax`

This will give you the available years

`1979-01-01 2025-06-15`

Using this you can then append based on those options. If you initiated 1979 to 1980. They you will want 2025-1981

count = (2025 - 1981) + 1 = 45

This will be the count

Example to Append 

`uv run cpc-chirps.py append cpc-precip-conus bafyr4id3pkettq3abnieftxxy3m6fkrq262uuc4nbicoeycjkzitdlrtaq --year --count 45`


# ERA 5

init

`uv run era5.py instantiate 2m_temperature --api-key <api-key>`

append

MONTH DOES NOT WORK FOR ERA5

`uv run era5.py append 2m_temperature bafyr4ieyjzlcrzlxz2274k5223ao3xqdr66tgy2g4tykcamhmf73v3fnse day --api-key <api-key>`


You will want to stride it and count it
`uv run era5.py append 2m_temperature bafyr4icu5l2v6p5ow7xymkyn3vefrltlnopouf5jrdkeny4oe2rztvc2pe day --count 28 --stride 28`
