# Historical Solar Wind + Proton Flux Data

***

## Solar Wind Data

1-min precise solar wind data ranging from Jan 1st, 2000 12am -> Jan 1st, 2026 11:59pm (UTC?)

it comes from: https://omniweb.gsfc.nasa.gov/form/omni_min.html

Must be parsed, is currently an HTML file, but easy enough to turn into csv

KEEP IN MIND!!
Unfortunately, I accidentally curl-ed info from Jan 1 -> Jan 1 next year, instead of Jan 1 -> Dec 31. This means there is duplicate information for Jan 1st of 2001-2025. Ensure you remove these duplicates before feeding to ML trainer.

ts folder is 1gb so im not tryna rerun it to remove duplicates myself

## Proton Flux Data

1-min precise data for GOES-18 proton flux data ranging from Oct 1st, 2022 12am -> Mar 16th, 2025 11:59pm (UTC?)

it comes from: https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/goes18/l2/data/sgps-l2-avg1m/

Each file is a different day, each day contains 72k rows:

1440 minutes * 2 sensor units * (
    13 energy channels for differential proton
    11 alpha channels for differential alpha
    1 integral proton row
) = 72k rows

~~ts data was 5gb and i am actively curl-ing more data~~
its now 8.25gb, but i am done curl-ing more data

each file comes as a NetCFD file, script in here to parse into CSV