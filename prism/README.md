For more info, always see the PRISM website: https://prism.oregonstate.edu/

# Data Variables
We are ETLing 3 data variables from PRISM, precipitation (precip), and maximum and minimum temperature (tmax,tmin). We are concnerned with the daily data, not monthly or yearly summaries. All 3 of these data variables only cover the continental US.

Each of these is available in two resolutions, referring to the length of the grid squares: 800 meters and 4 km. 4km is available publicly while 800 m is available at cost. We are only ETLing the 4km versions, which is why the dataset folders are suffixed with the resolution.

# Fetching
PRISM provides both FTP and HTTPS services for retrieving their data.

The FTP service is unencrypted and provides data in the `bil` format.  You can browse the FTP directories using your browser here: https://ftp.prism.oregonstate.edu/

The HTTPS service allows for downloading in both bil and netCDF. This is why we chose the HTTPS service, as it removes a layer of complexity of converting from the bil format, as well as allowing for an encrypted and secure file transfer.
You can see more information about the HTTPS service here: https://prism.oregonstate.edu/documents/PRISM_downloads_web_service.pdf

## Download Script
Files downloaded from the dataset are initially named `YYYY-MM-DD_N.nc.zip` by the download script. The YYYY-MM-DD specifies the date the data corresponds to, N is PRISM's grid count. Grid count ranges from 1-8, with a 1 specifying completely new and 8 representing finalized. PRISM assigns a new, higher number, when the data has been changed, and thus you should update your copy.

See more about the grid count at https://prism.oregonstate.edu/documents/PRISM_update_schedule.pdf

## Bootstrapping the data
Doing a full download of the PRISM data takes a long time. Their documentation urges at least a 2 second lapse in between web requests, so as not to overload their servers. Thus, if you are downloading all data for even 1 variable, it will take approximately 13.4 hours. (If each download takes 1 second and you wait 2 seconds).

To solve this issue, we recommend that you periodically download and store a full copy of the raw netCDF files. When you need to bootstrap the ETL downloading, you can load this in. Since our ETL will identify changes and new files by itself, as well as run idempotently, this will allow you to get to the processing of the ETL faster.
