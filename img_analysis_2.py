from astropy.io import fits
import json

with open('params.json', 'r') as file:
    params = json.load(file)

### Log date
date = params["date"]
### UT number
#ut = params["ut"]
### Relative path of destination file for parsed data
dest_arch_name = params["destination_filename"]
### Relative path of template file
tplt_arch_name = params["template_filename"]
### Relative path of observation csvs
obs_csv_name = params["observation_filename"]

### Example parsed img
# for log_section in log_sections:
#    for index, line in log_section:
#       check_start_read = re.search(r"START", parser.group())
#       if(check_start_read is not None): En vez de example_img, usar la linea de estas iteraciones

example_img = {"time": "03:21:49", "group": "START", "data": "lt1iaa", "Num_linea": 101346}

file_path = "img_files/UT{0}_N{1}-ONEIA-{2}T{3}.fits".format(1, example_img["data"][-1].upper(), '2025-08-05', example_img["time"].replace(":", "_"))


with fits.open(file_path) as hdul:
    # Print information about the FITS file structure
    hdul.info()

    # Access the primary HDU
    primary_hdu = hdul[0]

    # Get the header information
    header_info = primary_hdu.header
    print(header_info)
    print(header_info['DATE-OBS'])
    print(header_info['EXPTIME'])
    print(header_info['HIERARCH ESO DET EXP NO'])

    # Corroborar example_img["time"] con header_info["DATE"] ???
