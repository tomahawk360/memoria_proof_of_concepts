from io import StringIO
import csv
import requests

def fetch_obs_file(destination_folder, obs_date) -> int:
    """Fetch the VLT Observations CSV file from the ESO raw database and saves it in the obs_files folder
    
    Args:
        destination_folder: Relative path of the folder where the fetched CSV file is stored.
        obs_date: Date of the CSV file to be fetched.

    Returns:
        Status code of the fetch
    """

    url = "https://archive.eso.org/wdb/wdb/eso/eso_archive_main/query"

    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:142.0) Gecko/20100101 Firefox/142.0",
        "Referer": "https://archive.eso.org/eso/eso_archive_main.html",
    }

    payload = {
        "wdbo": (None, "csv/download"),
        "max_rows_returned": (None, "999999"),
        "instrument": (None, ""),
        "tab_object": (None, "on"),
        "target": (None, ""),
        "resolver": (None, "simbad"),
        "ra": (None, ""),
        "dec": (None, ""),
        "box": (None, "00 10 00"),
        "degrees_or_hours": (None, "hours"),
        "tab_target_coord": (None, "on"),
        "format": (None, "SexaHour"),
        "wdb_input_file": (None, ""),
        "night": (None, obs_date),
        "stime": (None, ""),
        "starttime": (None, "12"),
        "etime": (None, ""),
        "endtime": (None, "12"),
        "tab_prog_id": (None, "on"),
        "prog_id": (None, ""),
        "gto": (None, ""),
        "pi_coi": (None, ""),
        "obs_mode": (None, ""),
        "title": (None, ""),
        "image[]": (None, "EFOSC;'IMA%'"),
        "image[]": (None, "EMMI;'IMA%'"),
        "image[]": (None, "ERIS"),
        "image[]": (None, "FORS1;'IMA%'"),
        "image[]": (None, "FORS2;'IMA%'"),
        "image[]": (None, "HAWKI;'IMA%'"),
        "image[]": (None, "GROND"),
        "image[]": (None, "ISAAC;'IMA%'"),
        "image[]": (None, "NAOS+CONICA;'IMA%','SDI%'"),
        "image[]": (None, "OMEGACAM"),
        "image[]": (None, "SOFI;'IMA%'"),
        "image[]": (None, "SPHERE;'IMA%'"),
        "image[]": (None, "SUSI"),
        "image[]": (None, "TIMMI;'IMA%'"),
        "image[]": (None, "VIMOS;'IMA%'"),
        "image[]": (None, "VIRCAM"),
        "image[]": (None, "VISIR;'IMA%'"),
        "image[]": (None, "WFI"),
        "image[]": (None, "XSHOOTER;'IMA%'"),
        "spectrum[]": (None, "CES"),
        "spectrum[]": (None, "CRIRE;'SPECTRUM%'"),
        "spectrum[]": (None, "EFOSC;'SPECTRUM%'"),
        "spectrum[]": (None, "EMMI;'SPECTRUM%','ECHELLE%','MOS%'"),
        "spectrum[]": (None, "ERIS;'IFU%','%LSS%'"),
        "spectrum[]": (None, "ESPRESSO"),
        "spectrum[]": (None, "FEROS"),
        "spectrum[]": (None, "FORS1;'SPECTRUM%','MOS%','IMAGE_SPECTRUM%'"),
        "spectrum[]": (
            None,
            "FORS2;'SPECTRUM%','ECHELLE%','MOS%','MXU%','HIT%','IMAGE_SPECTRUM%'",
        ),
        "spectrum[]": (None, "GIRAF"),
        "spectrum[]": (None, "HARPS"),
        "spectrum[]": (None, "ISAAC;'SPECTRUM%'"),
        "spectrum[]": (None, "KMOS"),
        "spectrum[]": (None, "MUSE"),
        "spectrum[]": (None, "NAOS+CONICA;'SPECTRUM%'"),
        "spectrum[]": (None, "NIRPS"),
        "spectrum[]": (None, "SINFO"),
        "spectrum[]": (None, "SOFI;'SPECTRUM%'"),
        "spectrum[]": (None, "SPHERE;'IFU%','SPECTRUM%'"),
        "spectrum[]": (None, "TIMMI;'SPECTRUM%'"),
        "spectrum[]": (None, "UVES"),
        "spectrum[]": (None, "VIMOS;'IFU%','MOS%'"),
        "spectrum[]": (None, "VISIR;'SPECTRUM%','ECHELLE%'"),
        "spectrum[]": (None, "SHOOT"),
        "vlti[]": (None, "AMBER"),
        "vlti[]": (None, "GRAVITY"),
        "vlti[]": (None, "MATISSE"),
        "vlti[]": (None, "MIDI"),
        "vlti[]": (None, "PIONIER"),
        "vlti[]": (None, "VINCI"),
        "polarim[]": (None, "EFOSC;'POLARIM%'"),
        "polarim[]": (None, "FORS1;'POLARIM%'"),
        "polarim[]": (None, "FORS2;'POLARIM%'"),
        "polarim[]": (None, "ISAAC;'POLARIM%'"),
        "polarim[]": (None, "NAOS+CONICA;'POLARIM%'"),
        "polarim[]": (None, "SOFI;'POLARIM%'"),
        "polarim[]": (None, "SPHERE;'POLARIM%'"),
        "corono[]": (None, "EFOSC;'%CORO%'"),
        "corono[]": (None, "ERIS;'CORO%'"),
        "corono[]": (None, "NAOS+CONICA;'%CORO%'"),
        "corono[]": (None, "SPHERE;'%CORO%'"),
        "corono[]": (None, "VISIR;'%CORO%'"),
        "other[]": (None, "ALPACA"),
        "other[]": (None, "APICAM"),
        "other[]": (None, "APEXBOL"),
        "other[]": (None, "APEXHET"),
        "other[]": (None, "FAIM6"),
        "other[]": (None, "FAIM7"),
        "other[]": (None, "GRIPS19"),
        "other[]": (None, "LGSF"),
        "other[]": (None, "MAD"),
        "other[]": (None, "MASCOT"),
        "other[]": (None, "SPECU"),
        "other[]": (None, "WFCAM"),
        "sam[]": (None, "ERIS;'%SAM%'"),
        "sam[]": (None, "NAOS+CONICA;'%SAM%'"),
        "sam[]": (None, "SPHERE;'%SAM%'"),
        "sam[]": (None, "VISIR;'SAM%'"),
        "tab_dp_cat": (None, "on"),
        "dp_cat": (None, "SCIENCE"),
        "dp_cat": (None, "ACQUISITION"),
        "tab_dp_type": (None, "on"),
        "dp_type": (None, ""),
        "dp_type_user": (None, ""),
        "tab_dp_tech": (None, "on"),
        "dp_tech": (None, ""),
        "dp_tech_user": (None, ""),
        "tab_dp_id": (None, "on"),
        "dp_id": (None, ""),
        "origfile": (None, ""),
        "tab_rel_date": (None, "on"),
        "rel_date": (None, ""),
        "obs_name": (None, ""),
        "ob_id": (None, ""),
        "tab_tpl_start": (None, "on"),
        "tpl_start": (None, ""),
        "tab_tpl_id": (None, "on"),
        "tpl_id": (None, ""),
        "tab_exptime": (None, "on"),
        "exptime": (None, ""),
        "tab_filter_path": (None, "on"),
        "filter_path": (None, ""),
        "tab_wavelength_input": (None, "on"),
        "wavelength_input": (None, ""),
        "tab_fwhm_input": (None, "on"),
        "fwhm_input": (None, ""),
        "gris_path": (None, ""),
        "grat_path": (None, ""),
        "slit_path": (None, ""),
        "tab_instrument": (None, "on"),
        "add": (
            None,
            "((ins_id like 'EFOSC%' AND (dp_tech like 'IMA%')) or (ins_id like 'EMMI%' AND (dp_tech like 'IMA%')) or (ins_id like 'ERIS%') or (ins_id like ('ERIS%') AND ((dp_tech like 'IMA%') AND (dp_tech not like '%SAM%'))) or (ins_id like 'FORS1%' AND (dp_tech like 'IMA%')) or (ins_id like 'FORS2%' AND (dp_tech like 'IMA%')) or (ins_id like 'HAWKI%' AND (dp_tech like 'IMA%')) or (ins_id like 'GROND%') or (ins_id like 'ISAAC%' AND (dp_tech like 'IMA%')) or (ins_id like 'NAOS+CONICA%' AND (dp_tech like 'IMA%' OR dp_tech like 'SDI%')) or (ins_id like 'OMEGACAM%') or (ins_id like 'SOFI%' AND (dp_tech like 'IMA%')) or (ins_id like 'SPHERE%' AND (dp_tech like 'IMA%')) or (ins_id like 'SUSI%') or (ins_id like 'TIMMI%' AND (dp_tech like 'IMA%')) or (ins_id like 'VIMOS%' AND (dp_tech like 'IMA%')) or (ins_id like 'VIRCAM%') or (ins_id like 'VISIR%' AND (dp_tech like 'IMA%')) or (ins_id like 'WFI%') or (ins_id like 'XSHOOTER%' AND (dp_tech like 'IMA%')) or (ins_id like 'CES%') or (ins_id like 'CRIRE%' AND ((dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'EFOSC%' AND ((dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'EMMI%' AND ((dp_tech like 'SPECTRUM%' OR dp_tech like 'ECHELLE%' OR dp_tech like 'MOS%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'ERIS%' AND ((dp_tech like 'IFU%' OR dp_tech like '%LSS%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'ESPRESSO%') or (ins_id like 'FEROS%') or (ins_id like 'FORS1%' AND ((dp_tech like 'SPECTRUM%' OR dp_tech like 'MOS%' OR dp_tech like 'IMAGE_SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'FORS2%' AND ((dp_tech like 'SPECTRUM%' OR dp_tech like 'ECHELLE%' OR dp_tech like 'MOS%' OR dp_tech like 'MXU%' OR dp_tech like 'HIT%' OR dp_tech like 'IMAGE_SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'GIRAF%') or (ins_id like 'HARPS%') or (ins_id like 'ISAAC%' AND ((dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'KMOS%') or (ins_id like 'MUSE%') or (ins_id like 'NAOS+CONICA%' AND ((dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'NIRPS%') or (ins_id like 'SINFO%') or (ins_id like 'SOFI%' AND ((dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'SPHERE%' AND ((dp_tech like 'IFU%' OR dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'TIMMI%' AND ((dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'UVES%') or (ins_id like 'VIMOS%' AND ((dp_tech like 'IFU%' OR dp_tech like 'MOS%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'VISIR%' AND ((dp_tech like 'SPECTRUM%' OR dp_tech like 'ECHELLE%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'SHOOT%') or (ins_id in ('SHOOT','XSHOOTER') AND ((dp_tech like 'ECHELLE%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'AMBER%') or (ins_id like 'GRAVITY%') or (ins_id like 'MATISSE%') or (ins_id like 'MIDI%') or (ins_id like 'PIONIER%') or (ins_id like 'VINCI%') or (ins_id like 'EFOSC%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'FORS1%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'FORS2%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'ISAAC%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'NAOS+CONICA%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'SOFI%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'SPHERE%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'EFOSC%' AND (dp_tech like '%CORO%')) or (ins_id like 'ERIS%' AND (dp_tech like 'CORO%')) or (ins_id like 'NAOS+CONICA%' AND (dp_tech like '%CORO%')) or (ins_id like 'SPHERE%' AND (dp_tech like '%CORO%')) or (ins_id like 'VISIR%' AND (dp_tech like '%CORO%')) or (ins_id like 'ALPACA%') or (ins_id like 'APICAM%') or (ins_id like 'APEXBOL%') or (ins_id like 'APEXHET%') or (ins_id like 'FAIM6%') or (ins_id like 'FAIM7%') or (ins_id like 'GRIPS19%') or (ins_id like 'LGSF%') or (ins_id like 'MAD%') or (ins_id like 'MASCOT%') or (ins_id like 'SPECU%') or (ins_id like 'WFCAM%') or (ins_id like 'ERIS%' AND (dp_tech like '%SAM%')) or (ins_id like 'NAOS+CONICA%' AND (dp_tech like '%SAM%')) or (ins_id like 'SPHERE%' AND (dp_tech like '%SAM%')) or (ins_id like 'VISIR%' AND (dp_tech like 'SAM%')))",
        ),
        "tab_tel_airm_start": (None, "on"),
        "tab_stat_instrument": (None, "on"),
        "tab_ambient": (None, "on"),
        "tab_stat_exptime": (None, "on"),
        "tab_HDR": (None, "on"),
        "tab_mjd_obs": (None, "on"),
        "aladin_colour": (None, "aladin_instrument"),
        "tab_stat_plot": (None, "on"),
        "order": (None, ""),
    }

    # Make a POST request with data
    response = requests.post(url, files=payload, headers=header)

    # Access the response body as CSV
    if response.status_code == 200:
        obs_csv_list = []

        csv_data = StringIO(response.text)
        reader = csv.reader(csv_data)

        for row in reader:
            if len(row) > 1:
                obs_csv_list.append(row)

        with open(
            "{0}/{1}.csv".format(destination_folder, obs_date), "w", newline=""
        ) as csvfile:
            writer = csv.writer(csvfile)

            # Write the header row
            writer.writerow(obs_csv_list[0])

            # Write the remaining data rows
            writer.writerows(obs_csv_list[1:])

    else:
        print("Error while fetching observation csv ", response.status_code)

    return response.status_code
