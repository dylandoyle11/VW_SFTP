#!/usr/bin/python3

"""
------------------------------------------------------------------------
VW SFTP to Motoinsight FTP
------------------------------------------------------------------------
Author: Dylan Doyle
Updated: 2020-02-06
------------------------------------------------------------------------
Notes:

This script connects to the Volkswagen SFTP and downloads the most recent
file with the PREFIX "ops_mdm_veh_inven_en_fr_vw_can_". Once the download
is complete, the contents of the zip folder are extracted to a temporary
folder and all the files are then uploaded to the Motocommerce FTP server
------------------------------------------------------------------------
"""
from ftplib import FTP
import pysftp
import paramiko
import os
import shutil
import zipfile
from base64 import decodebytes
from pathlib import Path
import pandas as pd
import numpy as np
import sys
import math
from errors import *
sys.path.insert(1, os.path.join('..', 'SlackBot'))
from SlackBot import *
import traceback


messages = []
PREFIX = 'ops_mdm_veh_inven_en_fr_vw_can'
LOG_FILE = "log_file.txt"
channels = ['vw-ftp-script']
path = os.path.join(os.getcwd(), 'temp')


def unzip_file(filename, path):
    """
    -------------------------------------------------------
    Unzips given file.
    -------------------------------------------------------
    Args:
    filename: string
        Name of file to be unzipped
    path: string
        directory to locate filename
    ------------------------------------------------------
    """
    if filename:
        # Unzip
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            zip_ref.extractall(path=path)
            zip_ref.extract
        # Remove zip folder
        os.remove(filename)


def convert_file(curr_ext, new_ext, filename, path):
    """
    -------------------------------------------------------
    Changes extension of given file.
    -------------------------------------------------------
    Args: {arg type}
        curr_ext: string
            Current file extension
        new_ext: string
            New file extension
        filename: string
            Name of file to be unzipped
        path: string
            Directory to locate filename

    Returns:
        filename: string
            Altered filename
    ------------------------------------------------------
    """
    filename = filename.split('.')[0]
    os.rename(os.path.join(path, f'{filename+curr_ext}'), os.path.join(path, f'{filename+new_ext}'))
    filename = filename + new_ext
    return filename


def remove_temp_files(filename, path):
    """
    -------------------------------------------------------
    Clears temp folder.
    -------------------------------------------------------
    Args:
        filename: string
            Name of file to be removed
        path: string
            directory to locate filename
    Returns:
        Describe the type and semantics of the return value. If the function only
        returns None, this section is not required.
    ------------------------------------------------------
"""
    print('Removing "temp" folder and all its contents...')
    # Remove all files in temp folder
    for filename in os.listdir(path):
        file_path = os.path.join(path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f'Failed to delete {filepath}. Reason: {e}' % (file_path, e))


def log_file(filename):
    """
    -------------------------------------------------------
    Records downloaded file name into log to be tracked.
    -------------------------------------------------------
    Args:
        filename: string
            Name of file to log
    ------------------------------------------------------
    """
    print('Adding file to log...\n')
    with open(LOG_FILE, 'a') as log:
        log.write(f'{filename.strip(".txt")+".zip"}\n')


def check_duplicate(filename):
    """
    -------------------------------------------------------
    Checks log file for any repeat downloads.
    -------------------------------------------------------
    Args:
        filename: string
            Name of file to be checked in log file

    Returns:
        repeat: boolean
            status of duplicate detection
    ------------------------------------------------------
    """
    repeat = False
    for line in open(LOG_FILE, 'r+'):
        if filename in line.strip('\n'):
            repeat = True
    return repeat


def download_sftp(FTP_HOST_VW, FTP_USER_VW, FTP_PASS_VW, VW_HOSTKEY):
    """
    -------------------------------------------------------
    Download file from sftp.
    -------------------------------------------------------
    Args: {arg type}
        FTP_HOST_VW: string
            VW FTP host name
        FTP_USER_VW: string
            VW FTP user name
        FTP_PASS_VW: string
            VW FTP password
        VW_HOSTKEY: string
            VW host specific key
    Returns:
        Name of file downloaded
    Raises:
        FTPConnectionError: FTP connection credentials are incorrect
        KeyError: Incorrect file search key, cannot locate file within ftp
        DuplicateFileError: Detected file has already been logged and downloaded
    ------------------------------------------------------
    """
    # Add hostkey to cnopts
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys.add(FTP_HOST_VW, 'ssh-rsa', VW_HOSTKEY)

    try:
        # Download most recent zip file that matches naming scheme "/vw_out/ops_mdm_veh_inven_en_fr_vw_can_*" and assign it to filename
        with pysftp.Connection(host=FTP_HOST_VW, username=FTP_USER_VW, password=FTP_PASS_VW, cnopts=cnopts) as sftp:
            print('Login to VW SFTP was successful')

            # CD into "vw_out" folder
            sftp.cwd('/vw_out/')

            # Select files from directory that start with PREFIX
            files = []
            for f in sftp.listdir():
                if f.startswith(PREFIX):
                    files.append(f)

            # Check if any files were captured
            if len(files) == 0:
                raise KeyError(f'Unable to find any files in VW SFTP directory "/vw_out/" that begin with PREFIX "{PREFIX}"')

            # Grab most recent file among files with PREFIX
            filename = sorted(files, reverse=True)[0]

            repeat_status = check_duplicate(filename)
            if repeat_status is True:
                raise DuplicateFileError('File has already been logged.')

            # if file is found, begin down load
            if filename:
                print(f'Downloading "{filename}" to directory "{os.getcwd()}"...')
                sftp.get(filename)
                print(f'Download complete')
                log_file(filename)
            else:
                raise KeyError(f'Unable to find any files in VW SFTP directory "/vw_out/" that begin with PREFIX "{PREFIX}"')

    except paramiko.ssh_exception.AuthenticationException as e:
        raise FTPConnectionError(f'Unable to authenticate connection to sftp: {e}')
    except pysftp.exceptions.ConnectionException as e:
        raise FTPConnectionError(f'Unable to establish connection to host : {e}')

        # NOTE: This does not look at date modified, only date in the filename.
        # i.e. ops_mdm_veh_inven_en_fr_vw_can_20201207.zip will be chosen before ops_mdm_veh_inven_en_fr_vw_can_20201206.zip regardless of date modified

    # Check if filename is in log file


    return filename


def ftp_upload(FTP_HOST_MC, FTP_USER_MC, FTP_PASS_MC, filename, path):
    """
    -------------------------------------------------------
    Uploads file to ftp.
    -------------------------------------------------------
    Args:
        FTP_HOST_MC: string
            Moto FTP host name
        FTP_USER_MC: string
            Moto FTP user name
        FTP_PASS_MC: string
            Moto FTP password
        filename: string
            Name of file to be unzipped
        path: string
            directory to locate filename
    ------------------------------------------------------
    """
    with FTP(host=FTP_HOST_MC, user=FTP_USER_MC, passwd=FTP_PASS_MC) as ftp:
        print('Login to Motocommerce FTP was successful')
        for new_path, subdirs, files in os.walk(path):
            for name in files:
                if not new_path.endswith('__MACOSX'):
                    filename = os.path.join(new_path, name)
                    print(f'Uploading "{filename}"')
                    with open(filename, "rb") as fh:
                        # Use FTP's STOR command to upload the file
                        ftp.storbinary(f"STOR {name}", fh)


def change_pictures(filename):
    """
    -------------------------------------------------------
    Reformats image links to include picture numbers
    -------------------------------------------------------
    Args:
        filename: string
            Name of file to be altered
    ------------------------------------------------------
    """
    # Read downloaded file
    df = pd.read_csv(os.path.join(path, filename))
    pic_count = 0

    print('Updating image links...')
    # iterate through rows
    for index, row in df.iterrows():
        pic_path_list = []
        # Store picture count as integer
        pic_count = row['NON_TRASH_PHOTO_COUNT']
        # Iterate through cells not equal to nan
        if math.isnan(pic_count) is False:
            pic_count = int(pic_count)
            for i in range(pic_count):
                # Take pic_path, remove seq#.jpg and replace with pic number
                # Reappend .jpg extension and add the new link to list
                pic_path = row['IMAGE_URL_PATTERN']
                pic_path = pic_path.strip('seq#.jpg')
                pic_path = f'{pic_path}{(int(i)+1)}.jpg'
                # Add pipe character delimiter
                pic_path_list.append(f'{pic_path}|')

        # Consolidate list into single string
        final_pic_path = ''.join(pic_path_list)
        # Remove final delimiter
        final_pic_path = final_pic_path[:-1]
        # Overwrite cell with string of newly formatted links
        df.loc[index, 'IMAGE_URL_PATTERN'] = final_pic_path
    print('Image links successfully updated!')
    df.to_csv(os.path.join(path, filename), index=False)



def main():

    run_success = False
    messages.append('An error has occurred:')
    try:
        filename = download_sftp(FTP_HOST_VW, FTP_USER_VW, FTP_PASS_VW, VW_HOSTKEY)
        unzip_file(filename, path)
        filename = convert_file('.txt', '.csv', filename, path)
        change_pictures(filename)
        filename = convert_file('.csv', '.txt', filename, path)
        ftp_upload(FTP_HOST_MC, FTP_USER_MC, FTP_PASS_MC, filename, path)
        remove_temp_files(filename, path)
        os.rmdir(path)
        run_success = True

    except FTPConnectionError as e:
        print(e)
        messages.append(str(e))
    except KeyError as e:
        print(e)
        messages.append(str(e))
    except DuplicateFileError as e:
        print(e)
        messages.append(str(e))
    except Exception as e:
        print(f'Unknown error: {e}')
        messages.append(f'Unknown error: {str(e)}')

    if run_success is False:
        print("Sending notification via Slackbot...")
        slack_send_message(channels, messages)

    print('Terminating...')

if __name__ == '__main__':
    main()
