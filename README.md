# VW SFTP to Motoinsight FTP
This Python script downloads the most recent file with the prefix ops_mdm_veh_inven_en_fr_vw_can_ from the Volkswagen SFTP server, extracts the contents of the zip folder to a temporary folder, and uploads all the files to the Motocommerce FTP server.

## Requirements
Python 3.x
ftplib, pysftp, paramiko, os, shutil, zipfile, base64, pathlib, pandas, numpy, and sys libraries.
