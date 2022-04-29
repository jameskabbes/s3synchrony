[GitHub Pages](https://jameskabbes.github.io/s3synchrony)<br>
[PyPI](https://pypi.org/project/kabbes-s3synchrony)

# s3synchrony
Synchronizing data folders across all team members.

# Installation
```
pip install kabbes_s3synchrony
```

# Usage

## s3synchrony.__main__
To run s3synchrony within a command prompt, perform the following steps:

1. Navigate to the repository you would like to synchronize
```
cd C:/Path/to/repo
```

2. Make sure you have an "s3synchrony.json" file information on how to sync

3. Run the package from the command prompt
```
python -m s3synchrony
```

## Call Python script

```python
import s3synchrony
s3synchrony.run()
```


# Comprehensive Overview

## The Data Folder

When using S3Synchrony, you are synchronizing all of the data stored in a local directory with the data stored remotely, for example, an AWS S3 bucket. The S3 directory is referenced through both an AWS bucket, an AWS prefix, and the necessary credentials to access said prefix. The local directory to be used can be a relative or full path, and by default will be a subdirectory named "Data" stored in the same working directory.

- Project Folder
  - Data -> make sure you place your "Data" folder in your .gitignore
  - code, etc.

## smart_sync

The smart_sync function is the premier work of this package, and will perform all of the data synchronization for you. This function will check the passed platform name, and reference a self-contained list of supported platforms to instantiate the proper class. This list of supported platforms can be accessed via a call to get_supported_platforms().

Each connection type will require a different set of keyword arguments. For S3, the minimum arguments are "aws_bkt" and "aws_prfx". Please check the class docstrings for each connection type for more information.

All platform classes should be children of the DataPlatformConnection class which is an interface will all necessary public functions. For S3, a folder named .S3 will be created within your data folder. This .S3 folder will contain CSVs used for monitoring data changes and text files for storing small bits of information.

- **versions_remote.csv:** Contains the state of data stored remotely
- **versions_local.csv:** Contains the state of data stored locally
- **deleted_remote.csv:** Contains all files deleted remotely
- **deleted_local.csv:** Contains all files deleted locally
- **ignore_remoet.txt:** Contains a list of file paths to be ignored entirely

Using these CSVs, S3Synchrony can determine what files you have newly created, deleted, and modified. It will then prompt you to upload these changes to S3. Once you have done so, it will upload new CSVs as needed. After downloading these new CSVs, your collaborative peers will be prompted to download your own changes as well as upload their own.

In addition, a tmp folder will be utilised within the .S3 folder. This tmp folder contains downloaded files from S3 that are used to compute certain CSVs.

## Deletions

When deleting files, the user will be prompted to confirm their deletions. Files that are deleted locally will simply be removed. Files deleted from S3, however, will simply be moved into a "deleted" subfolder of the .S3 folder on S3.


## reset_all

Resetting all S3Synchrony services is as simple as deleting the .S3 folders contained locally and on S3. Once these are deleted, synchronization cannot occur until they are recreated, which can be done by simply making a new call to S3Synchrony.

Before resetting, however, a call to reset_confirm **must** occur. The user will then be prompted to confirm that they would like their .S3 folders removed.

# License
[GNU GPLv3](https://www.gnu.org/licenses/)


# Author(s)

*Created by*<br>
Sevan Brodjian - Ameren Innovation Center Intern

*Modified by*<br>
James Kabbes - Data Scientist: Ameren Innovation Center