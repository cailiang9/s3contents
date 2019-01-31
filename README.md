# GFContents

A HDFS, S3 and GS backed Jupyter ContentsManager implementation via tensorflow.gfile.

It aims to a be a transparent, drop-in replacement for Jupyter standard filesystem-backed storage system.

## Features

Supports local directories, HDFS, S3 and GS filesystems.
Supports multiple large file download and upload.

## Prerequisites

Write access (valid credentials) to an S3/GCS bucket, this could be on AWS/GCP or a self hosted S3 .
Tensorflow

## Installation

```
$ pip install gfcontents
```

## Jupyter config

Edit `~/.jupyter/jupyter_notebook_config.py` by filling the missing values:

### S3, HDFS and local directory

```python
from s3contents import GFContentsManager, HybridContentsManager
from IPython.html.services.contents.largefilemanager import LargeFileManager

c.NotebookApp.contents_manager_class = HybridContentsManager
c.HybridContentsManager.manager_classes = {
    "S3": GFContentsManager,
    "HDFS": GFContentsManager,
    "": LargeFileManager,
}
c.HybridContentsManager.manager_kwargs = {
    "S3": {
        "prefix": "s3://bucket_name/notebooks",
    },
    "HDFS": {
        "prefix": "hdfs://hdfsip:8020/user/logname",
    },
    "": {
        "root_dir": "/home/logname/",
    },
}
```
