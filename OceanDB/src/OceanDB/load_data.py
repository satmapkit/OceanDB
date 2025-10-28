import netCDF4 as nc
import pandas as pd
import psycopg as pg
from psycopg import sql
import glob
import struct
from io import BytesIO
import time
from dateutil.relativedelta import relativedelta
from datetime import timedelta
import os
import yaml
import numpy as np
from OceanDB.MOceanDB import OceanDB
from functools import cached_property

from OceanDB.load_sql import load_sql

filepath = "/app/data/copernicus/SEALEVEL_GLO_PHY_L3_MY_008_062/cmems_obs-sl_glo_phy-ssh_my_c2n-l3-duacs_PT1S_202411/2020/08/dt_global_c2n_phy_l3_1hz_20200801_20240205.nc"

ocean_db = OceanDB()

load_sql()

dataset = nc.Dataset(filepath)