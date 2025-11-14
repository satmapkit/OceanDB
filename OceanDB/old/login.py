import os
import copernicusmarine

copernicusmarine.login(username=os.getenv('COPERNICUS_USERNAME'), password=os.getenv('COPERNICUS_PASSWORD'))

