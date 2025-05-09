from setuptools import setup, find_packages
from BigShyftsDataFoundationPackage.Scripts import *
import BigShyftsDataFoundationPackage

setup(
  name='BigShyftsDataFoundationPKG',
  version=BigShyftsDataFoundationPackage.__version__,
  author=BigShyftsDataFoundationPackage.__author__,
  url='https://community.cloud.databricks.com/',
  author_email='ghoraipabitrakumar@gmail.com',
  description='Framework to build wheel file for Databricks code',
  entry_points={"console_scripts":
  ['BigShyftsDataFoundationPackage = BigShyftsDataFoundationPackage.testwhl:main']
  
  },
  packages = ["BigShyftsDataFoundationPackage"],

)