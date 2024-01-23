# -*- coding: utf-8 -*-
from setuptools import setup

setup(
    entry_points='''
        [ckan.plugins]
        edc_harvester=ckanext.edc.harvester:EDCHarvester
    '''
)
