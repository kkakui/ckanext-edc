# -*- coding: utf-8 -*-
from past.builtins import basestring
import logging

log = logging.getLogger(__name__)

def fix_edc_dcat(edc_dcat_dict):
    if 'edc:id' in edc_dcat_dict:
        # old format
        for key in ('id', 'name', 'description', 'contentType', 'keyword'):
            if 'edc:'+key in edc_dcat_dict:
                edc_dcat_dict[key] = edc_dcat_dict.pop('edc:'+key)
    edc_dcat_dict['identifier'] = edc_dcat_dict.get('id')
    edc_dcat_dict['title'] = edc_dcat_dict.get('name')
    edc_dcat_dict['dcat:distribution']['title'] = edc_dcat_dict.get('id')
    # edc_dcat_dict['dcat:distribution']['accessURL'] = edc_dcat_dict.get('dcat:service').get('dct:endpointUrl')
    edc_dcat_dict['dcat:distribution']['format'] = edc_dcat_dict.get('contentType')
    edc_dcat_dict['distribution'] = []
    edc_dcat_dict['distribution'].append(edc_dcat_dict.pop('dcat:distribution'))
    keyword = edc_dcat_dict.get('keyword')
    if keyword and not isinstance(keyword, list):
        edc_dcat_dict['keyword'] = [keyword]
    log.debug('fixed EDC DCAT: %s', edc_dcat_dict)
    return edc_dcat_dict
