# -*- coding: utf-8 -*-
import json
import requests
import datetime
import uuid
import re
from ckan import plugins
from ckanext.dcat.harvesters._json import DCATJSONHarvester
from ckanext.dcat.harvesters.base import DCATHarvester
from ckanext.dcat.interfaces import IDCATRDFHarvester
from ckanext.dcat import converters
from ckanext.dcat import utils
from ckanext.edc.converters import fix_edc_dcat

import logging
log = logging.getLogger(__name__)

class EDCHarvester(DCATJSONHarvester):

    def info(self):
        return {
            'name': 'edc',
            'title': 'EDC Connector',
            'description': 'Harvester for EDC Connectors using Management API. Please set an API endpoint for catalog requesting as URL.'
        }

    def validate_config(self, source_config):
        if not source_config:
            return source_config

        source_config_obj = json.loads(source_config)
        connector_dsp_endpoint =  source_config_obj.get('connector_dsp_endpoint')
        if not connector_dsp_endpoint or not isinstance(connector_dsp_endpoint, str):
            raise ValueError('connector_dsp_endpoint must be set as a URL string')

        return source_config

    def _get_content_and_type(self, url, harvest_job, page):
        if harvest_job.source.config:
            self.config = json.loads(harvest_job.source.config)
        else:
            self.config = {}
        if not url.lower().startswith('http'):
            log.debug('Getting local file %s', url)
            return DCATHarvester._get_content_and_type(self, url, harvest_job, page)
        try:
            log.debug('Getting catalog from %s', url)
            session = requests.Session()
            for harvester in plugins.PluginImplementations(IDCATRDFHarvester):
                session = harvester.update_session(session)
            headers = {'Content-Type': 'application/json'}
            api_key_header = self.config.get('api_key_header')
            api_key = self.config.get('api_key')
            if api_key:
                if api_key_header:
                    headers[api_key_header] = api_key
                else:
                    headers['x-api-key'] = api_key
            else:
                headers['x-api-key'] = 'ApiKeyDefaultValue'
            catalog_request = {
                '@context' : {
                    'edc': 'https://w3id.org/edc/v0.0.1/ns/'
                },
                'protocol': 'dataspace-protocol-http',
                'counterPartyAddress': self.config.get('connector_dsp_endpoint')
            }
            log.debug('Catelog request: %s', catalog_request)
            r = session.post(url, headers=headers, json=catalog_request)
            r.raise_for_status()
            content = r.content.decode('utf-8')
            content_type = r.headers.get('Content-Type').split(";", 1)[0]
            return content, content_type
        except requests.exceptions.HTTPError as error:
            msg = 'Could not get content from %s. Server responded with %s %s'\
                % (url, error.response.status_code, error.response.reason)
            self._save_gather_error(msg, harvest_job)
            return None, None
        except requests.exceptions.ConnectionError as error:
            msg = '''Could not get content from %s because a
                                connection error occurred. %s''' % (url, error)
            self._save_gather_error(msg, harvest_job)
            return None, None
        except requests.exceptions.Timeout as error:
            msg = 'Could not get content from %s because the connection timed'\
                ' out.' % url
            self._save_gather_error(msg, harvest_job)
            return None, None

    def _get_guids_and_datasets(self, content):
        doc = json.loads(content)

        if isinstance(doc, list):
            # Assume a list of datasets
            datasets = doc
        elif isinstance(doc, dict):
            datasets = doc.get('dcat:dataset', [])
            if not isinstance(datasets, list):
                datasets = [datasets]
            dataservice = doc.get('dcat:service')
        else:
            raise ValueError('Wrong JSON object')

        for dataset in datasets:
            if dataservice:
                dataset['dcat:service'] = dataservice
            as_string = json.dumps(dataset)

            # Get identifier
            guid = dataset.get('id')
            if not guid:
                # This is bad, any ideas welcomed
                guid = sha1(as_string).hexdigest()

            yield guid, as_string

    def _get_package_dict(self, harvest_object):

        content = harvest_object.content

        dcat_dict = json.loads(content)
        dcat_dict = fix_edc_dcat(dcat_dict)
        package_dict = converters.dcat_to_ckan(dcat_dict)
        package_dict['name'] = dcat_dict.get('id')
        return package_dict, dcat_dict

    def _generate_uuid_for_dataset(self, url, name):
        ns = uuid.uuid3(uuid.NAMESPACE_URL, url)
        return uuid.uuid3(ns, name)

    def _get_explain_url(self, dcat_dict):
        description = dcat_dict.get('description')
        if description:
            match = re.search('(?P<tag>explain_url:) *(?P<url>https?://[^\s"\']+)', description)
            if match:
                return match.group('url')
        return None

    def modify_package_dict(self, package_dict, dcat_dict, harvest_object):
        if not package_dict.get('notes'):
            package_dict['notes'] = self.config.get('default_notes') or 'a data asset offered to you'
        status = self._get_object_extra(harvest_object, 'status')
        if status == 'new':
            today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
            package_dict['extras'].append({'key': 'issued', 'value': today})
        connector_dsp_endpoint = self.config.get('connector_dsp_endpoint')
        package_dict['extras'].append({'key': 'caddec_provider_id', 'value': connector_dsp_endpoint})
        dataset_id = self._generate_uuid_for_dataset(connector_dsp_endpoint, package_dict.get('name'))
        package_dict['extras'].append({'key': 'caddec_dataset_id_for_detail', 'value': dataset_id})
        explain_url = self._get_explain_url(dcat_dict) or self.config.get('default_explain_url', utils.catalog_uri())
        for resource in package_dict['resources']:
            resource['explain_url'] = explain_url
            resource['caddec_required'] = 'required'
            resource['caddec_resource_type'] = 'file/http'
        return package_dict
