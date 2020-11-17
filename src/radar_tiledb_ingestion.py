#!/usr/bin/env python3
"""Radar Sync
Copyright 2019, 2020 CRS4

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

# pylint: disable=import-error, broad-except
# pylint: disable=too-many-arguments


import io
import os
import os.path
import re
import sys
import stat
import json
import logging
import argparse
import traceback
import tempfile

import paramiko
import datetime
from PIL import Image
import numpy as np
from tdmq.client import Client


class SSHClient():
    """
    Class that handles ssh/sftp remote connections and operations.
    """
    def __init__(self,
                 username,
                 hostname,
                 port=22,
                 key_file=None,
                 root_dir='/'):
        self._hostname = hostname
        self._port = port
        self._username = username
        self._key_file = key_file
        self._client = paramiko.SSHClient()
        self._root_dir = root_dir

    def list_folder(self):
        """
        Lists folder contents.
        """
        _contents = list()

        try:
            self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self._client.connect(
                hostname=self._hostname,
                port=self._port,
                username=self._username,
                key_filename=self._key_file
            )

            _, stdout, _ = self._client.exec_command('ls {}'.format(self._root_dir))

            for line in stdout:
                _contents.append(line.strip('\n'))

            self._client.close()
        except Exception as ex:
            logging.critical("*** Caught exception: %s: %s", ex.__class__, ex)
            try:
                self._client.close()
            except Exception:
                pass

        return _contents

    def retrieve_file(self, remote_file, local_file):
        _transport = paramiko.Transport((self._hostname, self._port))
        _r_key = paramiko.RSAKey.from_private_key_file(self._key_file)

        try:
            _transport.connect(
                username=self._username,
                pkey=_r_key
            )

            _client = paramiko.SFTPClient.from_transport(_transport)
            _client.chdir(self._root_dir)

            if stat.S_ISREG(_client.stat(remote_file).st_mode):
                logging.debug("Retrieving SFTP remote file '%s' to '%s'", remote_file, local_file)
                _client.get(remote_file, local_file)

        except Exception as ex:
            logging.critical("*** Caught exception: %s: %s", ex.__class__, ex)
            traceback.print_exc()
            _transport.close()
            sys.exit(1)

            _transport.close()


def load_description(desc_file):
    logging.debug('loading source description from %s.', desc_file)
    with open(desc_file) as fd:
        return json.load(fd)


def fetch_radar_data(ssh_client, image_file_name):
    radar_data = dict()

    try:
        temp_file_handle, temp_file_path = tempfile.mkstemp()
        os.close(temp_file_handle)

        ssh_client.retrieve_file(image_file_name, temp_file_path)
        png = Image.open(temp_file_path)

        for channel in png.mode:
            radar_data[channel] = np.asarray(png.getchannel(channel))

        os.unlink(temp_file_path)
    except ValueError as vex:
        logging.error(vex)
        radar_data["L"] = np.zeros(source.shape)
        radar_data["L"].fill(mask_value)
    return radar_data


def ingest_missings(file_list, time_list):
    def _to_filename(image_timestamp):
        return image_timestamp.strftime('cag01est2400%Y-%m-%d_%H:%M:%S.png')

    existent_images = set(map(_to_filename, time_list))
    missing_images = list(set(file_list) - existent_images)
    missing_images.sort()
    
    return missing_images

def ingest_latests(last_timestamp, file_list):
    """
    Returns a list of the files with a timestamp in the name later than
    last_timestamp.
    """
    def _iterator(file_name):
        # Is a radar image file
        if re.match(r'cag01est2400\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}.png', file_name):
            file_timestamp = datetime.datetime.strptime(
                file_name, 'cag01est2400%Y-%m-%d_%H:%M:%S.png')
            if file_timestamp > last_timestamp:
                return True
            else:
                return False
        else:
            return False

    return list(filter(_iterator, file_list))


def check_ssh_url(ssh_url):
    """
    Checks if the given string represents a valid SSH/SFTP path.
    """
    _match = re.match(
        r'((?P<schema>\w+)://)?'
        r'((?P<user>[\w\._-]+)@)?'
        r'(?P<host>[\w\._-]+)'
        r'(:(?P<port>\d*))?'
        r'(?P<path>/[\w\._\-/]*)?',
        ssh_url)

    if _match:
        if _match.group('schema') and _match.group('schema') != 'ssh':
            return (None, None, None, None)

        return (_match.group('user'), _match.group('host'),
                int(_match.group('port')) if _match.group('port') else 22,
                _match.group('path') if _match.group('path') else "/")

    return (None, None, None, None)


def main():
    """
    Parses the command line and performs the SSH-to-TileDB copy.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--dryrun', '-n', action='store_true',
                        help=('check TileDB/SSH files differences only, '
                              'does not perform any copy'))
    parser.add_argument('--debug', '-d', action='store_true',
                        help=('prints debug messages'))
    parser.add_argument('--tdmq-url', action='store', type=str, required=True,
                        dest='tdmq_url',
                        help=('tdmq server and path of the form'))
    parser.add_argument('--ssh-url', action='store', type=str, required=True,
                        dest='ssh_url',
                        help=(
                            'ssh server and path of the form: '
                            '<USER>@<NAME_NODE>:<PORT>/PATH'))
    parser.add_argument('--ssh-key', action='store', type=str, required=True,
                        dest='ssh_key',
                        help=('key for ssh server authentication'))
    parser.add_argument('--desc-file', action='store', type=str, required=True,
                        dest='source_desc_file',
                        help=('source descrption file'))

    # Only one of --hours and --sync can be provided on command line
    sync_group = parser.add_mutually_exclusive_group()
    sync_group.add_argument('--hours', action='store',
                        dest='hours', default=24, type=int,
                        help=('uploads only the radar images '
                              'more recent than the given number of hours'))
    sync_group.add_argument('--sync', '-s', action='store_true',
                        dest='sync',
                        help=('upload all the missing radar images'))

    args = parser.parse_args()

    # If the debug flag is set, print all messages
    if args.debug:
        logging.basicConfig(
            level=logging.DEBUG,
            format='[%(levelname)s] %(message)s')
    else:
        logging.basicConfig(
            level=logging.INFO,
            format='[%(levelname)s] %(message)s')

    logging.getLogger("paramiko").setLevel(logging.WARNING)

    (_ssh_username, _ssh_hostname, _ssh_port,
     _ssh_root) = check_ssh_url(args.ssh_url)
    if _ssh_hostname is None:
        logging.error(
            'Wrong, incomplete or absent SSH path: \'%s\'', args.ssh_url)
        sys.exit(1)

    if os.path.isfile(args.ssh_key) == False:
        logging.error(
            'SSH key file not found: \'%s\'', args.ssh_key)
        sys.exit(1)

    if os.path.isfile(args.source_desc_file) == False:
        logging.error(
            'Source description file not found: \'%s\'', args.source_desc_file)
        sys.exit(1)

    _source_desc = load_description(args.source_desc_file)

    ssh_client = SSHClient(
        username=_ssh_username,
        hostname=_ssh_hostname,
        port=_ssh_port,
        key_file=args.ssh_key,
        root_dir=_ssh_root
    )

    _folder_list = ssh_client.list_folder()

    def _name_filter(file_name):
        # Is a radar image file
        if re.match(r'cag01est2400\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}.png', file_name):
            return True
        else:
            return False

    # Filter out not image files
    _image_list = list(filter(_name_filter, _folder_list))

    # Instantiates a TDMQ client, retrieves the source if exists or registers a
    # new one
    tdmq_client = Client(args.tdmq_url)
    sources = tdmq_client.find_sources({'id': _source_desc['id']})
    if len(sources) > 0:
        assert len(sources) == 1
        source = sources[0]
        logging.info(f"Using source {source.tdmq_id} for {source.id}.")
    else:
        source = tdmq_client.register_source(_source_desc)
        logging.info(f"Created source {source.tdmq_id} for {source.id}.")

    try:
        ts = source.timeseries()
        last_image_time = max(sorted(ts.time))
        _last_slot = max(ts.tiledb_indices)
    except Exception as ex:  # FIXME too general
        ts = []
        last_image_time = datetime.datetime(1970, 1, 1, 0, 0, 0)
        _last_slot = 0

    # Builds the list of file to download
    if args.sync:
        _images_to_ingest = ingest_missings(_image_list, ts.time)
    else:
        start_time = (
            datetime.datetime.now() - datetime.timedelta(hours=args.hours)
        ).replace( minute=0, second=0, microsecond=0)

        logging.info(f"Requested images from {start_time} (last local image is {last_image_time}).")
        if start_time > last_image_time:
            last_image_time = start_time

        _images_to_ingest = ingest_latests(last_image_time, _image_list)

    logging.info(
        f"Remote files: {len(_folder_list)}, remote images: "
        f"{len(_image_list)}, images to sync: {len(_images_to_ingest)}.")

    for _image in _images_to_ingest:
        _timestamp = datetime.datetime.strptime(
            _image, 'cag01est2400%Y-%m-%d_%H:%M:%S.png')
        _last_slot = _last_slot + 1

        if args.dryrun:
            logging.debug(f"[DRY-RUN] Ingesting data at time {_timestamp}, slot {_last_slot}.")
        else:
            logging.debug(f"Ingesting data at time {_timestamp}, slot {_last_slot}.")
            _data = fetch_radar_data(ssh_client, _image)
            source.ingest(_timestamp, _data, _last_slot)
    logging.info(f"Done ingesting.")

if __name__ == "__main__":
    main()

# vim:ts=4:expandtab
