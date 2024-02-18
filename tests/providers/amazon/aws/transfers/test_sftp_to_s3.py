#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from unittest import mock

from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator

TASK_ID = "test_sftp_to_s3"
BUCKET = "test-s3-bucket"
S3_KEY = "test/test_1_file.csv"
SFTP_PATH = "/tmp/remote_path.txt"
AWS_CONN_ID = "aws_default"
SFTP_CONN_ID = "sftp_default"
S3_KEY_MULTIPLE = "test/"
SFTP_PATH_MULTIPLE = "/tmp/"


class TestSFTPToS3Operator:
    def assert_execute(
        self, mock_local_tmp_file, mock_s3_hook_load_file, mock_sftp_hook_retrieve_file, sftp_file, s3_file
    ):
        mock_local_tmp_file_value = mock_local_tmp_file.return_value.__enter__.return_value
        mock_sftp_hook_retrieve_file.assert_called_once_with(
            local_full_path=mock_local_tmp_file_value.name, remote_full_path=sftp_file
        )

        mock_s3_hook_load_file.assert_called_once_with(
            filename=mock_local_tmp_file_value.name,
            key=s3_file,
            bucket_name=BUCKET,
            replace=False,
        )

    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.retrieve_file")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.load_file")
    @mock.patch("airflow.providers.amazon.aws.transfers.sftp_to_s3.NamedTemporaryFile")
    def test_execute(self, mock_local_tmp_file, mock_s3_hook_load_file, mock_sftp_hook_retrieve_file):
        operator = SFTPToS3Operator(task_id=TASK_ID, s3_bucket=BUCKET, s3_key=S3_KEY, sftp_path=SFTP_PATH)
        operator.execute(None)

        self.assert_execute(
            mock_local_tmp_file,
            mock_s3_hook_load_file,
            mock_sftp_hook_retrieve_file,
            sftp_file=operator.sftp_path,
            s3_file=operator.s3_key,
        )

    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.retrieve_file")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.load_file")
    @mock.patch("airflow.providers.amazon.aws.transfers.sftp_to_s3.NamedTemporaryFile")
    def test_execute_multiple_files_different_names(
        self, mock_local_tmp_file, mock_s3_hook_load_file, mock_sftp_hook_retrieve_file
    ):
        operator = SFTPToS3Operator(
            task_id=TASK_ID,
            s3_bucket=BUCKET,
            s3_key=S3_KEY_MULTIPLE,
            sftp_path=SFTP_PATH_MULTIPLE,
            sftp_filenames=["test1.txt"],
            s3_filenames=["test1_s3.txt"],
        )
        operator.execute(None)

        self.assert_execute(
            mock_local_tmp_file,
            mock_s3_hook_load_file,
            mock_sftp_hook_retrieve_file,
            sftp_file=operator.sftp_path + operator.sftp_filenames[0],
            s3_file=operator.s3_key + operator.s3_filenames[0],
        )

    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.retrieve_file")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.load_file")
    @mock.patch("airflow.providers.amazon.aws.transfers.sftp_to_s3.NamedTemporaryFile")
    def test_execute_multiple_files_same_names(
        self, mock_local_tmp_file, mock_s3_hook_load_file, mock_sftp_hook_retrieve_file
    ):
        operator = SFTPToS3Operator(
            task_id=TASK_ID,
            s3_bucket=BUCKET,
            s3_key=S3_KEY_MULTIPLE,
            sftp_path=SFTP_PATH_MULTIPLE,
            sftp_filenames=["test1.txt"],
        )
        operator.execute(None)

        self.assert_execute(
            mock_local_tmp_file,
            mock_s3_hook_load_file,
            mock_sftp_hook_retrieve_file,
            sftp_file=operator.sftp_path + operator.sftp_filenames[0],
            s3_file=operator.s3_key + operator.sftp_filenames[0],
        )

    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.list_directory")
    def test_execute_multiple_files_prefix(
        self,
        mock_sftp_hook_list_directory,
    ):
        operator = SFTPToS3Operator(
            task_id=TASK_ID,
            s3_bucket=BUCKET,
            s3_key=S3_KEY_MULTIPLE,
            sftp_path=SFTP_PATH_MULTIPLE,
            sftp_filenames="test_prefix",
            s3_filenames="s3_prefix",
        )
        operator.execute(None)

        mock_sftp_hook_list_directory.assert_called_once_with(path=SFTP_PATH_MULTIPLE)
