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

from airflow.providers.amazon.aws.transfers.s3_to_sftp import S3ToSFTPOperator

TASK_ID = "test_s3_to_sftp"
BUCKET = "test-s3-bucket"
S3_KEY = "test/test_1_file.csv"
SFTP_PATH = "/tmp/remote_path.txt"
AWS_CONN_ID = "aws_default"
SFTP_CONN_ID = "sftp_default"
S3_KEYS_MULTIPLE = ["test/test_1_file.csv", "test/test_2_file.csv"]
SFTP_PATH_MULTIPLE = "/tmp/"


class TestS3ToSFTPOperator:
    def assert_execute(
        self, mock_local_tmp_file, mock_s3_hook_download_file, mock_sftp_hook_put_file, sftp_file, s3_file
    ):
        mock_local_tmp_file_value = mock_local_tmp_file.return_value.__enter__.return_value
        mock_s3_hook_download_file.assert_called_once_with(
            bucket_name=BUCKET, key=s3_file, local_path=mock_local_tmp_file.name
        )

        mock_sftp_hook_put_file.assert_called_once_with(
        local_filepath=mock_local_tmp_file.name, remote_filepath=sftp_file
        )
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook")
    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook")
    def test_execute_single_file_(self, mock_sftp_hook, mock_s3_hook):
        # Configure your mocks here
        mock_s3_hook.return_value.download_file.side_effect = lambda bucket_name, key, local_path: None
        mock_sftp_hook.return_value.put.side_effect = lambda local_filepath, remote_filepath: None

        operator = S3ToSFTPOperator(
            task_id="test_s3_to_sftp",
            s3_bucket=BUCKET,
            s3_key=S3_KEY,
            sftp_path=SFTP_PATH,
            sftp_conn_id="sftp_default",
            aws_conn_id="aws_default",
        )

        operator.execute(None)
    # @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.put")
    # @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.download_file")
    # @mock.patch("airflow.providers.amazon.aws.transfers.sftp_to_s3.NamedTemporaryFile")
    # def test_execute(self, mock_local_tmp_file, mock_s3_hook_download_file, mock_sftp_hook_put_file):
    #     operator = S3ToSFTPOperator(task_id=TASK_ID, s3_bucket=BUCKET, s3_key=S3_KEY, sftp_path=SFTP_PATH)
    #     operator.execute(None)

    #     self.assert_execute(
    #         mock_local_tmp_file,
    #         mock_s3_hook_download_file,
    #         mock_sftp_hook_put_file,
    #         sftp_file=operator.sftp_path,
    #         s3_file=operator.s3_key,
    #     )
    # def test_execute_single_file(self):
    #     operator = S3ToSFTPOperator(
    #         task_id=TASK_ID,
    #         s3_bucket=BUCKET,
    #         s3_key=S3_KEY,
    #         sftp_path=SFTP_PATH,
    #         sftp_conn_id=SFTP_CONN_ID,
    #         aws_conn_id=AWS_CONN_ID,
    #     )
    #     operator.execute(None)

    #     self.assert_execute(s3_keys=[S3_KEY], sftp_filenames=[S3_KEY.split('/')[-1]])

    # def test_execute_multiple_files(self):
    #     operator = S3ToSFTPOperator(
    #         task_id=TASK_ID,
    #         s3_bucket=BUCKET,
    #         s3_key=S3_KEYS_MULTIPLE,
    #         sftp_path=SFTP_PATH_MULTIPLE,
    #         sftp_conn_id=SFTP_CONN_ID,
    #         aws_conn_id=AWS_CONN_ID,
    #     )
    #     operator.execute(None)

    #     self.assert_execute(s3_keys=S3_KEYS_MULTIPLE, sftp_filenames=[key.split('/')[-1] for key in S3_KEYS_MULTIPLE])
