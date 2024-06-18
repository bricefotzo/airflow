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

from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Sequence
from urllib.parse import urlsplit

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.sftp.hooks.sftp import SFTPHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SFTPToS3Operator(BaseOperator):
    """
    Transfer files from an SFTP server to Amazon S3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SFTPToS3Operator`

    :param sftp_conn_id: The sftp connection id. The name or identifier for
        establishing a connection to the SFTP server.
    :param sftp_path: The sftp remote path. This is the specified file path
        for downloading the file from the SFTP server.
    :param aws_conn_id: The s3 connection id. The name or identifier for
        establishing a connection to S3
    :param s3_bucket: The targeted s3 bucket. This is the S3 bucket to where
        the file is uploaded.
    :param s3_key: The targeted s3 key. This is the specified path for
        uploading the file to S3.
    :param use_temp_file: If True, copies file first to local,
        if False streams file from SFTP to S3.
    """

    template_fields: Sequence[str] = ("s3_key", "sftp_path", "s3_bucket")

    def __init__(
        self,
        *,
        s3_bucket: str,
        s3_key: str,
        sftp_path: str,
        sftp_filenames: str | list[str] | None = None,
        s3_filenames: str | list[str] | None = None,
        sftp_conn_id: str = "sftp_default",
        aws_conn_id: str = "aws_default",
        replace: bool = False,
        use_temp_file: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.sftp_path = sftp_path
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_conn_id = aws_conn_id
        self.use_temp_file = use_temp_file
        self.replace = replace
        self.sftp_filenames = sftp_filenames
        self.s3_filenames = s3_filenames
        self.s3_hook: S3Hook | None = None
        self.sftp_hook: SFTPHook | None = None

    @staticmethod
    def get_s3_key(s3_key: str) -> str:
        """Parse the correct format for S3 keys regardless of how the S3 url is passed."""
        parsed_s3_key = urlsplit(s3_key)
        return parsed_s3_key.path.lstrip("/")

    def __upload_to_s3_from_sftp(self, remote_filename, s3_file_key):
        with NamedTemporaryFile() as local_tmp_file:
            self.sftp_hook.retrieve_file(
                remote_full_path=remote_filename, local_full_path=local_tmp_file.name
            )

            self.s3_hook.load_file(
                filename=local_tmp_file.name,
                key=s3_file_key,
                bucket_name=self.s3_bucket,
                replace=self.replace
            )
            self.log.info("File upload to %s", s3_file_key)

    def execute(self, context: Context):
        self.sftp_hook = SFTPHook(ssh_conn_id=self.sftp_conn_id)
        self.s3_hook = S3Hook(self.aws_conn_id)

        def resolve_filenames(pattern, filenames):
            if pattern == "*":
                return filenames
            else:
                return [f for f in filenames if pattern in f]

        def generate_s3_key(filename):
            if self.s3_filenames and isinstance(self.s3_filenames, str):
                return f"{self.s3_key}{filename.replace(self.ftp_filenames, self.s3_filenames)}"
            return f"{self.s3_key}{filename}"

        if self.sftp_filenames:
            if isinstance(self.sftp_filenames, str):
                self.log.info("Getting files in %s", self.sftp_path)

                list_dir = self.sftp_hook.list_directory(path=self.sftp_path)
                files = resolve_filenames(self.sftp_filenames, list_dir)

                for file in files:
                    self.log.info("Moving file %s", file)
                    s3_file_key = generate_s3_key(file)
                    self.__upload_to_s3_from_sftp(file, s3_file_key)

            else:  
                for sftp_file, s3_file in zip(self.sftp_filenames, self.s3_filenames if self.s3_filenames else self.sftp_filenames):
                    sftp_path = self.sftp_path + sftp_file
                    s3_key = self.s3_key + (s3_file if self.s3_filenames else sftp_file)
                    self.__upload_to_s3_from_sftp(sftp_path, s3_key)
        else:
            self.__upload_to_s3_from_sftp(self.sftp_path, self.s3_key)
