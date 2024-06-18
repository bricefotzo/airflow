from __future__ import annotations

from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Sequence
from urllib.parse import urlsplit

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.sftp.hooks.sftp import SFTPHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class S3ToSFTPOperator(BaseOperator):
    """
    Transfer files from Amazon S3 to an SFTP server using SFTPHook.

    :param sftp_conn_id: The SFTP connection id.
    :param sftp_path: The SFTP remote path for uploading files.
    :param aws_conn_id: The AWS connection id for S3.
    :param s3_bucket: The source S3 bucket.
    :param s3_key: The source S3 key. Can be a single key or a list of keys.
    :param sftp_filenames: Optional list of filenames to use when saving files to the SFTP server. 
        If not provided, the original S3 filenames are used.
    :param replace: If True, will replace files on SFTP if they exist. Note: This functionality 
        depends on the behavior of the SFTPHook and the SFTP server configuration.
    """

    template_fields: Sequence[str] = ("s3_key", "sftp_path", "s3_bucket")

    def __init__(
        self,
        *,
        s3_bucket: str,
        s3_key: str | list[str],
        sftp_path: str,
        sftp_filenames: str | list[str] | None = None,
        sftp_conn_id: str = "sftp_default",
        aws_conn_id: str = "aws_default",
        replace: bool = False,  # Note: This parameter may not be relevant with SFTPHook's put method.
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.sftp_path = sftp_path
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.sftp_filenames = sftp_filenames
        self.aws_conn_id = aws_conn_id
        self.replace = replace

    @staticmethod
    def get_s3_key(s3_key: str) -> str:
        """Parse the correct format for S3 keys regardless of how the S3 URL is passed."""
        parsed_s3_key = urlsplit(s3_key)
        print(parsed_s3_key)
        return parsed_s3_key.path.lstrip("/")

    def execute(self, context: Context) -> None:
        s3_hook = S3Hook(self.aws_conn_id)
        sftp_hook = SFTPHook(self.sftp_conn_id)

        if isinstance(self.s3_key, str):
            print("s3_key is a string")
            print(self.s3_key)
            self.s3_key = [self.s3_key]
            self.sftp_filenames = [self.sftp_filenames] if self.sftp_filenames else [s3_key.split('/')[-1] for s3_key in self.s3_key]

        for index, s3_key in enumerate(self.s3_key):
            print("s3_key is a list")
            print(s3_key)
            s3_file_key = self.get_s3_key(s3_key)
            sftp_filename = self.sftp_filenames[index] if self.sftp_filenames else s3_file_key.split('/')[-1]
            sftp_full_path = f"{self.sftp_path.rstrip('/')}/{sftp_filename}"

            with NamedTemporaryFile("wb", delete=True) as tmp_file:
                print(f"Downloading file {s3_file_key} from S3 to {tmp_file.name}")
                s3_hook.download_file(bucket_name=self.s3_bucket, key=s3_file_key, local_path=tmp_file.name)
                sftp_hook.put(local_filepath=tmp_file.name, remote_filepath=sftp_full_path)
                self.log.info(f"File {s3_file_key} transferred to SFTP as {sftp_full_path}")
