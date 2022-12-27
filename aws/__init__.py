import os
import boto3
from botocore.exceptions import ClientError

import logging

logging.basicConfig(level=logging.INFO)


def download_audio(filename, tmpdir):
    logger = logging.getLogger("aws")
    
    # load AWS specs
    BUCKET_NAME = os.environ.get("S3_DIRECT_UPLOAD_BUCKET")
    AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
    REGION_NAME = os.environ.get("AWS_REGION")

    exit_code = 0
    extension = ".wav"

    if not filename:
        logger.warning("audio_tagger.aws_puller:filename not given")
        return 1, ""

    logger.info("pulling audio for audio %s", filename)

    # create client and download file
    s3 = boto3.client(
        "s3",
        region_name=REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    output_filename = "".join([tmpdir, filename.strip("/")[-1], extension])

    try:
        s3.download_file(BUCKET_NAME, filename, output_filename)
    except ClientError as e:
        logger.debug("%s", e.MSG_TEMPLATE)
        exit_code = 1

    return exit_code, output_filename
